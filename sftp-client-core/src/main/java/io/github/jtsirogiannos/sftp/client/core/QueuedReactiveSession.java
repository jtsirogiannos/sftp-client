package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A reactive session: operations queued and run one at a time, in order, over a connection that may
 * still be on its way.
 *
 * <p>Usable the moment it is constructed. That is what lets {@code withSession} make it current on
 * the caller's own thread — where a Vert.x context, or a thread-local, actually is — instead of in
 * whatever continuation the pool's borrow completes on. Operations issued before the connection
 * arrives simply wait for it, and fail with its failure if it never does.
 *
 * <h2>The queue</h2>
 *
 * <p>Each operation waits for the one before it to finish. Chaining each on its predecessor, rather
 * than on the shared "connection ready" stage, is what makes the order strict: dependents of a single
 * {@code CompletableFuture} run in no particular order. It is also what makes a blocking session safe
 * to drive from a multi-threaded worker — only one task ever touches it, and each completion
 * happens-before the next task starts.
 *
 * <p>A transfer holds the queue for as long as it streams. Anything issued meanwhile is refused at
 * once rather than queued: a {@code stat} queued behind a download, and awaited inside that
 * download's {@code onNext}, would never run.
 *
 * <h2>Closing</h2>
 *
 * <p>Closing waits for queued operations to finish, then hands the connection back — recycled if no
 * transfer on it failed and the provider still vouches for it. A transfer still streaming is not
 * waited for, since nothing guarantees its subscriber will ever finish it; the connection is
 * discarded under it instead.
 */
final class QueuedReactiveSession implements ReactiveSftpClientSession {

    /** The unqueued operations on one connection, and how to hand it back. */
    interface Target extends ReactiveSftpOperations {

        /** The provider's verdict on the connection; must not make a round trip. */
        boolean isValid();

        /** Recycles the connection if {@code clean} and still valid, discards it otherwise. */
        CompletionStage<Void> release(boolean clean);
    }

    private final CompletableFuture<Target> connection;
    private final SftpCapabilities providerCapabilities;
    private final Object lock = new Object();

    /** Completes when the last operation issued has finished. Guarded by {@link #lock}. */
    private CompletableFuture<Void> tail;
    /** Transfers issued and not yet over. Guarded by {@link #lock}. */
    private int transfers;
    /** Set once, by close. Guarded by {@link #lock}; volatile for the routing check. */
    private volatile boolean released;
    private volatile CompletableFuture<Void> closed;
    /** A transfer on this session failed, so the connection's state is unknown. */
    private volatile boolean tainted;

    /**
     * @param providerCapabilities what to report before the connection has negotiated anything
     */
    QueuedReactiveSession(CompletionStage<? extends Target> connection,
            SftpCapabilities providerCapabilities) {
        this.connection = connection.toCompletableFuture().thenApply(target -> (Target) target);
        this.providerCapabilities = providerCapabilities;
        this.tail = this.connection.handle((target, failure) -> null);
    }

    /** Completes when the connection is ready; fails if it cannot be had. */
    CompletionStage<Void> ready() {
        return connection.thenApply(target -> null);
    }

    @Override
    public boolean isOpen() {
        return !released;
    }

    /**
     * Whether a call routed from the client may be issued here now: open, no transfer holding the
     * queue, and a connection the provider still vouches for — or one still on its way.
     */
    boolean acceptsCalls() {
        if (released) {
            return false;
        }
        synchronized (lock) {
            if (transfers > 0) {
                return false;
            }
        }
        if (!connection.isDone()) {
            return true;
        }
        Target target = connection.getNow(null);
        return target != null && target.isValid();
    }

    // --- the queue ---

    private IllegalStateException refusal() {
        if (released) {
            return new IllegalStateException("this SFTP session is closed");
        }
        if (transfers > 0) {
            return new IllegalStateException("a transfer on this session is still in progress, and "
                    + "has the connection to itself until it ends. Issue this on another session, or "
                    + "after the transfer completes.");
        }
        return null;
    }

    private <T> CompletionStage<T> enqueue(Function<Target, CompletionStage<T>> operation) {
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture<Void> done = new CompletableFuture<>();
        CompletableFuture<Void> previous;
        synchronized (lock) {
            IllegalStateException refused = refusal();
            if (refused != null) {
                return CompletableFuture.failedFuture(refused);
            }
            previous = tail;
            tail = done;
        }
        previous.whenComplete((ignored, unused) -> {
            CompletionStage<T> stage = issue(operation);
            stage.whenComplete((value, failure) -> {
                done.complete(null);
                settle(result, value, failure);
            });
        });
        return result;
    }

    /** Runs {@code operation} on the connection, turning every way it can fail into a failed stage. */
    private <T> CompletionStage<T> issue(Function<Target, CompletionStage<T>> operation) {
        try {
            CompletionStage<T> stage = operation.apply(connection.join());
            return stage == null
                    ? CompletableFuture.failedFuture(new NullPointerException("operation returned null"))
                    : stage;
        } catch (CompletionException e) {
            return CompletableFuture.failedFuture(e.getCause() == null ? e : e.getCause());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static <T> void settle(CompletableFuture<T> result, T value, Throwable failure) {
        if (failure == null) {
            result.complete(value);
        } else {
            result.completeExceptionally(failure instanceof CompletionException && failure.getCause() != null
                    ? failure.getCause()
                    : failure);
        }
    }

    /** Claims the queue for a transfer, or says why not. Returns the stage to wait for. */
    private CompletableFuture<Void> claimForTransfer(CompletableFuture<Void> done) {
        synchronized (lock) {
            IllegalStateException refused = refusal();
            if (refused != null) {
                throw refused;
            }
            transfers++;
            CompletableFuture<Void> previous = tail;
            tail = done;
            return previous;
        }
    }

    private void transferOver(CompletableFuture<Void> done, Throwable failure) {
        if (failure != null) {
            tainted = true;
        }
        synchronized (lock) {
            transfers--;
        }
        done.complete(null);
    }

    // --- operations ---

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return enqueue(target -> target.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return enqueue(target -> target.stat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return enqueue(target -> target.exists(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return enqueue(target -> target.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return enqueue(target -> target.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return enqueue(target -> target.remove(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return enqueue(target -> target.lstat(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return enqueue(target -> target.realpath(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return enqueue(target -> target.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return enqueue(target -> target.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return enqueue(target -> target.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return enqueue(target -> target.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return enqueue(target -> target.statvfs(path));
    }

    /**
     * Claims the connection when subscribed, not when called: a publisher nobody subscribes to
     * costs nothing and blocks nothing.
     */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        return subscriber -> {
            CompletableFuture<Void> done = new CompletableFuture<>();
            CompletableFuture<Void> previous;
            try {
                previous = claimForTransfer(done);
            } catch (IllegalStateException refused) {
                subscriber.onSubscribe(NoSubscription.INSTANCE);
                subscriber.onError(refused);
                return;
            }
            previous.whenComplete((ignored, unused) -> {
                Flow.Publisher<ByteBuffer> source;
                try {
                    source = connection.join().download(remotePath, fromOffset);
                } catch (RuntimeException e) {
                    Throwable failure = e instanceof CompletionException && e.getCause() != null
                            ? e.getCause() : e;
                    transferOver(done, failure);
                    subscriber.onSubscribe(NoSubscription.INSTANCE);
                    subscriber.onError(failure);
                    return;
                }
                source.subscribe(new TerminationWatch(subscriber, failure -> transferOver(done, failure)));
            });
        };
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        CompletableFuture<Void> previous;
        try {
            previous = claimForTransfer(done);
        } catch (IllegalStateException refused) {
            return CompletableFuture.failedFuture(refused);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        previous.whenComplete((ignored, unused) -> {
            CompletionStage<Void> transfer = issue(target -> target.upload(remotePath, data, mode));
            // Cancelling the caller's stage must reach the transfer, as it does on the client.
            result.whenComplete((value, failure) -> {
                if (failure instanceof CancellationException) {
                    transfer.toCompletableFuture().cancel(true);
                }
            });
            transfer.whenComplete((value, failure) -> {
                transferOver(done, failure);
                settle(result, value, failure);
            });
        });
        return result;
    }

    @Override
    public SftpCapabilities capabilities() {
        Target target = connection.isCompletedExceptionally() ? null : connection.getNow(null);
        // Once connected, the session's negotiated answer, as for a blocking session; before that,
        // the provider's best case is all there is.
        return target != null ? target.capabilities() : providerCapabilities;
    }

    // --- closing ---

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> previous;
        boolean abandon;
        synchronized (lock) {
            if (released) {
                return closed;
            }
            closed = new CompletableFuture<>();
            released = true;
            abandon = transfers > 0;
            previous = tail;
        }
        CompletableFuture<Void> result = closed;
        CompletableFuture<Void> waitFor = abandon ? connection.handle((t, e) -> null) : previous;
        waitFor.whenComplete((ignored, unused) -> {
            Target target = connection.isCompletedExceptionally() ? null : connection.getNow(null);
            if (target == null) {
                result.complete(null); // never connected: nothing to hand back
                return;
            }
            CompletionStage<Void> handedBack;
            try {
                handedBack = target.release(!abandon && !tainted);
            } catch (RuntimeException e) {
                handedBack = CompletableFuture.completedFuture(null);
            }
            handedBack.whenComplete((v, e) -> result.complete(null));
        });
        return result;
    }

    /** Reports a download's end, however it ended, exactly once. */
    private static final class TerminationWatch implements Flow.Subscriber<ByteBuffer> {

        private final Flow.Subscriber<? super ByteBuffer> downstream;
        private final Consumer<Throwable> onEnd;
        private final AtomicBoolean ended = new AtomicBoolean();

        TerminationWatch(Flow.Subscriber<? super ByteBuffer> downstream, Consumer<Throwable> onEnd) {
            this.downstream = downstream;
            this.onEnd = onEnd;
        }

        private void end(Throwable failure) {
            if (ended.compareAndSet(false, true)) {
                onEnd.accept(failure);
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            downstream.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    subscription.request(n);
                }

                @Override
                public void cancel() {
                    subscription.cancel();
                    // An abandoned read leaves the connection usable; providers are held to that.
                    end(null);
                }
            });
        }

        @Override
        public void onNext(ByteBuffer item) {
            downstream.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            end(throwable);
            downstream.onError(throwable);
        }

        @Override
        public void onComplete() {
            end(null);
            downstream.onComplete();
        }
    }

    private enum NoSubscription implements Flow.Subscription {
        INSTANCE;

        @Override
        public void request(long n) {
        }

        @Override
        public void cancel() {
        }
    }
}

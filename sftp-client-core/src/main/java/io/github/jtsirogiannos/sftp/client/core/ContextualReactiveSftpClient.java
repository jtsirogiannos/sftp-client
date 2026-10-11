package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.context.SessionKey;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * A reactive client whose calls join the session current for it, and borrow per call otherwise —
 * {@link ContextualSftpClient}'s routing, for the reactive facade.
 *
 * <p>The one difference that shapes everything: the current session is looked up <em>when a call is
 * made</em>, synchronously, on the caller's thread. That is the moment a thread local, a Vert.x
 * context, or a replayed Reactor or coroutine context can actually be seen. Nothing here consults the
 * context from inside a continuation.
 *
 * <p>For the same reason {@code withSession} binds its session before the session has a connection:
 * {@link #newSession()} returns one usable at once, whose operations queue until the borrow
 * completes, so the binding happens on the caller's thread rather than on whatever thread the pool
 * completes its borrow on.
 *
 * <p>{@link #download} is never routed. A publisher is subscribed whenever its subscriber decides,
 * possibly after the work has ended; and the usual reason to download inside a unit of work is to
 * feed an upload, which would hold the session's one transfer slot the download would need.
 */
public abstract class ContextualReactiveSftpClient implements ReactiveSftpClient {

    private final SftpSessionContext context;
    private final SessionKey<ReactiveSftpClientSession> key;

    /**
     * @param context where the current session is looked for and recorded
     * @param owner   whose connections the sessions are, compared by identity; null means this client
     * @param tenant  the tenant sessions here belong to, or null
     */
    protected ContextualReactiveSftpClient(SftpSessionContext context, Object owner, String tenant) {
        this.context = Objects.requireNonNull(context, "context");
        SessionKey<ReactiveSftpClientSession> base =
                SessionKey.of(ReactiveSftpClientSession.class, owner == null ? this : owner);
        this.key = tenant == null ? base : base.forTenant(tenant);
    }

    /** Runs calls when no session is current: typically one borrow per call. */
    protected abstract ReactiveSftpOperations perCall();

    /**
     * A new session, usable immediately — its operations wait for its connection. Never current,
     * never shared.
     */
    protected abstract ReactiveSftpClientSession newSession();

    /**
     * A session usable at once over one that is still being opened: its operations queue, in order,
     * until {@code opening} completes, and fail with its failure if it does not.
     *
     * <p>For a composed client whose {@link #newSession()} has only another client's
     * {@code openSession()} to build on — a confined view, say — since {@code withSession} needs a
     * session to bind before the connection exists.
     */
    protected static ReactiveSftpClientSession deferred(
            CompletionStage<ReactiveSftpClientSession> opening, SftpCapabilities capabilities) {
        return new QueuedReactiveSession(opening.thenApply(SessionTarget::new), capabilities);
    }

    /** Another session as a queue's connection: it orders its own calls, and closing is its job. */
    private static final class SessionTarget extends ForwardingReactiveSftpOperations
            implements QueuedReactiveSession.Target {

        private final ReactiveSftpClientSession session;

        SessionTarget(ReactiveSftpClientSession session) {
            this.session = session;
        }

        @Override
        protected ReactiveSftpOperations delegate() {
            return session;
        }

        /**
         * The inner session's own answer. Asked only while the outer queue holds no transfer, and
         * everything goes through that queue, so the inner one is idle too and its answer is about the
         * connection alone.
         */
        @Override
        public boolean isValid() {
            return acceptsCalls(session);
        }

        @Override
        public CompletionStage<Void> release(boolean clean) {
            return session.close();
        }
    }

    @Override
    public final SftpSessionContext sessionContext() {
        return context;
    }

    /** The key this client's current session is bound under. */
    public final SessionKey<ReactiveSftpClientSession> sessionKey() {
        return key;
    }

    /**
     * Joins the current session, or opens one, makes it current for the work, and closes it once the
     * work's stage completes — after which the returned stage completes with the work's outcome.
     */
    @Override
    public <R> CompletionStage<R> withSession(
            Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work) {
        Optional<ReactiveSftpClientSession> current = joinable();
        if (current.isPresent()) {
            return apply(work, current.get());
        }
        ReactiveSftpClientSession session = newSession();
        CompletionStage<R> outcome;
        try {
            outcome = context.composeWithin(key, session, () -> apply(work, session));
        } catch (RuntimeException e) {
            outcome = CompletableFuture.failedFuture(e);
        }
        CompletableFuture<R> result = new CompletableFuture<>();
        outcome.whenComplete((value, failure) -> session.close().whenComplete((closed, ignored) -> {
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(unwrap(failure));
            }
        }));
        return result;
    }

    /** Applies the work, turning a synchronous throw or a null stage into a failed stage. */
    private static <R> CompletionStage<R> apply(
            Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work,
            ReactiveSftpOperations operations) {
        try {
            CompletionStage<R> stage = work.apply(operations);
            return stage != null ? stage : CompletableFuture.failedFuture(
                    new NullPointerException("the session work returned no stage"));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause() : failure;
    }

    private Optional<ReactiveSftpClientSession> joinable() {
        return context.current(key).filter(ContextualReactiveSftpClient::acceptsCalls);
    }

    private ReactiveSftpOperations target() {
        Optional<ReactiveSftpClientSession> current = joinable();
        return current.isPresent() ? current.get() : perCall();
    }

    /** Whether a call may be issued on {@code session} now, unwrapping decorators to find out. */
    static boolean acceptsCalls(ReactiveSftpClientSession session) {
        ReactiveSftpClientSession inner = session;
        while (inner instanceof ForwardingReactiveSftpClientSession forwarding) {
            inner = forwarding.session();
        }
        return inner instanceof QueuedReactiveSession queued ? queued.acceptsCalls() : inner.isOpen();
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return target().list(path);
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return target().stat(path);
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return target().exists(path);
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return target().mkdir(path);
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return target().rename(from, to);
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return target().remove(path);
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return target().lstat(path);
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return target().realpath(path);
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return target().symlink(linkPath, targetPath);
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return target().readlink(linkPath);
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return target().hardlink(linkPath, targetPath);
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return target().setAttributes(path, update);
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return target().statvfs(path);
    }

    // --- composites, routed once ---
    //
    // The convenience methods are composed of several primitives, and on this facade every step
    // after the first runs in a continuation — on whichever thread completed the step before, where a
    // binding made on the caller's thread is not visible. Routed primitive by primitive, the first
    // step of mkdirs would join the current session and the rest would borrow a connection each, and
    // on a pool the session already fills, wait for it. So each composite resolves its target once,
    // when it is called, and runs every step there.

    @Override
    public CompletionStage<Void> mkdirs(String path) {
        return target().mkdirs(path);
    }

    @Override
    public CompletionStage<Integer> deleteRecursively(String path) {
        return target().deleteRecursively(path);
    }

    @Override
    public CompletionStage<Void> uploadAtomically(String remotePath, Flow.Publisher<ByteBuffer> data) {
        return target().uploadAtomically(remotePath, data);
    }

    /** Never routed; see the class comment. */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        return perCall().download(remotePath, fromOffset);
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        return target().upload(remotePath, data, mode);
    }

    /** What the backend can do. Constant for the client, whatever session a call lands on. */
    @Override
    public SftpCapabilities capabilities() {
        return perCall().capabilities();
    }
}

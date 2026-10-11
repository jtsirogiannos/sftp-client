package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

/**
 * One blocking session's operations, each offloaded to the worker — what a
 * {@link QueuedReactiveSession} drives for a blocking provider.
 *
 * <p>Unqueued and unguarded on purpose: the queue in front of it is what guarantees one call at a
 * time, which is all a single-owner {@link SftpSession} needs.
 */
final class BlockingSessionTarget implements QueuedReactiveSession.Target {

    private final SftpSession session;
    private final SftpSessionPool<SftpSession> pool;
    private final Executor worker;
    private final int chunk;

    BlockingSessionTarget(SftpSession session, SftpSessionPool<SftpSession> pool, Executor worker,
            int chunk) {
        this.session = session;
        this.pool = pool;
        this.worker = worker;
        this.chunk = chunk;
    }

    private <T> CompletionStage<T> offload(Supplier<T> operation) {
        CompletableFuture<T> result = new CompletableFuture<>();
        worker.execute(() -> {
            try {
                result.complete(operation.get());
            } catch (RuntimeException e) {
                result.completeExceptionally(e instanceof SftpException ? e
                        : new SftpException("operation failed", e));
            }
        });
        return result;
    }

    private CompletionStage<Void> offloadVoid(Runnable operation) {
        return offload(() -> {
            operation.run();
            return null;
        });
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return offload(() -> session.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return offload(() -> session.stat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return offload(() -> session.exists(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return offloadVoid(() -> session.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return offloadVoid(() -> session.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return offloadVoid(() -> session.remove(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return offload(() -> session.lstat(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return offload(() -> session.realpath(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return offloadVoid(() -> session.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return offload(() -> session.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return offloadVoid(() -> session.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return offloadVoid(() -> session.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return offload(() -> session.statvfs(path));
    }

    /** The session stays the queue's: nothing is handed back when the stream ends. */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        return new InputStreamPublisher(() -> session.openRead(remotePath, fromOffset), chunk, worker,
                failure -> { });
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        worker.execute(() -> {
            if (result.isDone()) {
                return; // cancelled before it started
            }
            OutputStream out;
            try {
                out = session.openWrite(remotePath, mode);
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
                return;
            }
            data.subscribe(new WriteSubscriber(out, () -> { }, worker, result));
        });
        return result;
    }

    @Override
    public SftpCapabilities capabilities() {
        return session.capabilities();
    }

    @Override
    public boolean isValid() {
        return session.isValid();
    }

    /** On the worker: discarding closes a socket, which can block. */
    @Override
    public CompletionStage<Void> release(boolean clean) {
        return offloadVoid(() -> {
            if (clean && session.isValid()) {
                pool.recycle(session);
            } else {
                pool.discard(session);
            }
        });
    }
}

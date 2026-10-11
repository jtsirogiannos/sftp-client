package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

/**
 * One thread-free session's operations — what a {@link QueuedReactiveSession} drives for an async
 * provider. Nothing here hops threads; the queue in front of it orders the calls.
 */
final class AsyncSessionTarget implements QueuedReactiveSession.Target {

    private final AsyncSftpSession session;
    private final SftpSessionPool<AsyncSftpSession> pool;
    private final Executor worker;

    AsyncSessionTarget(AsyncSftpSession session, SftpSessionPool<AsyncSftpSession> pool,
            Executor worker) {
        this.session = session;
        this.pool = pool;
        this.worker = worker;
    }

    /** Normalises failures as the async client does, including a provider that throws. */
    private static <T> CompletionStage<T> call(Supplier<CompletionStage<T>> operation) {
        CompletionStage<T> stage;
        try {
            stage = operation.get();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(normalise(e));
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        stage.whenComplete((value, failure) -> {
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(normalise(failure));
            }
        });
        return result;
    }

    private static Throwable normalise(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause() : failure;
        return cause instanceof SftpException ? cause : new SftpException("operation failed", cause);
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return call(() -> session.listAsync(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return call(() -> session.statAsync(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return call(() -> session.existsAsync(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return call(() -> session.mkdirAsync(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return call(() -> session.renameAsync(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return call(() -> session.removeAsync(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return call(() -> session.lstatAsync(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return call(() -> session.realpathAsync(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return call(() -> session.symlinkAsync(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return call(() -> session.readlinkAsync(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return call(() -> session.hardlinkAsync(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return call(() -> session.setAttributesAsync(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return call(() -> session.statvfsAsync(path));
    }

    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        return session.readAsync(remotePath, fromOffset);
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        return call(() -> session.writeAsync(remotePath, data, mode));
    }

    @Override
    public SftpCapabilities capabilities() {
        return session.capabilities();
    }

    @Override
    public boolean isValid() {
        return session.isValid();
    }

    /** On the worker, like the async client's own discards: closing may block. */
    @Override
    public CompletionStage<Void> release(boolean clean) {
        return CompletableFuture.runAsync(() -> {
            if (clean && session.isValid()) {
                pool.recycle(session);
            } else {
                pool.discard(session);
            }
        }, worker);
    }
}

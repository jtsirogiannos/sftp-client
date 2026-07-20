package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;

/**
 * Mutiny-flavoured view of the neutral {@link ReactiveSftpClient}.
 *
 * <p>This class is the entire Quarkus reactive integration, and its thinness is the proof that
 * Phase 0 was done right. Mutiny 2 is natively {@code java.util.concurrent.Flow}-based, so no
 * adapter jar is needed — {@code Multi.createFrom().publisher(...)} takes the core's publisher
 * directly, and the only real work is converting {@link ByteBuffer} to Vert.x {@link Buffer}
 * at the edge.
 *
 * <p>If this file ever grows past trivial conversion, framework concepts have leaked into the core.
 * Growing <em>wider</em> is expected, though: every operation the neutral client gains must appear
 * here as another one-line delegation, because this bean is the only reactive surface a Quarkus
 * application sees. {@link #unwrap()} is the safety valve for the gap between those catch-ups.
 */
public class MutinySftpClient implements AutoCloseable {

    private final ReactiveSftpClient delegate;

    public MutinySftpClient(ReactiveSftpClient delegate) {
        this.delegate = delegate;
    }

    /**
     * The neutral client underneath, exposed because this bean is the <em>only</em> reactive bean
     * Quarkus registers: without an escape hatch, anything this wrapper lags behind on is
     * unreachable in a Quarkus application. It once lagged by thirteen operations.
     */
    public ReactiveSftpClient unwrap() {
        return delegate;
    }

    public Uni<List<RemoteFile>> list(String path) {
        return Uni.createFrom().completionStage(() -> delegate.list(path));
    }

    public Uni<RemoteFile> stat(String path) {
        return Uni.createFrom().completionStage(() -> delegate.stat(path));
    }

    public Uni<Boolean> exists(String path) {
        return Uni.createFrom().completionStage(() -> delegate.exists(path));
    }

    public Uni<Void> mkdir(String path) {
        return Uni.createFrom().completionStage(() -> delegate.mkdir(path));
    }

    public Uni<Void> rename(String from, String to) {
        return Uni.createFrom().completionStage(() -> delegate.rename(from, to));
    }

    public Uni<Void> remove(String path) {
        return Uni.createFrom().completionStage(() -> delegate.remove(path));
    }

    /** Attributes without following symlinks — what a tree walk must use. */
    public Uni<RemoteFile> lstat(String path) {
        return Uni.createFrom().completionStage(() -> delegate.lstat(path));
    }

    public Uni<String> realpath(String path) {
        return Uni.createFrom().completionStage(() -> delegate.realpath(path));
    }

    public Uni<Void> symlink(String linkPath, String targetPath) {
        return Uni.createFrom().completionStage(() -> delegate.symlink(linkPath, targetPath));
    }

    public Uni<String> readlink(String linkPath) {
        return Uni.createFrom().completionStage(() -> delegate.readlink(linkPath));
    }

    public Uni<Void> hardlink(String linkPath, String targetPath) {
        return Uni.createFrom().completionStage(() -> delegate.hardlink(linkPath, targetPath));
    }

    public Uni<Void> setAttributes(String path, AttributeUpdate update) {
        return Uni.createFrom().completionStage(() -> delegate.setAttributes(path, update));
    }

    public Uni<FilesystemStats> statvfs(String path) {
        return Uni.createFrom().completionStage(() -> delegate.statvfs(path));
    }

    /** {@code mkdir -p}. An existing chain is not an error. */
    public Uni<Void> mkdirs(String path) {
        return Uni.createFrom().completionStage(() -> delegate.mkdirs(path));
    }

    /** Deletes a tree, removing symlinks as links, never following them. */
    public Uni<Integer> deleteRecursively(String path) {
        return Uni.createFrom().completionStage(() -> delegate.deleteRecursively(path));
    }

    public Uni<Void> chmod(String path, int mode) {
        return Uni.createFrom().completionStage(() -> delegate.chmod(path, mode));
    }

    /** Directory entries matching {@code filter}, filtered client-side. */
    public Uni<List<RemoteFile>> list(String path, Predicate<RemoteFile> filter) {
        return Uni.createFrom().completionStage(() -> delegate.list(path, filter));
    }

    public Uni<Optional<RemoteFile>> findFirst(String path, Predicate<RemoteFile> match) {
        return Uni.createFrom().completionStage(() -> delegate.findFirst(path, match));
    }

    public Multi<Buffer> download(String remotePath) {
        return Multi.createFrom().publisher(delegate.download(remotePath))
                .map(MutinySftpClient::toVertxBuffer);
    }

    public Uni<Void> upload(String remotePath, Multi<Buffer> data) {
        // Multi IS a Flow.Publisher in Mutiny 2, so this hands straight through.
        return Uni.createFrom().completionStage(
                () -> delegate.upload(remotePath, data.map(b -> ByteBuffer.wrap(b.getBytes()))));
    }

    /**
     * Publishes by rename, so a reader watching the destination never sees a partial file.
     * Replacing an existing file needs SFTP v5+ rename semantics and fails on a v3 server rather
     * than degrading — see the neutral client's documentation.
     */
    public Uni<Void> uploadAtomically(String remotePath, Multi<Buffer> data) {
        return Uni.createFrom().completionStage(
                () -> delegate.uploadAtomically(remotePath, data.map(b -> ByteBuffer.wrap(b.getBytes()))));
    }

    public SftpCapabilities capabilities() {
        return delegate.capabilities();
    }

    /**
     * Closes the underlying client and its connection pool.
     *
     * <p>This wrapper owns the delegate it was handed, so the container disposing this bean is what
     * releases the endpoint's connections at shutdown.
     */
    @Override
    public void close() {
        delegate.close();
    }

    private static Buffer toVertxBuffer(ByteBuffer bb) {
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return Buffer.buffer(bytes);
    }
}

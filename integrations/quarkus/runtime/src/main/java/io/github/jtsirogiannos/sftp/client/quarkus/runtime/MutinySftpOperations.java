package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

/**
 * Every operation as a {@link Uni} or {@link Multi} — the Mutiny view of
 * {@link ReactiveSftpOperations}. What a {@link MutinySftpClient} offers per call, and what its
 * {@code withSession} hands the work.
 *
 * <h2>Staying on the request's context</h2>
 *
 * <p>Each operation runs at subscription and its result is emitted back on the Vert.x context it was
 * subscribed from. Without that, the result would arrive on whichever worker or I/O thread completed
 * it, the next step of the pipeline would run there, and the request's duplicated context — where a
 * unit of work's current session lives — would be gone. Quarkus' own reactive clients return on the
 * caller's context for the same reason. Quarkus' own worker executor already carries the request's
 * context onto its threads; returning to it explicitly makes that hold whatever executor the client
 * was built with. Off a Vert.x context there is nothing to return to, and the result is emitted
 * where it completed.
 */
public class MutinySftpOperations {

    private final ReactiveSftpOperations operations;

    MutinySftpOperations(ReactiveSftpOperations operations) {
        this.operations = operations;
    }

    /**
     * For CDI's client proxies only: an application-scoped bean of a subclass is proxied, and a proxy
     * needs a constructor to call. Every method of a proxy forwards to the real bean.
     */
    protected MutinySftpOperations() {
        this.operations = null;
    }

    /** Where calls go. */
    ReactiveSftpOperations operations() {
        return operations;
    }

    /** Runs the call at subscription and emits its result on the subscriber's Vert.x context. */
    static <T> Uni<T> call(Supplier<? extends CompletionStage<? extends T>> operation) {
        return Uni.createFrom().deferred(() -> onCallersContext(
                Uni.createFrom().<T>completionStage(operation::get)));
    }

    static <T> Uni<T> onCallersContext(Uni<T> uni) {
        Context caller = Vertx.currentContext();
        return caller == null ? uni : uni.emitOn(command -> caller.runOnContext(ignored -> command.run()));
    }

    public Uni<List<RemoteFile>> list(String path) {
        return call(() -> operations.list(path));
    }

    public Uni<RemoteFile> stat(String path) {
        return call(() -> operations.stat(path));
    }

    public Uni<Boolean> exists(String path) {
        return call(() -> operations.exists(path));
    }

    public Uni<Void> mkdir(String path) {
        return call(() -> operations.mkdir(path));
    }

    public Uni<Void> rename(String from, String to) {
        return call(() -> operations.rename(from, to));
    }

    public Uni<Void> remove(String path) {
        return call(() -> operations.remove(path));
    }

    /** Attributes without following symlinks — what a tree walk must use. */
    public Uni<RemoteFile> lstat(String path) {
        return call(() -> operations.lstat(path));
    }

    public Uni<String> realpath(String path) {
        return call(() -> operations.realpath(path));
    }

    public Uni<Void> symlink(String linkPath, String targetPath) {
        return call(() -> operations.symlink(linkPath, targetPath));
    }

    public Uni<String> readlink(String linkPath) {
        return call(() -> operations.readlink(linkPath));
    }

    public Uni<Void> hardlink(String linkPath, String targetPath) {
        return call(() -> operations.hardlink(linkPath, targetPath));
    }

    public Uni<Void> setAttributes(String path, AttributeUpdate update) {
        return call(() -> operations.setAttributes(path, update));
    }

    public Uni<FilesystemStats> statvfs(String path) {
        return call(() -> operations.statvfs(path));
    }

    /** {@code mkdir -p}. An existing chain is not an error. */
    public Uni<Void> mkdirs(String path) {
        return call(() -> operations.mkdirs(path));
    }

    /** Deletes a tree, removing symlinks as links, never following them. */
    public Uni<Integer> deleteRecursively(String path) {
        return call(() -> operations.deleteRecursively(path));
    }

    public Uni<Void> chmod(String path, int mode) {
        return call(() -> operations.chmod(path, mode));
    }

    /** Directory entries matching {@code filter}, filtered client-side. */
    public Uni<List<RemoteFile>> list(String path, Predicate<RemoteFile> filter) {
        return call(() -> operations.list(path, filter));
    }

    public Uni<Optional<RemoteFile>> findFirst(String path, Predicate<RemoteFile> match) {
        return call(() -> operations.findFirst(path, match));
    }

    public Multi<Buffer> download(String remotePath) {
        return Multi.createFrom().publisher(operations.download(remotePath))
                .map(MutinySftpOperations::toVertxBuffer);
    }

    /** As {@link #download(String)}, starting at {@code fromOffset} — the resume primitive. */
    public Multi<Buffer> download(String remotePath, long fromOffset) {
        return Multi.createFrom().publisher(operations.download(remotePath, fromOffset))
                .map(MutinySftpOperations::toVertxBuffer);
    }

    public Uni<Void> upload(String remotePath, Multi<Buffer> data) {
        // Multi IS a Flow.Publisher in Mutiny 2, so this hands straight through.
        return call(
                () -> operations.upload(remotePath, data.map(b -> ByteBuffer.wrap(b.getBytes()))));
    }

    /** As {@link #upload(String, Multi)}, with explicit write semantics. */
    public Uni<Void> upload(String remotePath, Multi<Buffer> data,
            io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode mode) {
        return call(
                () -> operations.upload(remotePath, data.map(b -> ByteBuffer.wrap(b.getBytes())), mode));
    }

    /**
     * Publishes by rename, so a reader watching the destination never sees a partial file.
     * Replacing an existing file needs SFTP v5+ rename semantics and fails on a v3 server rather
     * than degrading — see the neutral client's documentation.
     */
    public Uni<Void> uploadAtomically(String remotePath, Multi<Buffer> data) {
        return call(
                () -> operations.uploadAtomically(remotePath, data.map(b -> ByteBuffer.wrap(b.getBytes()))));
    }

    public SftpCapabilities capabilities() {
        return operations.capabilities();
    }

    static Buffer toVertxBuffer(ByteBuffer bb) {
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return Buffer.buffer(bytes);
    }
}

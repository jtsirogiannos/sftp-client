package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Every operation as a {@link Mono} or {@link Flux} — the Reactor view of
 * {@link ReactiveSftpOperations}.
 *
 * <p>What a {@link ReactorSftpClient} offers per call, and what its {@code withSession} hands the
 * work. Each call is made at subscription, with the subscriber context's sessions replayed onto the
 * library's session context — which is how a call deep in a unit of work joins its session though
 * Reactor's context is invisible to the library.
 */
public class ReactorSftpOperations {

    private final ReactiveSftpOperations operations;
    final SftpSessionContext context;

    ReactorSftpOperations(ReactiveSftpOperations operations, SftpSessionContext context) {
        this.operations = operations;
        this.context = context;
    }

    /** Where calls go; a client over many tenants picks the subscriber's tenant here. */
    ReactiveSftpOperations target(ContextView subscriber) {
        return operations;
    }

    /** Makes the call at subscription, with the subscriber's sessions current. */
    <T> Mono<T> call(Function<ReactiveSftpOperations, CompletionStage<T>> operation) {
        return Mono.deferContextual(subscriber -> {
            ReactiveSftpOperations target = target(subscriber);
            return Mono.fromCompletionStage(context.callWith(SftpReactorContext.bindings(subscriber),
                    () -> operation.apply(target)));
        });
    }

    public Mono<List<RemoteFile>> list(String path) {
        return call(ops -> ops.list(path));
    }

    public Mono<RemoteFile> stat(String path) {
        return call(ops -> ops.stat(path));
    }

    public Mono<Boolean> exists(String path) {
        return call(ops -> ops.exists(path));
    }

    public Mono<Void> mkdir(String path) {
        return call(ops -> ops.mkdir(path));
    }

    public Mono<Void> rename(String from, String to) {
        return call(ops -> ops.rename(from, to));
    }

    public Mono<Void> remove(String path) {
        return call(ops -> ops.remove(path));
    }

    /** Attributes without following symlinks — what a tree walk must use. */
    public Mono<RemoteFile> lstat(String path) {
        return call(ops -> ops.lstat(path));
    }

    public Mono<String> realpath(String path) {
        return call(ops -> ops.realpath(path));
    }

    public Mono<Void> symlink(String linkPath, String targetPath) {
        return call(ops -> ops.symlink(linkPath, targetPath));
    }

    public Mono<String> readlink(String linkPath) {
        return call(ops -> ops.readlink(linkPath));
    }

    public Mono<Void> hardlink(String linkPath, String targetPath) {
        return call(ops -> ops.hardlink(linkPath, targetPath));
    }

    public Mono<Void> setAttributes(String path, AttributeUpdate update) {
        return call(ops -> ops.setAttributes(path, update));
    }

    public Mono<FilesystemStats> statvfs(String path) {
        return call(ops -> ops.statvfs(path));
    }

    /** {@code mkdir -p}. An existing chain is not an error. */
    public Mono<Void> mkdirs(String path) {
        return call(ops -> ops.mkdirs(path));
    }

    /** Deletes a tree, removing symlinks as links, never following them. */
    public Mono<Integer> deleteRecursively(String path) {
        return call(ops -> ops.deleteRecursively(path));
    }

    public Mono<Void> chmod(String path, int mode) {
        return call(ops -> ops.chmod(path, mode));
    }

    /** Directory entries matching {@code filter}, filtered client-side. */
    public Mono<List<RemoteFile>> list(String path, Predicate<RemoteFile> filter) {
        return call(ops -> ops.list(path, filter));
    }

    /** The first entry matching {@code match}; empty when nothing does. */
    public Mono<RemoteFile> findFirst(String path, Predicate<RemoteFile> match) {
        return call(ops -> ops.findFirst(path, match)).flatMap(Mono::justOrEmpty);
    }

    /**
     * Backpressured chunks. Cancelling the subscription abandons the transfer. On a client this
     * borrows its own connection whatever session is current: a flux is subscribed whenever its
     * subscriber decides.
     */
    public Flux<ByteBuffer> download(String remotePath) {
        return download(remotePath, 0);
    }

    /** As {@link #download(String)}, starting at {@code fromOffset} — the resume primitive. */
    public Flux<ByteBuffer> download(String remotePath, long fromOffset) {
        return Flux.deferContextual(subscriber -> Flux.from(
                FlowAdapters.toPublisher(target(subscriber).download(remotePath, fromOffset))));
    }

    /** Consumes {@code data} with backpressure; completes when the upload has landed. */
    public Mono<Void> upload(String remotePath, Publisher<ByteBuffer> data) {
        return call(ops -> ops.upload(remotePath, FlowAdapters.toFlowPublisher(data)));
    }

    /** As {@link #upload(String, Publisher)}, with explicit write semantics. */
    public Mono<Void> upload(String remotePath, Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        return call(ops -> ops.upload(remotePath, FlowAdapters.toFlowPublisher(data), mode));
    }

    /**
     * Publishes by rename, so a reader watching the destination never sees a partial file.
     * Replacing an existing file needs SFTP v5+ rename semantics and fails on a v3 server rather
     * than degrading.
     */
    public Mono<Void> uploadAtomically(String remotePath, Publisher<ByteBuffer> data) {
        return call(ops -> ops.uploadAtomically(remotePath, FlowAdapters.toFlowPublisher(data)));
    }

    public SftpCapabilities capabilities() {
        return operations.capabilities();
    }
}

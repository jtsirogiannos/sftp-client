package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import java.nio.ByteBuffer;
import java.util.List;

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
 */
public class MutinySftpClient implements AutoCloseable {

    private final ReactiveSftpClient delegate;

    public MutinySftpClient(ReactiveSftpClient delegate) {
        this.delegate = delegate;
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

    public Multi<Buffer> download(String remotePath) {
        return Multi.createFrom().publisher(delegate.download(remotePath))
                .map(MutinySftpClient::toVertxBuffer);
    }

    public Uni<Void> upload(String remotePath, Multi<Buffer> data) {
        // Multi IS a Flow.Publisher in Mutiny 2, so this hands straight through.
        return Uni.createFrom().completionStage(
                () -> delegate.upload(remotePath, data.map(b -> ByteBuffer.wrap(b.getBytes()))));
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

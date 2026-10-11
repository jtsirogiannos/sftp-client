package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Forwards every operation to another {@link ReactiveSftpOperations}. The reactive counterpart to
 * {@link ForwardingSftpOperations}, and a base for decorators of clients and sessions alike.
 *
 * <p>As there, only the abstract operations are forwarded: the {@code default} convenience methods
 * — {@code mkdirs}, {@code deleteRecursively}, {@code uploadAtomically}, {@code findFirst},
 * filtered {@code list} — run on the decorator and their constituent calls pass through it
 * individually, so a decorator observes what actually crossed the wire.
 *
 * <p>Decorating this interface is harder than decorating the blocking one, and the difference is
 * worth stating because it is where the mistakes live: an operation here <em>returns</em> long
 * before it finishes. A decorator that measures around the method call measures the cost of
 * constructing a {@link CompletionStage} and reports approximately zero. Work belongs in a
 * completion callback, and for {@link #download} in the publisher's terminal signal.
 */
public abstract class ForwardingReactiveSftpOperations implements ReactiveSftpOperations {

    /** The instance every call is forwarded to. Called per operation, so keep it cheap. */
    protected abstract ReactiveSftpOperations delegate();

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return delegate().list(path);
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return delegate().stat(path);
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return delegate().exists(path);
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return delegate().mkdir(path);
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return delegate().rename(from, to);
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return delegate().remove(path);
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return delegate().lstat(path);
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return delegate().realpath(path);
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return delegate().symlink(linkPath, targetPath);
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return delegate().readlink(linkPath);
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return delegate().hardlink(linkPath, targetPath);
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return delegate().setAttributes(path, update);
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return delegate().statvfs(path);
    }

    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        return delegate().download(remotePath, fromOffset);
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode mode) {
        return delegate().upload(remotePath, data, mode);
    }

    @Override
    public SftpCapabilities capabilities() {
        return delegate().capabilities();
    }
}

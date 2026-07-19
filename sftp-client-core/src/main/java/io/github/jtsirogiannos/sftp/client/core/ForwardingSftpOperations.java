package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Forwards every operation to another {@link SftpOperations}. A base for decorators.
 *
 * <p>Metrics, tracing, retry and logging are all the same shape: wrap a client, do something around
 * each call, delegate. Written out longhand that is nineteen identical methods per decorator, and
 * two decorators that drift are worse than one. This class holds the boilerplate so a decorator
 * overrides only what it actually changes.
 *
 * <p>It stays in the core rather than in any observability module, and it learns nothing about what
 * a meter or a span is — that separation is the same rule that keeps {@code sftp-client-api} at zero
 * dependencies.
 *
 * <h2>Only the primitives are forwarded, and that is deliberate</h2>
 *
 * <p>{@link SftpOperations} has a second tier of {@code default} methods — {@code mkdirs},
 * {@code deleteRecursively}, {@code visit}, {@code uploadAtomically} — composed entirely from the
 * primitives below. They are <em>not</em> forwarded, so they run on the decorator and their
 * constituent calls pass through it individually.
 *
 * <p>For a decorator that measures things, that is the useful behaviour: {@code mkdirs} on a
 * four-deep path reports the {@code exists} and {@code mkdir} calls it actually made, which is what
 * crossed the wire and what a slow server would show up in. Forwarding the composite instead would
 * hand the whole sequence to the delegate and observe one opaque call. A decorator that genuinely
 * wants the composite as a unit can still override it.
 */
public abstract class ForwardingSftpOperations implements SftpOperations {

    /** The instance every call is forwarded to. Called per operation, so keep it cheap. */
    protected abstract SftpOperations delegate();

    @Override
    public List<RemoteFile> list(String path) {
        return delegate().list(path);
    }

    @Override
    public RemoteFile stat(String path) {
        return delegate().stat(path);
    }

    @Override
    public RemoteFile lstat(String path) {
        return delegate().lstat(path);
    }

    @Override
    public boolean exists(String path) {
        return delegate().exists(path);
    }

    @Override
    public String realpath(String path) {
        return delegate().realpath(path);
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        delegate().symlink(linkPath, targetPath);
    }

    @Override
    public String readlink(String linkPath) {
        return delegate().readlink(linkPath);
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        delegate().hardlink(linkPath, targetPath);
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        delegate().setAttributes(path, update);
    }

    @Override
    public FilesystemStats statvfs(String path) {
        return delegate().statvfs(path);
    }

    @Override
    public void mkdir(String path) {
        delegate().mkdir(path);
    }

    @Override
    public void rename(String from, String to) {
        delegate().rename(from, to);
    }

    @Override
    public void remove(String path) {
        delegate().remove(path);
    }

    @Override
    public InputStream download(String remotePath) {
        return delegate().download(remotePath);
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        return delegate().download(remotePath, target);
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        return delegate().download(remotePath, target, fromOffset);
    }

    @Override
    public void upload(String remotePath, InputStream data) {
        delegate().upload(remotePath, data);
    }

    @Override
    public void upload(String remotePath, InputStream data, SftpSession.WriteMode mode) {
        delegate().upload(remotePath, data, mode);
    }

    @Override
    public SftpCapabilities capabilities() {
        return delegate().capabilities();
    }
}

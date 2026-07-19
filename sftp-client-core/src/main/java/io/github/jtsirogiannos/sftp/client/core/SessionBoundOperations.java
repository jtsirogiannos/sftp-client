package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Predicate;

/**
 * The operations, pinned to one already-borrowed session.
 *
 * <p>Handed to the block passed to {@code SftpClient.withSession}, so a sequence of calls travels
 * over a single connection instead of borrowing and returning one per operation. Every default
 * method on {@link SftpOperations} — {@code mkdirs}, {@code deleteRecursively}, {@code findFirst} —
 * comes along for free, because they are composed from the primitives below.
 *
 * <p>Deliberately not thread-safe and not something to keep: a session is single-owner, and the one
 * behind this view goes back to the pool the moment the block returns. The lifetime is the block's.
 */
final class SessionBoundOperations implements SftpOperations {

    private static final int COPY_BUFFER = 64 * 1024;

    private final SftpSession session;

    SessionBoundOperations(SftpSession session) {
        this.session = session;
    }

    @Override
    public List<RemoteFile> list(String path) {
        return session.list(path);
    }

    @Override
    public void visit(String path, Predicate<RemoteFile> visitor) {
        // Straight through: no borrow, and no discard-on-early-stop either. Stopping early leaves
        // entries queued on this session, so the block's session is poisoned for reuse — which the
        // client handles by discarding it when a visit was abandoned.
        session.list(path, visitor);
    }

    @Override
    public RemoteFile stat(String path) {
        return session.stat(path);
    }

    @Override
    public RemoteFile lstat(String path) {
        return session.lstat(path);
    }

    @Override
    public boolean exists(String path) {
        return session.exists(path);
    }

    @Override
    public String realpath(String path) {
        return session.realpath(path);
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        session.symlink(linkPath, targetPath);
    }

    @Override
    public String readlink(String linkPath) {
        return session.readlink(linkPath);
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        session.hardlink(linkPath, targetPath);
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        session.setAttributes(path, update);
    }

    @Override
    public FilesystemStats statvfs(String path) {
        return session.statvfs(path);
    }

    @Override
    public void mkdir(String path) {
        session.mkdir(path);
    }

    @Override
    public void rename(String from, String to) {
        session.rename(from, to);
    }

    @Override
    public void remove(String path) {
        session.remove(path);
    }

    /**
     * The raw stream, valid only until the block returns.
     *
     * <p>Unlike the pooled client's version this does not tie a session's fate to the stream: the
     * block owns the session, so closing the stream must not release it.
     */
    @Override
    public InputStream download(String remotePath) {
        return session.openRead(remotePath);
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        return download(remotePath, target, 0);
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        try (InputStream in = session.openRead(remotePath, fromOffset)) {
            byte[] buffer = new byte[COPY_BUFFER];
            long total = 0;
            int read;
            while ((read = in.read(buffer)) != -1) {
                target.write(buffer, 0, read);
                total += read;
            }
            return total;
        } catch (IOException e) {
            throw new SftpException("download of '" + remotePath + "' failed", e);
        }
    }

    @Override
    public void upload(String remotePath, InputStream data) {
        upload(remotePath, data, SftpSession.WriteMode.OVERWRITE);
    }

    @Override
    public void upload(String remotePath, InputStream data, SftpSession.WriteMode mode) {
        try (OutputStream out = session.openWrite(remotePath, mode)) {
            data.transferTo(out);
        } catch (IOException e) {
            throw new SftpException("upload to '" + remotePath + "' failed", e);
        }
    }

    @Override
    public SftpCapabilities capabilities() {
        // The session's answer, not the provider's: inside a block we know what was negotiated.
        return session.capabilities();
    }
}

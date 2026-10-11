package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * The operations, pinned to one borrowed session until {@link #close()} hands it back.
 *
 * <p>What {@code openSession()} returns and what a {@code withSession} block runs over, so a sequence
 * of calls travels over a single connection instead of borrowing and returning one per operation. Every default method on {@link SftpOperations} —
 * {@code mkdirs}, {@code deleteRecursively}, {@code findFirst} — comes along for free, because they
 * are composed from the primitives below.
 *
 * <h2>Recycled or discarded</h2>
 *
 * <p>On close the session goes back to the pool <strong>if and only if the provider vouches for the
 * channel and nothing is still reading from it</strong>. That is two facts, both knowable:
 *
 * <ul>
 *   <li>{@link SftpSession#isValid()}, which the SPI obliges to return false whenever the channel may
 *       be out of step with the server. A recoverable failure — a missing file, a permission refusal —
 *       leaves it true, because the server answered and the channel is fine.</li>
 *   <li>Whether a stream opened here is still open. Its unread bytes would be the next borrower's
 *       first reply.</li>
 * </ul>
 *
 * <p>What it deliberately does not look at is whether the caller's block threw. That used to be the
 * rule, and it charged a reconnect for every business exception that happened to be thrown inside a
 * block, while learning nothing: an exception from user code says nothing about the connection, and
 * one from the provider already shows up in {@code isValid()}.
 *
 * <p>Single-owner and not thread-safe, like the session behind it. Only the {@code released} flag is
 * read across threads, so that a close from elsewhere is seen by anything still holding this.
 */
final class PooledSftpClientSession implements SftpClientSession {

    private static final int COPY_BUFFER = 64 * 1024;

    private final SftpSession session;
    private final SftpSessionPool<SftpSession> pool;
    private final AtomicBoolean released = new AtomicBoolean();
    private final AtomicInteger openStreams = new AtomicInteger();

    PooledSftpClientSession(SftpSession session, SftpSessionPool<SftpSession> pool) {
        this.session = session;
        this.pool = pool;
    }

    @Override
    public List<RemoteFile> list(String path) {
        return session().list(path);
    }

    @Override
    public void visit(String path, Predicate<RemoteFile> visitor) {
        // Straight through. Stopping early is fine: the SPI obliges the provider to close the
        // directory handle and leave the session usable, and the conformance suite holds it to that.
        session().list(path, visitor);
    }

    @Override
    public RemoteFile stat(String path) {
        return session().stat(path);
    }

    @Override
    public RemoteFile lstat(String path) {
        return session().lstat(path);
    }

    @Override
    public boolean exists(String path) {
        return session().exists(path);
    }

    @Override
    public String realpath(String path) {
        return session().realpath(path);
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        session().symlink(linkPath, targetPath);
    }

    @Override
    public String readlink(String linkPath) {
        return session().readlink(linkPath);
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        session().hardlink(linkPath, targetPath);
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        session().setAttributes(path, update);
    }

    @Override
    public FilesystemStats statvfs(String path) {
        return session().statvfs(path);
    }

    @Override
    public void mkdir(String path) {
        session().mkdir(path);
    }

    @Override
    public void rename(String from, String to) {
        session().rename(from, to);
    }

    @Override
    public void remove(String path) {
        session().remove(path);
    }

    /**
     * The raw stream, valid only while this session is open.
     *
     * <p>Unlike the pooled client's version, closing the stream does not release the session — this
     * object owns that. It does count, though: a session closed with a stream still open is discarded
     * rather than recycled.
     */
    @Override
    public InputStream download(String remotePath) {
        InputStream in = session().openRead(remotePath);
        openStreams.incrementAndGet();
        return new TrackedInputStream(in);
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        return download(remotePath, target, 0);
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        try (InputStream in = session().openRead(remotePath, fromOffset)) {
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
        try (OutputStream out = session().openWrite(remotePath, mode)) {
            data.transferTo(out);
        } catch (IOException e) {
            throw new SftpException("upload to '" + remotePath + "' failed", e);
        }
    }

    @Override
    public SftpCapabilities capabilities() {
        // The session's answer, not the provider's: inside a session we know what was negotiated.
        return session.capabilities();
    }

    /** Hands the session back — recycled if it is provably clean, discarded otherwise. Idempotent. */
    @Override
    public void close() {
        if (!released.compareAndSet(false, true)) {
            return;
        }
        if (openStreams.get() == 0 && session.isValid()) {
            pool.recycle(session);
        } else {
            pool.discard(session);
        }
    }

    @Override
    public boolean isOpen() {
        return !released.get();
    }

    /**
     * Whether a call routed from the client may go over this session now: open, vouched for by the
     * provider, and not in the middle of a read. Read from whichever thread is routing, which is why
     * every input to it is safe to read across threads.
     */
    boolean acceptsCalls() {
        return !released.get() && openStreams.get() == 0 && session.isValid();
    }

    /**
     * The session, if this handle may still use it.
     *
     * <p>The open-stream rule is enforced rather than documented because breaking it does not fail —
     * it corrupts. JSch keeps one request queue per channel and up to sixteen READs in flight behind
     * an open download; a {@code stat} issued meanwhile reads a DATA packet as its reply, and a second
     * {@code get} resets the queue under the first. {@code upload(dst, download(src))} on one session
     * did exactly that.
     */
    private SftpSession session() {
        if (released.get()) {
            throw new IllegalStateException("this SFTP session is closed");
        }
        if (openStreams.get() > 0) {
            throw new IllegalStateException("a download opened on this session is still being read. "
                    + "Close that stream first, or open a second session for work that has to overlap "
                    + "it: several SFTP clients, JSch among them, cannot interleave a read with other "
                    + "requests on one channel.");
        }
        return session;
    }

    /** Counts itself out on close, once, however many times close is called. */
    private final class TrackedInputStream extends FilterInputStream {

        private boolean closed;

        TrackedInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                super.close();
            } finally {
                openStreams.decrementAndGet();
            }
        }
    }
}

package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Blocking facade over the provider contract: borrow, run one operation, return.
 *
 * <p>Deliberately not implemented by delegating to {@link DefaultReactiveSftpClient} and blocking on
 * the result — that would drag an {@link java.util.concurrent.Executor} into the blocking path and
 * turn every call into a thread hand-off. The two facades share the pool, not the code.
 */
public class DefaultSftpClient implements SftpClient {

    private static final int COPY_BUFFER = 64 * 1024;

    private final SftpProvider provider;
    private final SftpSessionPool<SftpSession> pool;

    public DefaultSftpClient(SftpProvider provider, SftpSessionPool<SftpSession> pool) {
        this.provider = provider;
        this.pool = pool;
    }

    @Override
    public List<RemoteFile> list(String path) {
        return onSession(s -> s.list(path));
    }

    /**
     * Streams the walk through a single borrowed session, rather than materialising first.
     *
     * <p>A visitor that stops early leaves queued entries on the wire, so the session is discarded
     * rather than recycled — same contract as an abandoned transfer.
     */
    @Override
    public void visit(String path, java.util.function.Predicate<RemoteFile> visitor) {
        boolean[] stoppedEarly = {false};
        SftpSession session = pool.borrow();
        try {
            session.list(path, entry -> {
                boolean carryOn = visitor.test(entry);
                stoppedEarly[0] = !carryOn;
                return carryOn;
            });
        } catch (RuntimeException e) {
            pool.discard(session);
            throw e instanceof SftpException ? e : new SftpException("operation failed", e);
        }
        if (stoppedEarly[0]) {
            pool.discard(session);
        } else {
            pool.recycle(session);
        }
    }

    @Override
    public RemoteFile stat(String path) {
        return onSession(s -> s.stat(path));
    }

    @Override
    public RemoteFile lstat(String path) {
        return onSession(s -> s.lstat(path));
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        onSessionVoid(s -> s.symlink(linkPath, targetPath));
    }

    @Override
    public String readlink(String linkPath) {
        return onSession(s -> s.readlink(linkPath));
    }

    @Override
    public String realpath(String path) {
        return onSession(s -> s.realpath(path));
    }

    @Override
    public void setAttributes(String path, io.github.jtsirogiannos.sftp.client.api.AttributeUpdate update) {
        onSessionVoid(s -> s.setAttributes(path, update));
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        onSessionVoid(s -> s.hardlink(linkPath, targetPath));
    }

    @Override
    public io.github.jtsirogiannos.sftp.client.api.FilesystemStats statvfs(String path) {
        return onSession(s -> s.statvfs(path));
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        SftpSession session = pool.borrow();
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
        } finally {
            // Mid-transfer session, same contract as the other transfer paths.
            pool.discard(session);
        }
    }

    @Override
    public boolean exists(String path) {
        return onSession(s -> s.exists(path));
    }

    @Override
    public void mkdir(String path) {
        onSessionVoid(s -> s.mkdir(path));
    }

    @Override
    public void rename(String from, String to) {
        onSessionVoid(s -> s.rename(from, to));
    }

    @Override
    public void remove(String path) {
        onSessionVoid(s -> s.remove(path));
    }

    @Override
    public InputStream download(String remotePath) {
        SftpSession session = pool.borrow();
        InputStream in;
        try {
            in = session.openRead(remotePath);
        } catch (RuntimeException e) {
            pool.discard(session);
            throw e;
        }
        // The session stays checked out until the caller closes the stream — that close is the only
        // thing that releases it, which is why the javadoc on SftpClient.download is emphatic.
        return new SessionBoundInputStream(in, session, pool);
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        try (InputStream in = download(remotePath)) {
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
        SftpSession session = pool.borrow();
        try (OutputStream out = session.openWrite(remotePath, mode)) {
            data.transferTo(out);
        } catch (IOException e) {
            throw new SftpException("upload to '" + remotePath + "' failed", e);
        } finally {
            // Mid-transfer sessions are never recycled, matching the reactive path's contract.
            pool.discard(session);
        }
    }

    @Override
    public SftpCapabilities capabilities() {
        return provider.capabilities();
    }

    @Override
    public void withSessionWithoutResult(java.util.function.Consumer<
            io.github.jtsirogiannos.sftp.client.api.SftpOperations> work) {
        withSession(operations -> {
            work.accept(operations);
            return null;
        });
    }

    /**
     * One borrow for the whole block.
     *
     * <p>Returned to the pool if the block completes, discarded if it throws — a session whose last
     * operation blew up is in an unknown state, and the pool's rule everywhere else is that anything
     * doubtful gets closed rather than handed to the next caller.
     */
    @Override
    public <R> R withSession(java.util.function.Function<
            io.github.jtsirogiannos.sftp.client.api.SftpOperations, R> work) {
        SftpSession session = pool.borrow();
        boolean healthy = false;
        try {
            R result = work.apply(new SessionBoundOperations(session));
            healthy = true;
            return result;
        } catch (RuntimeException e) {
            throw e instanceof SftpException ? e : new SftpException("session work failed", e);
        } finally {
            if (healthy && session.isValid()) {
                pool.recycle(session);
            } else {
                pool.discard(session);
            }
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private <T> T onSession(Function<SftpSession, T> op) {
        SftpSession session = null;
        try {
            session = pool.borrow();
            T value = op.apply(session);
            pool.recycle(session);
            return value;
        } catch (RuntimeException e) {
            if (session != null) {
                pool.discard(session);
            }
            throw e instanceof SftpException ? e : new SftpException("operation failed", e);
        }
    }

    private void onSessionVoid(Consumer<SftpSession> op) {
        onSession(s -> {
            op.accept(s);
            return null;
        });
    }

    /**
     * Ties a borrowed session's lifetime to the stream handed to the caller. Closing the stream
     * discards the session rather than recycling it: a caller who stops reading early leaves unread
     * bytes in the channel, and there is no way from here to tell a complete read from an abandoned
     * one.
     */
    private static final class SessionBoundInputStream extends FilterInputStream {

        private final SftpSession session;
        private final SftpSessionPool<SftpSession> pool;
        private boolean closed;

        SessionBoundInputStream(InputStream in, SftpSession session, SftpSessionPool<SftpSession> pool) {
            super(in);
            this.session = session;
            this.pool = pool;
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
                pool.discard(session);
            }
        }
    }
}

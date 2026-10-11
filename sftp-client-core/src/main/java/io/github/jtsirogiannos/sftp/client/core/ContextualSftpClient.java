package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.context.SessionKey;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A client whose calls join the session current for it, and borrow per call otherwise.
 *
 * <p>The whole of contextual demarcation for the blocking facade, in one place. A subclass supplies
 * two things — how to run a call with no session ({@link #perCall()}) and how to open one
 * ({@link #newSession()}) — and this class decides, call by call, which applies:
 *
 * <ul>
 *   <li>If a session is current under this client's {@link SessionKey} and can take the call, the
 *       call goes over it. That is what lets code deep in a call stack, holding only the injected
 *       client, take part in a unit of work it was never handed.</li>
 *   <li>Otherwise the call borrows per call, exactly as it did before sessions could be current.</li>
 * </ul>
 *
 * <p>"Can take the call" means open, still vouched for by the provider, and not in the middle of a
 * read. A session closed from another thread is therefore never joined even though the binding that
 * names it is still in scope here: a closed session's connection may already be serving someone else.
 *
 * <p>{@link #download(String)} is never routed. Its stream outlives the call, and a stream on the
 * current session would outlive the unit of work it belongs to.
 *
 * <p>Public so that composed clients — a tenant's view of a shared endpoint is one — inherit the same
 * routing rather than re-deriving it.
 */
public abstract class ContextualSftpClient implements SftpClient {

    private final SftpSessionContext context;
    private final SessionKey<SftpClientSession> key;

    /**
     * @param context where the current session is looked for and recorded
     * @param owner   whose connections the sessions are, compared by identity; null means this client
     * @param tenant  the tenant sessions here belong to, or null
     */
    protected ContextualSftpClient(SftpSessionContext context, Object owner, String tenant) {
        this.context = Objects.requireNonNull(context, "context");
        SessionKey<SftpClientSession> base =
                SessionKey.of(SftpClientSession.class, owner == null ? this : owner);
        this.key = tenant == null ? base : base.forTenant(tenant);
    }

    /** Runs calls when no session is current: typically one borrow per call. */
    protected abstract SftpOperations perCall();

    /** Opens a new session — never current, never shared — on a connection of its own. */
    protected abstract SftpClientSession newSession();

    @Override
    public final SftpSessionContext sessionContext() {
        return context;
    }

    /** The key this client's current session is bound under. */
    public final SessionKey<SftpClientSession> sessionKey() {
        return key;
    }

    @Override
    public SftpClientSession openSession() {
        return newSession();
    }

    /**
     * Joins the current session, or opens one and makes it current for the block.
     *
     * <p>Joining is how a nested block, or one reached through a service that knows nothing of the
     * outer one, avoids borrowing a second connection — and on a pool of one, waiting for itself.
     * The participant does nothing at its end; the block that opened the session closes it.
     */
    @Override
    public <R> R withSession(Function<SftpOperations, R> work) {
        Optional<SftpClientSession> current = joinable();
        if (current.isPresent()) {
            return work.apply(current.get());
        }
        try (SftpClientSession session = newSession()) {
            return context.callWithin(key, session, () -> work.apply(session));
        }
    }

    @Override
    public void withSessionWithoutResult(Consumer<SftpOperations> work) {
        withSession(operations -> {
            work.accept(operations);
            return null;
        });
    }

    private Optional<SftpClientSession> joinable() {
        return context.current(key).filter(ContextualSftpClient::acceptsCalls);
    }

    private SftpOperations target() {
        Optional<SftpClientSession> current = joinable();
        return current.isPresent() ? current.get() : perCall();
    }

    /**
     * Whether a call may go over {@code session} right now. Decorated sessions are unwrapped to the
     * pooled session underneath, which is the one that knows about open streams.
     */
    static boolean acceptsCalls(SftpClientSession session) {
        SftpClientSession inner = session;
        while (inner instanceof ForwardingSftpClientSession forwarding) {
            inner = forwarding.session();
        }
        return inner instanceof PooledSftpClientSession pooled
                ? pooled.acceptsCalls()
                : inner.isOpen();
    }

    @Override
    public List<RemoteFile> list(String path) {
        return target().list(path);
    }

    @Override
    public void visit(String path, Predicate<RemoteFile> visitor) {
        target().visit(path, visitor);
    }

    @Override
    public RemoteFile stat(String path) {
        return target().stat(path);
    }

    @Override
    public RemoteFile lstat(String path) {
        return target().lstat(path);
    }

    @Override
    public boolean exists(String path) {
        return target().exists(path);
    }

    @Override
    public String realpath(String path) {
        return target().realpath(path);
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        target().symlink(linkPath, targetPath);
    }

    @Override
    public String readlink(String linkPath) {
        return target().readlink(linkPath);
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        target().hardlink(linkPath, targetPath);
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        target().setAttributes(path, update);
    }

    @Override
    public FilesystemStats statvfs(String path) {
        return target().statvfs(path);
    }

    @Override
    public void mkdir(String path) {
        target().mkdir(path);
    }

    @Override
    public void rename(String from, String to) {
        target().rename(from, to);
    }

    @Override
    public void remove(String path) {
        target().remove(path);
    }

    /** Never routed: the stream outlives the call, and must not outlive the current session with it. */
    @Override
    public InputStream download(String remotePath) {
        return perCall().download(remotePath);
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        return target().download(remotePath, target);
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        return target().download(remotePath, target, fromOffset);
    }

    @Override
    public void upload(String remotePath, InputStream data) {
        target().upload(remotePath, data);
    }

    @Override
    public void upload(String remotePath, InputStream data, SftpSession.WriteMode mode) {
        target().upload(remotePath, data, mode);
    }

    /** What the backend can do. Constant for the client, whatever session a call lands on. */
    @Override
    public SftpCapabilities capabilities() {
        return perCall().capabilities();
    }
}

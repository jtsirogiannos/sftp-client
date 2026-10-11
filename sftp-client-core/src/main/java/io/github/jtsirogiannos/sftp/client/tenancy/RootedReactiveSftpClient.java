package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ContextualReactiveSftpClient;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * A reactive client confined to one directory of another — {@link RootedSftpClient}, for the
 * reactive facade, with the same rules: every path rewritten in and out, anything that could name a
 * path outside refused, a namespace rather than a security boundary.
 *
 * <p>The view owns nothing, so {@link #close()} does nothing; close the underlying client.
 */
public final class RootedReactiveSftpClient extends ContextualReactiveSftpClient {

    private final ReactiveSftpClient base;
    private final RootDirectory root;
    private final RootedReactiveSftpOperations perCall;

    private RootedReactiveSftpClient(ReactiveSftpClient base, RootDirectory root, Object owner,
            String tenant) {
        super(base.sessionContext(), owner, tenant);
        this.base = base;
        this.root = root;
        this.perCall = new RootedReactiveSftpOperations(base, root);
    }

    /**
     * Confines {@code base} to {@code rootDirectory}, which must be absolute and must not be
     * {@code /}.
     */
    public static ReactiveSftpClient of(ReactiveSftpClient base, String rootDirectory) {
        Objects.requireNonNull(base, "base");
        return new RootedReactiveSftpClient(base, RootDirectory.of(rootDirectory), null, null);
    }

    /** A tenant's view: sessions keyed to the underlying client and the tenant. */
    static RootedReactiveSftpClient forTenant(ReactiveSftpClient base, RootDirectory root,
            String tenant) {
        return new RootedReactiveSftpClient(base, root, base, tenant);
    }

    /** The directory this view is confined to, as the underlying client sees it. */
    public String rootDirectory() {
        return root.path();
    }

    @Override
    protected ReactiveSftpOperations perCall() {
        return perCall;
    }

    @Override
    protected ReactiveSftpClientSession newSession() {
        return RootedReactiveSftpOperations.session(
                deferred(base.openSession(), base.capabilities()), root);
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        return base.openSession().thenApply(session -> RootedReactiveSftpOperations.session(session, root));
    }

    /** Owns nothing: the underlying client's pool is not this view's to close. */
    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "RootedReactiveSftpClient[" + root + "]";
    }
}

package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ContextualSftpClient;

import java.util.Objects;

/**
 * A client confined to one directory of another: inside it, {@code /} is that directory.
 *
 * <pre>{@code
 * SftpClient acme = RootedSftpClient.of(client, "/tenants/acme");
 * acme.upload("/inbound/orders.csv", data);   // writes /tenants/acme/inbound/orders.csv
 * acme.list("/");                             // lists /tenants/acme, paths reported as /inbound...
 * }</pre>
 *
 * <p>The SCHEMA strategy of Hibernate's multi-tenancy, for SFTP — and the building block a
 * multi-tenant client uses for it — but useful on its own wherever one component should see one
 * corner of a server. Every path is rewritten on the way in and on the way out, and anything that
 * could name a path outside is refused with {@code SSH_FX_PERMISSION_DENIED}; see
 * {@link RootDirectory} for the rules.
 *
 * <p><strong>This is a namespace, not a security boundary.</strong> It keeps honest code where it
 * belongs. It cannot stop the server following a symlink that something else planted inside the
 * root, and it shares the underlying account with everything else on the connection. Where tenants
 * must not be able to reach each other at all, give each its own credentials and let the server
 * confine them — OpenSSH's {@code ChrootDirectory} — which is the connection-per-tenant strategy.
 *
 * <h2>Sessions</h2>
 *
 * <p>{@code withSession} and {@code openSession} give confined sessions over the underlying client's
 * connections. The view's current session is its own: never the underlying client's, so a call here
 * is always rewritten before it can reach an unconfined session.
 *
 * <p>The view owns nothing, so {@link #close()} does nothing; close the underlying client.
 */
public final class RootedSftpClient extends ContextualSftpClient {

    private final SftpClient base;
    private final RootDirectory root;
    private final RootedSftpOperations perCall;

    private RootedSftpClient(SftpClient base, RootDirectory root, Object owner, String tenant) {
        super(base.sessionContext(), owner, tenant);
        this.base = base;
        this.root = root;
        this.perCall = new RootedSftpOperations(base, root);
    }

    /**
     * Confines {@code base} to {@code rootDirectory}, which must be absolute and must not be
     * {@code /}. Nothing is checked on the server until the first call; a root that does not exist
     * fails then, as {@code no such file}.
     */
    public static SftpClient of(SftpClient base, String rootDirectory) {
        Objects.requireNonNull(base, "base");
        return new RootedSftpClient(base, RootDirectory.of(rootDirectory), null, null);
    }

    /** A tenant's view: sessions keyed to the underlying client and the tenant. */
    static RootedSftpClient forTenant(SftpClient base, RootDirectory root, String tenant) {
        return new RootedSftpClient(base, root, base, tenant);
    }

    /** The directory this view is confined to, as the underlying client sees it. */
    public String rootDirectory() {
        return root.path();
    }

    @Override
    protected SftpOperations perCall() {
        return perCall;
    }

    @Override
    protected SftpClientSession newSession() {
        return RootedSftpOperations.session(base.openSession(), root);
    }

    /** Owns nothing: the underlying client's pool is not this view's to close. */
    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "RootedSftpClient[" + root + "]";
    }
}

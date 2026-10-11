package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ContextualSftpClient;

/**
 * A tenant's view of its own connection, unconfined: the connection is already the tenant's, so
 * there is nothing to rewrite. What it adds is a session key carrying the tenant, and a
 * {@code close()} that leaves the connection to the multi-tenant client that owns it.
 */
final class TenantSftpClient extends ContextualSftpClient {

    private final SftpClient base;

    TenantSftpClient(SftpClient base, String tenant) {
        super(base.sessionContext(), base, tenant);
        this.base = base;
    }

    @Override
    protected SftpOperations perCall() {
        return base;
    }

    @Override
    protected SftpClientSession newSession() {
        return base.openSession();
    }

    /** The multi-tenant client owns the connection. */
    @Override
    public void close() {
    }
}

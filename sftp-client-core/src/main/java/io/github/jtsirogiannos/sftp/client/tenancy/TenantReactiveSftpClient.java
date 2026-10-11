package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ContextualReactiveSftpClient;

import java.util.concurrent.CompletionStage;

/**
 * A tenant's reactive view of its own connection, unconfined — {@link TenantSftpClient}, for the
 * reactive facade: a session key carrying the tenant, and a {@code close()} that leaves the
 * connection to the multi-tenant client that owns it.
 */
final class TenantReactiveSftpClient extends ContextualReactiveSftpClient {

    private final ReactiveSftpClient base;

    TenantReactiveSftpClient(ReactiveSftpClient base, String tenant) {
        super(base.sessionContext(), base, tenant);
        this.base = base;
    }

    @Override
    protected ReactiveSftpOperations perCall() {
        return base;
    }

    @Override
    protected ReactiveSftpClientSession newSession() {
        return deferred(base.openSession(), base.capabilities());
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        return base.openSession();
    }

    /** The multi-tenant client owns the connection. */
    @Override
    public void close() {
    }
}

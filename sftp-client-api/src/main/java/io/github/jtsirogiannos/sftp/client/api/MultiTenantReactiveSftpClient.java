package io.github.jtsirogiannos.sftp.client.api;

/**
 * {@link MultiTenantSftpClient}, for the reactive facade: one client, each call on the current
 * tenant's connection and in its namespace, with the same two isolation strategies and the same
 * refusal to guess when there is no current tenant.
 *
 * <p>The current tenant is asked of the {@link TenantResolver} when each call is made, on the calling
 * thread — the one moment an event-loop request's context is reliably at hand. A
 * {@code withSession} resolves it once, when the session opens.
 */
public interface MultiTenantReactiveSftpClient extends ReactiveSftpClient {

    /** A client for one tenant, whatever the current tenant is; valid across an eviction. */
    ReactiveSftpClient forTenant(String tenantId);

    /** Forgets a tenant: closes its own connections and asks for its definition again on next use. */
    void evictTenant(String tenantId);

    /** Closes every tenant's connections and the shared ones. */
    @Override
    void close();
}

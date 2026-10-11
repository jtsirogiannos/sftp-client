package io.github.jtsirogiannos.sftp.client.api;

/**
 * One client, many tenants: each call lands on the current tenant's connection and in its namespace.
 *
 * <p>Hibernate's multi-tenancy, for SFTP. Each tenant is isolated in one of two ways, or both:
 *
 * <ul>
 *   <li><strong>Its own connection</strong> — host, credentials and pool of its own; Hibernate's
 *       DATABASE strategy. Isolation is the server's job, typically an account per tenant with an
 *       OpenSSH {@code ChrootDirectory}. The right choice when tenants must not be able to reach each
 *       other at all.</li>
 *   <li><strong>Its own root directory</strong> on a shared connection — Hibernate's SCHEMA
 *       strategy. Every path is confined client-side to, say, {@code /tenants/acme}. Far cheaper in
 *       connections; a namespace that keeps honest code in its lane, not a security boundary.</li>
 * </ul>
 *
 * <p>The operations inherited from {@link SftpClient} act for the <em>current</em> tenant, as the
 * configured {@link TenantResolver} reports it at the moment of each call. With no current tenant a
 * call fails rather than falling back to anything unconfined. {@link #forTenant} names one
 * explicitly instead.
 *
 * <h2>Sessions</h2>
 *
 * <p>A session belongs to one tenant. {@code withSession} resolves the tenant once, when it opens,
 * and its current session is keyed to that tenant, so tenants never share one — not even two tenants
 * whose root directories sit on the same connection.
 */
public interface MultiTenantSftpClient extends SftpClient {

    /**
     * A client for one tenant, whatever the current tenant is.
     *
     * <p>Cheap, and safe to keep: the returned client stays valid across {@link #evictTenant}, picking
     * up the tenant's fresh definition on its next call. Its {@code close()} does nothing — the
     * tenants' connections belong to this client.
     */
    SftpClient forTenant(String tenantId);

    /**
     * Forgets a tenant: closes its own connections, if it has any, and asks for its definition again
     * on next use. For offboarding, and for credentials that have been rotated.
     */
    void evictTenant(String tenantId);

    /** Closes every tenant's connections and the shared ones. */
    @Override
    void close();
}

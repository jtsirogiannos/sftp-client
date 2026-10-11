package io.github.jtsirogiannos.sftp.client.api;

import java.util.Optional;

/**
 * Which tenant the work in hand belongs to — Hibernate's {@code CurrentTenantIdentifierResolver}, for
 * SFTP.
 *
 * <p>Asked by a {@link MultiTenantSftpClient} on every call that does not name its tenant, at the
 * moment of the call and on the calling thread, so an implementation can read whatever the
 * application already knows: a request attribute, the security context, a JWT claim, a thread local
 * set by a filter. It must be quick and must not do I/O — it runs once per operation.
 *
 * <p>Empty means "no tenant", and the client then refuses the call rather than guessing: there is
 * no default tenant unless the resolver chooses to return one.
 */
@FunctionalInterface
public interface TenantResolver {

    /** The current tenant's identifier, or empty when there is none. */
    Optional<String> currentTenant();
}

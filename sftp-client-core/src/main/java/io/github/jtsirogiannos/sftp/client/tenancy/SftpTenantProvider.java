package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Turns a tenant identifier into that tenant's isolation — Hibernate's
 * {@code MultiTenantConnectionProvider}, for SFTP.
 *
 * <p>Asked once per tenant, on first use, and again only after
 * {@link io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient#evictTenant}. It may look
 * credentials up in a vault or a database; nothing else waits on it while it does. Returning null
 * means "no such tenant", and the call that asked fails.
 *
 * <pre>{@code
 * // Every tenant in its own directory on the shared connection:
 * SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}")
 *
 * // Every tenant with its own account on the shared host, confined by the server:
 * (tenant, base) -> SftpTenant.connection(base.toBuilder()
 *         .username(tenant)
 *         .password(vault.secretFor(tenant))
 *         .build())
 * }</pre>
 */
@FunctionalInterface
public interface SftpTenantProvider {

    /**
     * @param tenantId the tenant asked for
     * @param base     the multi-tenant client's shared connection, a starting point to derive a
     *                 tenant's own from with {@link SftpConnectionConfig#toBuilder()}; null when the
     *                 client was built without one
     * @return the tenant's isolation, or null if there is no such tenant
     */
    SftpTenant resolve(String tenantId, SftpConnectionConfig base);

    /**
     * Every tenant in a directory named after it, on the shared connection.
     *
     * <p>The identifier becomes a path segment, so it is checked before it gets there: letters,
     * digits, {@code .}, {@code _} and {@code -} only, and never {@code .} or {@code ..}. An
     * identifier read from a request header is attacker-controlled, and without the check
     * {@code ../globex} would be a way into another tenant's directory.
     *
     * @param template an absolute path containing {@code {tenant}}, such as
     *                 {@code /tenants/{tenant}}
     */
    static SftpTenantProvider rootDirectoryTemplate(String template) {
        Objects.requireNonNull(template, "template");
        if (!template.contains(TenantIdentifiers.PLACEHOLDER)) {
            throw new IllegalArgumentException(
                    "a root directory template needs a " + TenantIdentifiers.PLACEHOLDER
                            + " placeholder, got '" + template + "'");
        }
        // Validated once with a harmless identifier, so a bad template fails at configuration time.
        SftpTenant.rootDirectory(template.replace(TenantIdentifiers.PLACEHOLDER, "x"));
        return (tenantId, base) -> SftpTenant.rootDirectory(
                template.replace(TenantIdentifiers.PLACEHOLDER, TenantIdentifiers.asPathSegment(tenantId)));
    }
}

/** The rules for putting a tenant identifier into a path. */
final class TenantIdentifiers {

    static final String PLACEHOLDER = "{tenant}";

    private static final Pattern SAFE_SEGMENT = Pattern.compile("[A-Za-z0-9._-]+");

    private TenantIdentifiers() {
    }

    static String asPathSegment(String tenantId) {
        Objects.requireNonNull(tenantId, "tenantId");
        if (!SAFE_SEGMENT.matcher(tenantId).matches() || ".".equals(tenantId) || "..".equals(tenantId)) {
            throw new IllegalArgumentException("tenant identifier '" + tenantId + "' cannot name a "
                    + "directory: use letters, digits, '.', '_' and '-' only, and not '.' or '..'");
        }
        return tenantId;
    }
}

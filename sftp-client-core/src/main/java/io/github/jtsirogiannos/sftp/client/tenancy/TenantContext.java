package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.TenantResolver;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The current tenant, per thread, for the extent of a call — the default {@link TenantResolver}.
 *
 * <pre>{@code
 * TenantContext.runAs("acme", () -> client.upload("/inbound/orders.csv", data));
 * }</pre>
 *
 * <p>Scoped like the session context and for the same reasons: set for the extent of a call and
 * restored on every way out, never set and forgotten, so a pooled thread cannot carry one request's
 * tenant into the next. A framework application usually has the tenant already — in the request, the
 * security context — and is better served by a {@link TenantResolver} that reads it from there.
 */
public final class TenantContext {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private TenantContext() {
    }

    /** Runs {@code work} as {@code tenantId}, restoring whatever tenant was current before. */
    public static <R> R callAs(String tenantId, Supplier<R> work) {
        Objects.requireNonNull(tenantId, "tenantId");
        String previous = CURRENT.get();
        CURRENT.set(tenantId);
        try {
            return work.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    /** {@link #callAs} for work that returns nothing. */
    public static void runAs(String tenantId, Runnable work) {
        callAs(tenantId, () -> {
            work.run();
            return null;
        });
    }

    /** The tenant current on this thread, if any. */
    public static Optional<String> current() {
        return Optional.ofNullable(CURRENT.get());
    }

    /** A resolver reading this context. */
    public static TenantResolver resolver() {
        return TenantContext::current;
    }
}

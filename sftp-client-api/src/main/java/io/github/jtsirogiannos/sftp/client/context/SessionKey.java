package io.github.jtsirogiannos.sftp.client.context;

import java.util.Objects;
import java.util.Optional;

/**
 * Which session a binding belongs to: the client that owns the connections, optionally narrowed to a
 * tenant, and the type of session stored.
 *
 * <p>The compound key Hibernate Reactive uses for the same job — a base key per session factory,
 * narrowed by tenant identifier — for the same reason: one thread, or one request, routinely holds
 * sessions for several endpoints at once, and for several tenants of one endpoint, and they must
 * never be confused for each other.
 *
 * <p><strong>The owner is compared by identity</strong>, never with {@code equals}. It is the client
 * instance whose pool the session came from, and two clients that happen to be configured alike
 * still own different connections — a blocking client and a Mutiny client for the same endpoint have
 * separate pools, so a session from one can never serve a call on the other.
 *
 * @param <S> the session type bound under this key
 */
public final class SessionKey<S> {

    private final Class<S> type;
    private final Object owner;
    private final String tenant;

    private SessionKey(Class<S> type, Object owner, String tenant) {
        this.type = Objects.requireNonNull(type, "type");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.tenant = tenant;
    }

    /** A key for sessions of {@code type} owned by {@code owner}, for no particular tenant. */
    public static <S> SessionKey<S> of(Class<S> type, Object owner) {
        return new SessionKey<>(type, owner, null);
    }

    /** The same owner and type, narrowed to one tenant. */
    public SessionKey<S> forTenant(String tenantId) {
        Objects.requireNonNull(tenantId, "tenantId");
        if (tenantId.isBlank()) {
            throw new IllegalArgumentException("a tenant identifier must not be blank");
        }
        return new SessionKey<>(type, owner, tenantId);
    }

    public Class<S> type() {
        return type;
    }

    public Object owner() {
        return owner;
    }

    public Optional<String> tenant() {
        return Optional.ofNullable(tenant);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SessionKey<?> that
                && type == that.type
                && owner == that.owner
                && Objects.equals(tenant, that.tenant);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * System.identityHashCode(owner) + type.hashCode()) + Objects.hashCode(tenant);
    }

    @Override
    public String toString() {
        return "SessionKey[" + type.getSimpleName() + " of " + owner.getClass().getSimpleName() + "@"
                + Integer.toHexString(System.identityHashCode(owner))
                + (tenant == null ? "" : ", tenant=" + tenant) + "]";
    }
}

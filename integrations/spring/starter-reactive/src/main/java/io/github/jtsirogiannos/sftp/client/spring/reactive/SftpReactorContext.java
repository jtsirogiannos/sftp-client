package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.context.SessionBindings;

import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.Objects;
import java.util.function.Function;

/**
 * What the Reactor facade keeps in a subscriber's {@link Context}: the current SFTP sessions, and
 * the current tenant.
 *
 * <p>Reactor's context is not ambient — it cannot be read from arbitrary code, only from inside a
 * pipeline — so the library's own session context cannot see it. The facade reads it at
 * subscription and replays it around each call into the library instead, which is the moment the
 * library looks.
 *
 * <pre>{@code
 * client.list("/inbound").contextWrite(SftpReactorContext.tenant("acme"))
 * }</pre>
 */
public final class SftpReactorContext {

    static final Object BINDINGS = new Object() {
        @Override
        public String toString() {
            return "SftpReactorContext.BINDINGS";
        }
    };

    static final Object TENANT = new Object() {
        @Override
        public String toString() {
            return "SftpReactorContext.TENANT";
        }
    };

    private SftpReactorContext() {
    }

    /**
     * Names the tenant for a pipeline over a multi-tenant client: {@code .contextWrite(tenant("acme"))}.
     * Without one, the client's own {@code TenantResolver} decides.
     */
    public static Function<Context, Context> tenant(String tenantId) {
        Objects.requireNonNull(tenantId, "tenantId");
        return context -> context.put(TENANT, tenantId);
    }

    static SessionBindings bindings(ContextView context) {
        return context.getOrDefault(BINDINGS, SessionBindings.empty());
    }
}

package io.github.jtsirogiannos.sftp.client.quarkus.it;

import io.github.jtsirogiannos.sftp.client.api.TenantResolver;

import io.vertx.ext.web.RoutingContext;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Optional;

/**
 * The current tenant is whatever the request says it is — the shape of a real resolver, which would
 * read a verified claim rather than a bare header. Asked on the request's own thread or context when
 * each call is made, so the request-scoped routing context is always the right one.
 */
@ApplicationScoped
public class HeaderTenantResolver implements TenantResolver {

    @Inject
    RoutingContext request;

    @Override
    public Optional<String> currentTenant() {
        return Optional.ofNullable(request.request().getHeader("X-Tenant"));
    }
}

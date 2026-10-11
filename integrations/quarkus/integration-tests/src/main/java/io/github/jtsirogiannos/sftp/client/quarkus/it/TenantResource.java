package io.github.jtsirogiannos.sftp.client.quarkus.it;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.NamedSftp;

import io.smallrye.mutiny.Uni;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * Multi-tenancy and declarative sessions end to end: the tenant from the request, each request one
 * unit of work over the {@code partners} endpoint, every path confined to the tenant's directory.
 */
@Path("/tenant")
public class TenantResource {

    /** Unconfined: onboarding creates the directory a tenant is then confined to. */
    @Inject
    @NamedSftp("it")
    SftpClient operator;

    @Inject
    @NamedSftp("partners")
    SftpClient partners;

    @Inject
    @NamedSftp("partners")
    MutinySftpClient reactivePartners;

    @POST
    @Path("/onboard/{tenant}")
    @Produces(MediaType.TEXT_PLAIN)
    public String onboard(@PathParam("tenant") String tenant) {
        operator.mkdirs("/upload/tenants/" + tenant);
        return "onboarded";
    }

    /** Several calls, one connection: the endpoint has a pool of one. */
    @POST
    @Path("/files/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    @WithSftpSession("partners")
    public String upload(@PathParam("name") String name, String content) {
        partners.mkdirs("/inbound");
        partners.upload("/inbound/" + name, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
        return "session=" + !partners.sessionContext().capture().isEmpty()
                + ",exists=" + partners.exists("/inbound/" + name);
    }

    @GET
    @Path("/files")
    @Produces(MediaType.TEXT_PLAIN)
    public String list() {
        if (!partners.exists("/inbound")) {
            return "";
        }
        return partners.list("/inbound").stream()
                .map(RemoteFile::path)
                .sorted()
                .collect(Collectors.joining(","));
    }

    @GET
    @Path("/reactive/exists/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    @WithSftpSession("partners")
    public Uni<String> reactiveExists(@PathParam("name") String name) {
        return reactivePartners.exists("/inbound")
                .chain(inbound -> inbound ? reactivePartners.exists("/inbound/" + name) : Uni.createFrom().item(false))
                .map(String::valueOf);
    }
}

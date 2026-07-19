package io.github.jtsirogiannos.sftp.client.quarkus.it;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;

import io.smallrye.mutiny.Uni;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * The application under test: a thin HTTP surface over the injected clients.
 *
 * <p>HTTP rather than direct injection because {@code @QuarkusIntegrationTest} runs against a
 * <em>packaged</em> application — a runner jar or a native binary in its own process — where there
 * are no beans to inject into the test. Driving it over the wire is what lets one set of tests
 * verify both.
 *
 * <p>Injection is deliberately unqualified. A single endpoint is configured, and the extension marks
 * a lone endpoint's beans {@code @Default} precisely so this works; the qualified multi-endpoint
 * path is covered by the deployment module's test.
 */
@Path("/sftp")
public class SftpResource {

    @Inject
    SftpClient client;

    @Inject
    MutinySftpClient reactiveClient;

    /** Proves the provider was resolved through ServiceLoader — the thing native image breaks. */
    @GET
    @Path("/capabilities")
    @Produces(MediaType.TEXT_PLAIN)
    public String capabilities() {
        return "v" + client.capabilities().maxSftpVersion()
                + ",pipelined=" + client.capabilities().pipelinedStreaming()
                + ",atomicCreateNew=" + client.capabilities().atomicCreateNew();
    }

    @POST
    @Path("/files/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    public String upload(@PathParam("name") String name, String content) {
        client.upload("/upload/" + name,
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
        return "uploaded";
    }

    @GET
    @Path("/files/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    public String download(@PathParam("name") String name) {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        client.download("/upload/" + name, sink);
        return sink.toString(StandardCharsets.UTF_8);
    }

    @GET
    @Path("/list")
    @Produces(MediaType.TEXT_PLAIN)
    public String list() {
        return client.list("/upload").stream()
                .map(RemoteFile::name)
                .sorted()
                .collect(Collectors.joining(","));
    }

    /** The Mutiny facade, so the reactive path is exercised in native too. */
    @GET
    @Path("/reactive/exists/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> reactiveExists(@PathParam("name") String name) {
        return reactiveClient.exists("/upload/" + name).map(String::valueOf);
    }
}

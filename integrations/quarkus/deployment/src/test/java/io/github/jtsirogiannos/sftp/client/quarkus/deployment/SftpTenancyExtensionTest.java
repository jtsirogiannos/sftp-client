package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.github.jtsirogiannos.sftp.client.tenancy.TenantContext;
import io.quarkus.test.QuarkusUnitTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** A multi-tenant endpoint in a Quarkus application: configured, injected, and confined. */
class SftpTenancyExtensionTest {

    static final EmbeddedSftpServer SERVER = new EmbeddedSftpServer();

    static {
        SERVER.start();
        System.setProperty("sftp-tenancy-test.port", String.valueOf(SERVER.port()));
        try (SftpClient setup = SftpClients.blocking("mina", SERVER.config())) {
            setup.mkdirs("/upload/tenants/acme");
            setup.mkdirs("/upload/tenants/globex");
        }
    }

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .withApplicationRoot(jar -> jar.addClasses(Settlement.class))
            .overrideConfigKey("sftp.endpoints.partners.provider", "mina")
            .overrideConfigKey("sftp.endpoints.partners.host", "localhost")
            .overrideConfigKey("sftp.endpoints.partners.port", "${sftp-tenancy-test.port}")
            .overrideConfigKey("sftp.endpoints.partners.username", EmbeddedSftpServer.USERNAME)
            .overrideConfigKey("sftp.endpoints.partners.password", EmbeddedSftpServer.PASSWORD)
            .overrideConfigKey("sftp.endpoints.partners.host-key-strategy", "INSECURE_ACCEPT_ANY")
            .overrideConfigKey("sftp.endpoints.partners.tenancy.enabled", "true")
            .overrideConfigKey("sftp.endpoints.partners.tenancy.root-directory", "/upload/tenants/{tenant}");

    @ApplicationScoped
    public static class Settlement {

        @Inject
        SftpClient client;

        @WithSftpSession
        public boolean settle() {
            client.mkdir("/settled");
            return !client.sessionContext().capture().isEmpty();
        }
    }

    @Inject
    MultiTenantSftpClient sftp;

    @Inject
    SftpClient plain;

    @Inject
    MutinySftpClient mutiny;

    @Inject
    Settlement settlement;

    /**
     * Checked through the application's own client, confined to the tenant. Not through
     * {@code SERVER} directly: QuarkusUnitTest loads this class twice, once per class loader, so the
     * static block starts a second server that the application never connects to.
     */
    private boolean exists(String tenant, String path) {
        return sftp.forTenant(tenant).exists(path);
    }

    @Test
    void eachTenantIsConfinedToItsDirectory() {
        TenantContext.runAs("acme", () -> sftp.upload("/orders.csv",
                new ByteArrayInputStream("acme's".getBytes(StandardCharsets.UTF_8))));
        sftp.forTenant("globex").mkdir("/inbound");

        assertThat(exists("acme", "/orders.csv")).isTrue();
        assertThat(exists("globex", "/orders.csv")).describedAs("acme's file is not globex's").isFalse();
        assertThat(exists("globex", "/inbound")).isTrue();
        assertThat(sftp.forTenant("acme").list("/")).extracting(RemoteFile::path).contains("/orders.csv");
    }

    @Test
    void theEndpointIsInjectableAsThePlainTypeToo() {
        assertThat(plain).isInstanceOf(MultiTenantSftpClient.class);
    }

    @Test
    void theMutinyClientIsMultiTenantUnderneath() {
        assertThat(mutiny.unwrap()).isInstanceOf(MultiTenantReactiveSftpClient.class);
        TenantContext.runAs("globex", () -> mutiny.mkdir("/from-mutiny").await().indefinitely());
        assertThat(exists("globex", "/from-mutiny")).isTrue();
        assertThat(exists("acme", "/from-mutiny")).isFalse();
    }

    @Test
    void anAnnotatedMethodRunsInTheCurrentTenantsSession() {
        assertThat(TenantContext.callAs("acme", settlement::settle)).isTrue();
        assertThat(exists("acme", "/settled")).isTrue();
        assertThat(exists("globex", "/settled")).isFalse();
    }
}

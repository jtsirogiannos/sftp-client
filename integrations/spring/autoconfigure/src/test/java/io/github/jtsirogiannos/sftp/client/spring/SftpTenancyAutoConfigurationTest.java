package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.TenantResolver;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenant;
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenantProvider;
import io.github.jtsirogiannos.sftp.client.tenancy.TenantContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Multi-tenancy configured through properties and beans: what an application declares, and what it
 * then gets injected.
 */
class SftpTenancyAutoConfigurationTest {

    private EmbeddedSftpServer server;

    @BeforeEach
    void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
        try (SftpClient setup = SftpClients.blocking("mina", server.config())) {
            setup.mkdirs("/upload/tenants/acme");
            setup.mkdirs("/upload/tenants/globex");
        }
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(SftpClientAutoConfiguration.class))
                .withPropertyValues(
                        "sftp.endpoints.partners.provider=mina",
                        "sftp.endpoints.partners.host=localhost",
                        "sftp.endpoints.partners.port=" + server.port(),
                        "sftp.endpoints.partners.username=" + EmbeddedSftpServer.USERNAME,
                        "sftp.endpoints.partners.password=" + EmbeddedSftpServer.PASSWORD,
                        "sftp.endpoints.partners.host-key-strategy=insecure_accept_any",
                        "sftp.endpoints.partners.tenancy.enabled=true");
    }

    private static ByteArrayInputStream text(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private boolean onServer(String path) {
        try (SftpClient check = SftpClients.blocking("mina", server.config())) {
            return check.exists(path);
        }
    }

    @Test
    void aRootDirectoryTemplateConfinesEachTenant() {
        runner().withPropertyValues("sftp.endpoints.partners.tenancy.root-directory=/upload/tenants/{tenant}")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    MultiTenantSftpClient sftp = context.getBean(MultiTenantSftpClient.class);
                    assertThat(context.getBean(SftpClient.class))
                            .describedAs("still injectable as the plain type")
                            .isSameAs(sftp);

                    TenantContext.runAs("acme", () -> sftp.upload("/orders.csv", text("acme's")));
                    sftp.forTenant("globex").upload("/orders.csv", text("globex's"));

                    assertThat(onServer("/upload/tenants/acme/orders.csv")).isTrue();
                    assertThat(onServer("/upload/tenants/globex/orders.csv")).isTrue();
                    assertThat(sftp.forTenant("acme").list("/")).extracting(RemoteFile::path)
                            .containsExactly("/orders.csv");
                });
    }

    @Test
    void theReactiveBeanIsMultiTenantToo() {
        runner().withPropertyValues("sftp.endpoints.partners.tenancy.root-directory=/upload/tenants/{tenant}")
                .run(context -> {
                    ReactiveSftpClient reactive = context.getBean(ReactiveSftpClient.class);
                    assertThat(reactive).isInstanceOf(MultiTenantReactiveSftpClient.class);
                    ((MultiTenantReactiveSftpClient) reactive).forTenant("acme").mkdir("/reactive")
                            .toCompletableFuture().get();
                    assertThat(onServer("/upload/tenants/acme/reactive")).isTrue();
                });
    }

    @Test
    void aTenantResolverBeanDecidesTheCurrentTenant() {
        runner().withPropertyValues("sftp.endpoints.partners.tenancy.root-directory=/upload/tenants/{tenant}")
                .withBean(TenantResolver.class, () -> () -> Optional.of("globex"))
                .run(context -> {
                    context.getBean(SftpClient.class).mkdir("/resolved");
                    assertThat(onServer("/upload/tenants/globex/resolved")).isTrue();
                });
    }

    @Test
    void aTenantProviderBeanDecidesIsolation() {
        runner().withBean("sftpTenantProvider-partners", SftpTenantProvider.class,
                        () -> (tenant, base) -> SftpTenant.connection(base)
                                .withRootDirectory("/upload/tenants/" + tenant))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    context.getBean(MultiTenantSftpClient.class).forTenant("acme").mkdir("/own-connection");
                    assertThat(onServer("/upload/tenants/acme/own-connection")).isTrue();
                });
    }

    @Test
    void tenancyWithNothingToSayHowTenantsAreIsolatedStopsTheContext() {
        runner().run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining("sftp.endpoints.partners is misconfigured")
                    .hasMessageContaining("tenancy.root-directory");
        });
    }

    public static class Settlement {

        private final SftpClient client;

        public Settlement(SftpClient client) {
            this.client = client;
        }

        @WithSftpSession("partners")
        public boolean settle() {
            client.mkdir("/settled");
            return !client.sessionContext().capture().isEmpty();
        }
    }

    @Test
    void anAnnotatedMethodRunsInTheCurrentTenantsSession() {
        runner().withPropertyValues("sftp.endpoints.partners.tenancy.root-directory=/upload/tenants/{tenant}")
                .withBean(Settlement.class)
                .run(context -> {
                    Settlement settlement = context.getBean(Settlement.class);
                    assertThat(TenantContext.callAs("acme", settlement::settle)).isTrue();
                    assertThat(onServer("/upload/tenants/acme/settled")).isTrue();
                    assertThat(onServer("/upload/tenants/globex/settled")).isFalse();
                });
    }
}

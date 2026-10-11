package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.DefaultSftpClient;
import io.github.jtsirogiannos.sftp.client.core.ObservedProvider;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Many tenants through one client: both isolation strategies, the current tenant, sessions per
 * tenant, eviction, and the per-tenant decorator. Against the in-memory provider, observed, so the
 * test can see which account each connection was opened as.
 */
public class MultiTenantSftpClientTest {

    private ObservedProvider provider;
    private SftpClient server;
    private final List<MultiTenantSftpClient> built = new CopyOnWriteArrayList<>();

    @BeforeMethod
    public void setUp() {
        provider = new ObservedProvider();
        server = new DefaultSftpClient(provider, provider.pool(4));
        server.mkdirs("/tenants/acme");
        server.mkdirs("/tenants/globex");
    }

    @AfterMethod
    public void tearDown() {
        built.forEach(MultiTenantSftpClient::close);
        server.close();
    }

    private MultiTenantSftpClient confined() {
        return track(MultiTenantSftpClientBuilder.create(provider, ObservedProvider.config())
                .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
                .build());
    }

    private MultiTenantSftpClient ownConnections(PoolSettings tenantPool) {
        return track(MultiTenantSftpClientBuilder.create(provider, ObservedProvider.config())
                .tenants((tenant, base) -> SftpTenant.connection(base.toBuilder().username(tenant).build()))
                .tenantPool(tenantPool)
                .build());
    }

    private MultiTenantSftpClient track(MultiTenantSftpClient client) {
        built.add(client);
        return client;
    }

    // --- root directory per tenant ---

    @Test
    public void eachConfinedTenantSeesOnlyItsOwnDirectory() {
        MultiTenantSftpClient sftp = confined();

        TenantContext.runAs("acme", () -> sftp.upload("/orders.csv", text("acme's")));
        TenantContext.runAs("globex", () -> sftp.upload("/orders.csv", text("globex's")));

        assertThat(server.exists("/tenants/acme/orders.csv")).isTrue();
        assertThat(server.exists("/tenants/globex/orders.csv")).isTrue();
        assertThat(sftp.forTenant("acme").list("/")).extracting(RemoteFile::path)
                .containsExactly("/orders.csv");
        assertThat(read(sftp.forTenant("globex"), "/orders.csv")).isEqualTo("globex's");
    }

    @Test
    public void withNoCurrentTenantACallFailsRatherThanFallingBackToAnything() {
        MultiTenantSftpClient sftp = confined();

        assertThatThrownBy(() -> sftp.list("/"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No current SFTP tenant");
    }

    @Test
    public void anIdentifierThatCouldNameAnotherDirectoryIsRefused() {
        MultiTenantSftpClient sftp = confined();

        assertThatThrownBy(() -> sftp.forTenant("../globex").list("/"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sftp.forTenant("acme/../globex").list("/"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void confinedTenantsShareOneConnection() {
        MultiTenantSftpClient sftp = confined();
        int before = provider.sessions.size();

        sftp.forTenant("acme").mkdir("/a");
        sftp.forTenant("globex").mkdir("/g");
        sftp.forTenant("acme").exists("/a");

        assertThat(provider.sessions.size() - before)
                .describedAs("the SCHEMA strategy: one pool, one connection reused")
                .isEqualTo(1);
    }

    // --- connection per tenant ---

    @Test
    public void tenantsWithTheirOwnConnectionNeverShareOne() {
        MultiTenantSftpClient sftp = ownConnections(PoolSettings.defaults().withMaxSize(2));

        sftp.forTenant("acme").mkdir("/acme-dir");
        sftp.forTenant("globex").mkdir("/globex-dir");
        sftp.forTenant("acme").exists("/acme-dir");

        assertThat(provider.sessions).extracting(observed -> observed.username)
                .contains("acme", "globex");
        assertThat(provider.sessions.stream().filter(o -> "acme".equals(o.username)).count())
                .describedAs("acme's pool reused acme's connection")
                .isEqualTo(1);
    }

    @Test
    public void bothStrategiesCompose() {
        MultiTenantSftpClient sftp = track(MultiTenantSftpClientBuilder.create(provider, ObservedProvider.config())
                .tenants((tenant, base) -> SftpTenant.connection(base.toBuilder().username(tenant).build())
                        .withRootDirectory("/tenants/" + tenant))
                .build());

        sftp.forTenant("acme").upload("/both.txt", text("x"));

        assertThat(server.exists("/tenants/acme/both.txt")).isTrue();
        assertThat(provider.last().username).isEqualTo("acme");
    }

    @Test
    public void evictionClosesATenantsConnectionsAndAKeptHandleCarriesOn() {
        MultiTenantSftpClient sftp = ownConnections(PoolSettings.defaults().withMaxSize(2));
        SftpClient acme = sftp.forTenant("acme");
        acme.mkdir("/before");
        ObservedProvider.Observed first = provider.sessions.stream()
                .filter(o -> "acme".equals(o.username)).findFirst().orElseThrow();

        sftp.evictTenant("acme");

        assertThat(first.closed).describedAs("its pool closed with it").isTrue();
        assertThat(acme.exists("/before")).describedAs("the kept handle simply reconnects").isTrue();
        assertThat(provider.sessions.stream().filter(o -> "acme".equals(o.username)).count())
                .isEqualTo(2);
    }

    @Test
    public void evictionAsksForTheDefinitionAgain() {
        AtomicInteger asked = new AtomicInteger();
        MultiTenantSftpClient sftp = track(MultiTenantSftpClientBuilder.create(provider, ObservedProvider.config())
                .tenants((tenant, base) -> {
                    asked.incrementAndGet();
                    return SftpTenant.rootDirectory("/tenants/" + tenant);
                })
                .build());

        sftp.forTenant("acme").exists("/");
        sftp.forTenant("acme").exists("/");
        sftp.evictTenant("acme");
        sftp.forTenant("acme").exists("/");

        assertThat(asked.get()).isEqualTo(2);
    }

    @Test
    public void quietTenantsParkedConnectionsAreSweptOnOtherTenantsTraffic() throws Exception {
        MultiTenantSftpClient sftp = ownConnections(
                PoolSettings.defaults().withMaxSize(2).withMaxIdle(Duration.ofMillis(60)));
        sftp.forTenant("acme").mkdir("/a");
        ObservedProvider.Observed acmes = provider.last();

        Thread.sleep(150); // acme goes quiet past maxIdle
        sftp.forTenant("globex").mkdir("/g");

        assertThat(acmes.closed).describedAs("closed on globex's traffic, not left open").isTrue();
    }

    @Test
    public void anUnknownTenantFails() {
        MultiTenantSftpClient sftp = track(MultiTenantSftpClientBuilder.create(provider, ObservedProvider.config())
                .tenants((tenant, base) -> "acme".equals(tenant) ? SftpTenant.rootDirectory("/tenants/acme") : null)
                .build());

        assertThatThrownBy(() -> sftp.forTenant("initech").list("/"))
                .isInstanceOf(SftpException.class)
                .hasMessageContaining("unknown SFTP tenant 'initech'");
        assertThat(sftp.forTenant("acme").exists("/")).isTrue();
    }

    @Test
    public void aConfinedTenantNeedsASharedConnection() {
        MultiTenantSftpClient sftp = track(MultiTenantSftpClientBuilder.create(provider, null)
                .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
                .build());

        assertThatThrownBy(() -> sftp.forTenant("acme").list("/"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without a shared one");
    }

    // --- sessions ---

    @Test
    public void aSessionBelongsToOneTenantAndCallsForItJoin() {
        MultiTenantSftpClient sftp = confined();
        int before = provider.sessions.size();

        TenantContext.runAs("acme", () -> sftp.withSessionWithoutResult(ops -> {
            ops.mkdir("/in-session");
            sftp.upload("/in-session/joined.txt", text("j")); // the current tenant's call joins
        }));

        assertThat(server.exists("/tenants/acme/in-session/joined.txt")).isTrue();
        assertThat(provider.sessions.size() - before).isLessThanOrEqualTo(1);
    }

    @Test
    public void anotherTenantInsideTheBlockDoesNotJoinIt() {
        MultiTenantSftpClient sftp = confined();

        TenantContext.runAs("acme", () -> sftp.withSessionWithoutResult(ops -> {
            int before = provider.sessions.size();
            sftp.forTenant("globex").mkdir("/globex-during-acme");
            assertThat(provider.sessions.size() - before)
                    .describedAs("globex borrowed its own; acme's session stayed acme's")
                    .isEqualTo(1);
        }));

        assertThat(server.exists("/tenants/globex/globex-during-acme")).isTrue();
        assertThat(server.exists("/tenants/acme/globex-during-acme")).isFalse();
    }

    // --- decoration and lifecycle ---

    @Test
    public void theDecoratorWrapsEachTenantOnceWithItsIdentifier() {
        List<String> decorated = new CopyOnWriteArrayList<>();
        MultiTenantSftpClient sftp = track(MultiTenantSftpClientBuilder.create(provider, ObservedProvider.config())
                .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
                .decorate((tenant, client) -> {
                    decorated.add(tenant);
                    return client;
                })
                .build());

        sftp.forTenant("acme").exists("/");
        sftp.forTenant("acme").exists("/");
        sftp.forTenant("globex").exists("/");

        assertThat(decorated).containsExactly("acme", "globex");
    }

    @Test
    public void aCustomResolverDecidesTheCurrentTenant() {
        MultiTenantSftpClient sftp = track(MultiTenantSftpClientBuilder.create(provider, ObservedProvider.config())
                .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
                .tenantResolver(() -> Optional.of("globex"))
                .build());

        sftp.mkdir("/resolved");

        assertThat(server.exists("/tenants/globex/resolved")).isTrue();
    }

    @Test
    public void closingATenantsClientIsHarmlessAndClosingTheWholeClosesEverything() {
        MultiTenantSftpClient sftp = ownConnections(PoolSettings.defaults().withMaxSize(2));
        SftpClient acme = sftp.forTenant("acme");
        acme.mkdir("/x");

        acme.close();
        assertThat(acme.exists("/x")).describedAs("a tenant's client owns nothing").isTrue();

        sftp.close();
        assertThat(provider.sessions.stream().filter(o -> "acme".equals(o.username))
                .collect(Collectors.toList())).allMatch(o -> o.closed);
        assertThatThrownBy(() -> acme.exists("/x")).isInstanceOf(IllegalStateException.class);
    }

    private static String read(SftpClient client, String path) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        client.download(path, out);
        return out.toString(StandardCharsets.UTF_8);
    }

    private static InputStream text(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}

package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.DefaultReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.core.DefaultSftpClient;
import io.github.jtsirogiannos.sftp.client.core.ObservedProvider;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.provider.inmemory.InMemorySftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Confinement and tenancy on the reactive facade: the same rules as the blocking one, with every
 * refusal a failed stage rather than a throw.
 */
public class ReactiveTenancyTest {

    private ExecutorService worker;
    private ObservedProvider provider;
    private BoundedSftpSessionPool<SftpSession> pool;
    private SftpClient server;
    private ReactiveSftpClient reactive;

    @BeforeMethod
    public void setUp() {
        worker = Executors.newFixedThreadPool(4);
        provider = new ObservedProvider();
        pool = provider.pool(4);
        server = new DefaultSftpClient(provider, provider.pool(2));
        server.mkdirs("/tenants/acme");
        server.mkdirs("/tenants/globex");
        server.upload("/tenants/globex/secret.txt",
                new java.io.ByteArrayInputStream("globex only".getBytes(StandardCharsets.UTF_8)));
        reactive = new DefaultReactiveSftpClient(provider, pool, worker);
    }

    @AfterMethod
    public void tearDown() {
        reactive.close();
        server.close();
        worker.shutdownNow();
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    // --- a confined view ---

    @Test
    public void aRootedViewMapsInAndOutAndFailsStagesRatherThanThrowing() throws Exception {
        ReactiveSftpClient acme = RootedReactiveSftpClient.of(reactive, "/tenants/acme");

        await(acme.mkdirs("/inbound/2026"));
        await(acme.upload("/inbound/2026/orders.csv", text("a,b")));

        assertThat(server.exists("/tenants/acme/inbound/2026/orders.csv")).isTrue();
        assertThat(await(acme.list("/inbound/2026"))).extracting(RemoteFile::path)
                .containsExactly("/inbound/2026/orders.csv");
        assertThat(await(acme.realpath("inbound/../inbound"))).isEqualTo("/inbound");

        CompletionStage<Boolean> escape = acme.exists("/../globex/secret.txt");
        assertThat(escape.toCompletableFuture()).describedAs("a stage, not a throw").isCompletedExceptionally();
        assertThatThrownBy(() -> await(escape)).hasCauseInstanceOf(SftpException.class);
        assertThat(await(collectFailure(acme.download("/../globex/secret.txt"))))
                .isInstanceOf(SftpException.class);
    }

    @Test
    public void aRootedViewsUnitOfWorkIsConfinedAndJoinedByTheView() throws Exception {
        ReactiveSftpClient acme = RootedReactiveSftpClient.of(reactive, "/tenants/acme");

        List<RemoteFile> listed = await(acme.withSession(ops -> acme.mkdir("/joined")
                .thenCompose(v -> ops.upload("/joined/f.txt", text("x")))
                .thenCompose(v -> ops.list("/joined"))));

        assertThat(listed).extracting(RemoteFile::path).containsExactly("/joined/f.txt");
        assertThat(server.exists("/tenants/acme/joined/f.txt")).isTrue();

        var session = await(acme.openSession());
        assertThat(await(session.stat("/joined")).path()).isEqualTo("/joined");
        assertThatThrownBy(() -> await(session.stat("/../globex"))).hasCauseInstanceOf(SftpException.class);
        await(session.close());
    }

    // --- many tenants ---

    private MultiTenantReactiveSftpClient confined(
            io.github.jtsirogiannos.sftp.client.spi.SftpProvider over) {
        return MultiTenantSftpClientBuilder.create(over, ObservedProvider.config())
                .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
                .tenantResolver(() -> Optional.ofNullable(CURRENT.get()))
                .buildReactive(worker);
    }

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    @Test
    public void eachTenantSeesOnlyItsOwnDirectoryAndNoTenantFailsTheStage() throws Exception {
        try (MultiTenantReactiveSftpClient sftp = confined(provider)) {
            await(sftp.forTenant("acme").upload("/orders.csv", text("acme's")));
            await(sftp.forTenant("globex").upload("/orders.csv", text("globex's")));

            assertThat(server.exists("/tenants/acme/orders.csv")).isTrue();
            assertThat(await(sftp.forTenant("acme").list("/"))).extracting(RemoteFile::path)
                    .containsExactly("/orders.csv");

            CompletionStage<List<RemoteFile>> noTenant = sftp.list("/");
            assertThatThrownBy(() -> await(noTenant)).hasCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("No current SFTP tenant");
        }
    }

    @Test
    public void theCurrentTenantIsResolvedWhenTheCallIsMade() throws Exception {
        try (MultiTenantReactiveSftpClient sftp = confined(provider)) {
            CURRENT.set("globex");
            CompletionStage<Void> made;
            try {
                made = sftp.mkdir("/resolved-on-call");
            } finally {
                CURRENT.remove();
            }
            await(made);

            assertThat(server.exists("/tenants/globex/resolved-on-call")).isTrue();
        }
    }

    /** Over a provider that opens thread-free sessions, both strategies at once. */
    @Test
    public void aThreadFreeProviderServesTenantsToo() throws Exception {
        InMemorySftpProvider inMemory = new InMemorySftpProvider();
        try (MultiTenantReactiveSftpClient sftp = MultiTenantSftpClientBuilder
                .create(inMemory, ObservedProvider.config())
                .tenants((tenant, base) -> "operator".equals(tenant)
                        ? SftpTenant.connection(base) // its own connection, unconfined
                        : SftpTenant.rootDirectory("/tenants/" + tenant))
                .buildReactive(worker)) {
            await(sftp.forTenant("operator").mkdirs("/tenants/acme"));

            await(sftp.forTenant("acme").withSession(ops -> ops.upload("/a.txt", text("x"))));

            assertThat(await(sftp.forTenant("operator").exists("/tenants/acme/a.txt"))).isTrue();
            assertThat(await(sftp.forTenant("acme").list("/"))).extracting(RemoteFile::path)
                    .containsExactly("/a.txt");
        }
    }

    @Test
    public void tenantsEachSessionStaysTheirs() throws Exception {
        try (MultiTenantReactiveSftpClient sftp = confined(provider)) {
            await(sftp.forTenant("acme").withSession(ops -> sftp.forTenant("globex").mkdir("/g")
                    .thenCompose(v -> ops.mkdir("/a"))));

            assertThat(server.exists("/tenants/globex/g")).isTrue();
            assertThat(server.exists("/tenants/acme/a")).isTrue();
            assertThat(server.exists("/tenants/acme/g")).isFalse();
        }
    }

    @Test
    public void evictionReconnectsAKeptHandle() throws Exception {
        try (MultiTenantReactiveSftpClient sftp = MultiTenantSftpClientBuilder
                .create(provider, ObservedProvider.config())
                .tenants((tenant, base) -> SftpTenant.connection(base.toBuilder().username(tenant).build())
                        .withRootDirectory("/tenants/" + tenant))
                .buildReactive(worker)) {
            ReactiveSftpClient acme = sftp.forTenant("acme");
            await(acme.mkdir("/before"));
            ObservedProvider.Observed first = provider.sessions.stream()
                    .filter(o -> "acme".equals(o.username)).findFirst().orElseThrow();

            sftp.evictTenant("acme");

            assertThat(first.closed).isTrue();
            assertThat(await(acme.exists("/before"))).isTrue();
        }
    }

    private static Flow.Publisher<ByteBuffer> text(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private final AtomicBoolean sent = new AtomicBoolean();

            @Override
            public void request(long n) {
                if (sent.compareAndSet(false, true)) {
                    subscriber.onNext(ByteBuffer.wrap(bytes));
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                sent.set(true);
            }
        });
    }

    private static CompletableFuture<Throwable> collectFailure(Flow.Publisher<ByteBuffer> publisher) {
        CompletableFuture<Throwable> failure = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
            }

            @Override
            public void onError(Throwable throwable) {
                failure.complete(throwable);
            }

            @Override
            public void onComplete() {
                failure.complete(null);
            }
        });
        return failure;
    }
}

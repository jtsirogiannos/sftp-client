package io.github.jtsirogiannos.sftp.client.samples;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenant;
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenantProvider;
import io.github.jtsirogiannos.sftp.client.tenancy.TenantContext;
import io.github.jtsirogiannos.sftp.client.micrometer.MicrometerSftpClient;
import io.github.jtsirogiannos.sftp.client.micrometer.SftpPoolMetrics;
import io.github.jtsirogiannos.sftp.client.opentelemetry.OpenTelemetrySftpClient;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.OpenTelemetry;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Flow;

/**
 * Every Java snippet in the root README, as real code. None of these methods is called — the value
 * is entirely in the compile: if a facade method is renamed or removed, this module fails to build,
 * which is the signal the README has gone stale. {@code ReadmeSamplesInSyncTest} enforces the
 * converse, that the README's lines are actually present here.
 *
 * <p>Each method body is copied verbatim from one fenced {@code java} block. Keep them that way — the
 * in-sync test compares line by line, and the point is defeated the moment the two drift. Every
 * method over-declares {@code throws Exception} so the sample bodies need no try/catch noise; they
 * never run, so it costs nothing.
 */
@SuppressWarnings("unused")
final class ReadmeSamples {

    private ReadmeSamples() {
    }

    // "Using it" — plain Java, no framework.
    static void blockingUsage(SftpConnectionConfig config) throws Exception {
        try (var client = SftpClients.blocking("mina", config)) {
            client.list("/inbound").forEach(System.out::println);
        }
    }

    // The convenience operations on SftpClient.
    static void convenienceOperations(SftpClient client) throws Exception {
        client.mkdirs("/outbound/2026/07/18");           // mkdir -p
        client.upload(Path.of("report.csv"), "/outbound/2026/07/18/report.csv");
        client.list("/inbound", f -> f.name().endsWith(".csv"));
        client.deleteRecursively("/outbound/2026/06");   // symlinks removed, never followed
        client.chmod("/outbound/report.csv", 0640);
        client.findFirst("/inbound", f -> f.name().startsWith("SETTLE"));
    }

    // Publishing a file nobody can read half of.
    static void atomicUpload(SftpClient client, InputStream data) throws Exception {
        client.uploadAtomically("/inbound/orders.csv", data);
    }

    // One connection for a block of work.
    static void withSession(SftpClient client, InputStream data) throws Exception {
        client.withSessionWithoutResult(work -> {          // withSession(...) if you need a return value
            work.mkdirs("/outbound/2026/07");
            work.uploadAtomically("/outbound/2026/07/report.csv", data);
            work.rename("/outbound/2026/07/old.csv", "/archive/old.csv");
        });
    }

    // The block's session is current: calls on the client itself join it.
    static void currentSession(SftpClient client, Archiver archiver) throws Exception {
        client.withSessionWithoutResult(work -> {
            work.mkdirs("/outbound/2026/07");
            archiver.archive("/outbound/2026/07");   // calls client.rename(...) — the same connection
        });
    }

    // The explicit form.
    static void openSession(SftpClient client, Path report) throws Exception {
        try (SftpClientSession session = client.openSession()) {
            session.mkdirs("/outbound/2026/10");
            session.upload("/outbound/2026/10/report.csv", Files.newInputStream(report));
        }
    }

    // Declaratively.
    static final class Settlements {

        private SftpClient client;

        @WithSftpSession("bank-a")
        public void settle(String day) {
            client.mkdirs("/settlements/" + day);                    // the injected client
            client.upload("/settlements/" + day + "/batch.csv", batch());
        }

        private InputStream batch() {
            return InputStream.nullInputStream();
        }
    }

    // On the reactive facade.
    static void reactiveSession(ReactiveSftpClient reactive, Flow.Publisher<ByteBuffer> chunks)
            throws Exception {
        reactive.withSession(ops -> ops.mkdirs("/outbound/2026/10")
                .thenCompose(done -> ops.upload("/outbound/2026/10/report.csv", chunks)));
    }

    // Many tenants through one client.
    static void multiTenant(SftpConnectionConfig sharedConfig, InputStream data, Vault vault)
            throws Exception {
        MultiTenantSftpClient sftp = SftpClients.multiTenant("mina", sharedConfig)
                .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
                .build();

        TenantContext.runAs("acme", () -> sftp.upload("/inbound/orders.csv", data));   // /tenants/acme/inbound/orders.csv
        sftp.forTenant("globex").list("/inbound");

        MultiTenantSftpClient perTenant = SftpClients.multiTenant("mina", sharedConfig)
                .tenants((tenant, base) -> SftpTenant.connection(base.toBuilder()
                        .username(tenant)
                        .password(vault.secretFor(tenant))
                        .build()))
                .build();
    }

    /** Stands in for an application service that was handed the injected client. */
    interface Archiver {
        void archive(String directory);
    }

    /** Stands in for wherever an application keeps per-tenant credentials. */
    interface Vault {
        char[] secretFor(String tenant);
    }

    // Sizing the pool, before production sizes it for you.
    static void poolSizing(SftpConnectionConfig config) throws Exception {
        PoolSettings pool = PoolSettings.defaults()
                .withMaxSize(24)                              // concurrent operations per endpoint
                .withAcquireTimeout(Duration.ofSeconds(5));   // fail fast rather than queue

        try (var client = SftpClients.blocking("mina", config, pool)) {
            // ... use client
        }
    }

    // The reactive facade carries the same operations, plus explicit write semantics and resumable reads.
    static void reactiveOverloads(ReactiveSftpClient client, Flow.Publisher<ByteBuffer> chunks,
            long resumeFrom) throws Exception {
        client.upload("/inbound/log.txt", chunks, WriteMode.APPEND);
        client.download("/inbound/big.csv", resumeFrom);            // skip bytes already fetched
    }

    // Testing your own code — the in-memory provider.
    static void inMemory(SftpConnectionConfig config) throws Exception {
        try (var client = SftpClients.blocking("inmemory", config)) {
            // ... use client
        }
    }

    // Observability — both facades can be wrapped.
    static void observability(SftpClient client, MeterRegistry registry, SftpConnectionConfig config,
            OpenTelemetry openTelemetry, BoundedSftpSessionPool<?> pool) throws Exception {
        SftpClient metered = MicrometerSftpClient.wrap(client, registry, config);
        SftpClient traced  = OpenTelemetrySftpClient.wrap(client, openTelemetry, config);
        SftpPoolMetrics.bind(pool, registry, Tags.of("endpoint", "bank-a"));
    }
}

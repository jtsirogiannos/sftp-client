package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.MultiTenantReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.TenantResolver;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.core.AsyncReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.core.DefaultReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A reactive client per tenant, each a confined or own-connection view, all owned here and kept in a
 * {@link TenantRegistry} — {@link DefaultMultiTenantSftpClient}, for the reactive facade.
 */
final class DefaultMultiTenantReactiveSftpClient implements MultiTenantReactiveSftpClient {

    private final SftpProvider provider;
    private final ReactiveSftpClient shared;
    private final PoolSettings tenantPool;
    private final SftpSessionContext context;
    private final Executor worker;
    private final TenantResolver resolver;
    private final BiFunction<String, ReactiveSftpClient, ReactiveSftpClient> decorator;
    private final TenantRegistry<ReactiveSftpClient> tenants;
    private final ConcurrentMap<String, TenantHandle> handles = new ConcurrentHashMap<>();

    DefaultMultiTenantReactiveSftpClient(SftpProvider provider, SftpConnectionConfig sharedConfig,
            PoolSettings sharedPool, PoolSettings tenantPool, SftpSessionContext context,
            Executor worker, TenantResolver resolver, SftpTenantProvider definitions,
            BiFunction<String, ReactiveSftpClient, ReactiveSftpClient> decorator) {
        this.provider = provider;
        this.tenantPool = tenantPool;
        this.context = context;
        this.worker = worker;
        this.resolver = resolver;
        this.decorator = decorator;
        this.shared = sharedConfig == null ? null : open(sharedConfig, sharedPool).client();
        this.tenants = new TenantRegistry<>(definitions, sharedConfig, tenantPool, this::create);
    }

    private record Opened(ReactiveSftpClient client, BoundedSftpSessionPool<?> pool) {
    }

    /** The same choice SftpClients makes: thread-free sessions where the provider can open them. */
    private Opened open(SftpConnectionConfig config, PoolSettings pool) {
        if (provider instanceof AsyncSftpProvider async) {
            var sessions = BoundedSftpSessionPool.async(async, config, pool.maxSize(), pool.maxIdle(),
                    pool.acquireTimeout());
            return new Opened(new AsyncReactiveSftpClient(async, sessions, worker, context), sessions);
        }
        var sessions = BoundedSftpSessionPool.blocking(provider, config, pool.maxSize(), pool.maxIdle(),
                pool.acquireTimeout());
        return new Opened(new DefaultReactiveSftpClient(provider, sessions, worker, context), sessions);
    }

    private TenantRegistry.Tenant<ReactiveSftpClient> create(String tenantId, SftpTenant definition) {
        Opened own = null;
        ReactiveSftpClient base;
        if (definition.connection().isPresent()) {
            own = open(definition.connection().get(), definition.pool().orElse(tenantPool));
            base = own.client();
        } else if (shared != null) {
            base = shared;
        } else {
            throw new IllegalStateException("SFTP tenant '" + tenantId + "' has no connection of its "
                    + "own, and this client was built without a shared one to confine it on");
        }
        ReactiveSftpClient view = definition.rootDirectory()
                .<ReactiveSftpClient>map(root ->
                        RootedReactiveSftpClient.forTenant(base, RootDirectory.of(root), tenantId))
                .orElseGet(() -> new TenantReactiveSftpClient(base, tenantId));
        return new TenantRegistry.Tenant<>(decorator.apply(tenantId, view),
                own == null ? null : own.client(), own == null ? null : own.pool());
    }

    // --- tenants ---

    @Override
    public ReactiveSftpClient forTenant(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("a tenant identifier must not be blank");
        }
        return handles.computeIfAbsent(tenantId, TenantHandle::new);
    }

    @Override
    public void evictTenant(String tenantId) {
        tenants.evict(tenantId);
    }

    /**
     * The current tenant's client, resolved now. A missing tenant fails the stage the caller gets,
     * never throws: a caller composing stages has no way to catch a synchronous throw.
     */
    private <T> CompletionStage<T> current(Function<ReactiveSftpClient, CompletionStage<T>> call) {
        ReactiveSftpClient client;
        try {
            client = forTenant(resolver.currentTenant().orElseThrow(() -> new IllegalStateException(
                    "No current SFTP tenant: the TenantResolver returned none. Configure one that "
                            + "knows the current request's tenant, or name one with forTenant(...).")));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return call.apply(client);
    }

    // --- the current tenant's operations ---

    @Override
    public SftpSessionContext sessionContext() {
        return context;
    }

    @Override
    public SftpCapabilities capabilities() {
        return provider.capabilities();
    }

    @Override
    public <R> CompletionStage<R> withSession(
            Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work) {
        return current(client -> client.withSession(work));
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        return current(ReactiveSftpClient::openSession);
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return current(client -> client.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return current(client -> client.stat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return current(client -> client.exists(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return current(client -> client.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return current(client -> client.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return current(client -> client.remove(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return current(client -> client.lstat(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return current(client -> client.realpath(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return current(client -> client.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return current(client -> client.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return current(client -> client.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return current(client -> client.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return current(client -> client.statvfs(path));
    }

    /** The tenant is resolved now, when the publisher is asked for, not when it is subscribed. */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        String tenantId = resolver.currentTenant().orElse(null);
        if (tenantId == null) {
            return failing(new IllegalStateException("No current SFTP tenant: the TenantResolver "
                    + "returned none. Configure one that knows the current request's tenant, or name "
                    + "one with forTenant(...)."));
        }
        return forTenant(tenantId).download(remotePath, fromOffset);
    }

    /** A publisher that fails its subscriber: how a download refuses without throwing. */
    private static Flow.Publisher<ByteBuffer> failing(Throwable failure) {
        return subscriber -> {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.onError(failure);
        };
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        return current(client -> client.upload(remotePath, data, mode));
    }

    @Override
    public void close() {
        tenants.close();
        if (shared != null) {
            shared.close();
        }
    }

    /**
     * One tenant's client, looked up afresh on every call so it outlives an eviction. An operation
     * whose borrow lost a race with an eviction — failing before any request was sent — is retried
     * once against the tenant as now defined; a unit of work is not.
     */
    private final class TenantHandle implements ReactiveSftpClient {

        private final String tenantId;

        TenantHandle(String tenantId) {
            this.tenantId = tenantId;
        }

        private <T> CompletionStage<T> call(Function<ReactiveSftpClient, CompletionStage<T>> operation) {
            TenantRegistry.Tenant<ReactiveSftpClient> tenant;
            try {
                tenant = tenants.get(tenantId);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
            return operation.apply(tenant.client).handle((value, failure) -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(value);
                }
                Throwable cause = failure instanceof java.util.concurrent.CompletionException
                        && failure.getCause() != null ? failure.getCause() : failure;
                if (cause instanceof IllegalStateException && tenants.wasEvicted(tenantId, tenant)) {
                    return operation.apply(tenants.get(tenantId).client);
                }
                return CompletableFuture.<T>failedFuture(cause);
            }).thenCompose(Function.identity());
        }

        @Override
        public <R> CompletionStage<R> withSession(
                Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work) {
            try {
                return tenants.get(tenantId).client.withSession(work);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public CompletionStage<ReactiveSftpClientSession> openSession() {
            try {
                return tenants.get(tenantId).client.openSession();
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public SftpSessionContext sessionContext() {
            return context;
        }

        @Override
        public SftpCapabilities capabilities() {
            return provider.capabilities();
        }

        @Override
        public CompletionStage<List<RemoteFile>> list(String path) {
            return call(c -> c.list(path));
        }

        @Override
        public CompletionStage<RemoteFile> stat(String path) {
            return call(c -> c.stat(path));
        }

        @Override
        public CompletionStage<Boolean> exists(String path) {
            return call(c -> c.exists(path));
        }

        @Override
        public CompletionStage<Void> mkdir(String path) {
            return call(c -> c.mkdir(path));
        }

        @Override
        public CompletionStage<Void> rename(String from, String to) {
            return call(c -> c.rename(from, to));
        }

        @Override
        public CompletionStage<Void> remove(String path) {
            return call(c -> c.remove(path));
        }

        @Override
        public CompletionStage<RemoteFile> lstat(String path) {
            return call(c -> c.lstat(path));
        }

        @Override
        public CompletionStage<String> realpath(String path) {
            return call(c -> c.realpath(path));
        }

        @Override
        public CompletionStage<Void> symlink(String linkPath, String targetPath) {
            return call(c -> c.symlink(linkPath, targetPath));
        }

        @Override
        public CompletionStage<String> readlink(String linkPath) {
            return call(c -> c.readlink(linkPath));
        }

        @Override
        public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
            return call(c -> c.hardlink(linkPath, targetPath));
        }

        @Override
        public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
            return call(c -> c.setAttributes(path, update));
        }

        @Override
        public CompletionStage<FilesystemStats> statvfs(String path) {
            return call(c -> c.statvfs(path));
        }

        @Override
        public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
            try {
                return tenants.get(tenantId).client.download(remotePath, fromOffset);
            } catch (RuntimeException e) {
                return failing(e);
            }
        }

        /**
         * Not retried: {@code data} may have been subscribed by the failed attempt, and a publisher
         * is not guaranteed to be subscribable twice.
         */
        @Override
        public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
                SftpSession.WriteMode mode) {
            try {
                return tenants.get(tenantId).client.upload(remotePath, data, mode);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        /** Owns nothing: the multi-tenant client does. */
        @Override
        public void close() {
        }

        @Override
        public String toString() {
            return "ReactiveSftpClient[tenant=" + tenantId + "]";
        }
    }
}

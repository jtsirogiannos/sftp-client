package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.api.TenantResolver;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.core.DefaultSftpClient;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A blocking client per tenant, each a confined or own-connection view, all owned here and kept in a
 * {@link TenantRegistry}.
 *
 * <p>What callers hold is never the view itself but a {@link TenantHandle}, which looks the tenant
 * up on every call. That is what keeps {@code forTenant}'s result valid across an eviction.
 */
final class DefaultMultiTenantSftpClient implements MultiTenantSftpClient {

    private final SftpProvider provider;
    private final DefaultSftpClient shared;
    private final PoolSettings tenantPool;
    private final SftpSessionContext context;
    private final TenantResolver resolver;
    private final BiFunction<String, SftpClient, SftpClient> decorator;

    private final TenantRegistry<SftpClient> tenants;
    private final ConcurrentMap<String, TenantHandle> handles = new ConcurrentHashMap<>();

    DefaultMultiTenantSftpClient(SftpProvider provider, SftpConnectionConfig sharedConfig,
            PoolSettings sharedPool, PoolSettings tenantPool, SftpSessionContext context,
            TenantResolver resolver, SftpTenantProvider definitions,
            BiFunction<String, SftpClient, SftpClient> decorator) {
        this.provider = provider;
        this.shared = sharedConfig == null ? null : new DefaultSftpClient(provider,
                BoundedSftpSessionPool.blocking(provider, sharedConfig, sharedPool.maxSize(),
                        sharedPool.maxIdle(), sharedPool.acquireTimeout()), context);
        this.tenantPool = tenantPool;
        this.context = context;
        this.resolver = resolver;
        this.decorator = decorator;
        this.tenants = new TenantRegistry<>(definitions, sharedConfig, tenantPool, this::create);
    }

    // --- tenants ---

    @Override
    public SftpClient forTenant(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("a tenant identifier must not be blank");
        }
        return handles.computeIfAbsent(tenantId, TenantHandle::new);
    }

    @Override
    public void evictTenant(String tenantId) {
        tenants.evict(tenantId);
    }

    private SftpClient current() {
        String tenantId = resolver.currentTenant().orElseThrow(() -> new IllegalStateException(
                "No current SFTP tenant: the TenantResolver returned none. Run the work inside "
                        + "TenantContext.runAs(...), configure a TenantResolver that knows the "
                        + "current request's tenant, or name one with forTenant(...)."));
        return forTenant(tenantId);
    }

    private TenantRegistry.Tenant<SftpClient> create(String tenantId, SftpTenant definition) {
        DefaultSftpClient own = null;
        BoundedSftpSessionPool<SftpSession> ownPool = null;
        SftpClient base;
        if (definition.connection().isPresent()) {
            PoolSettings pool = definition.pool().orElse(tenantPool);
            ownPool = BoundedSftpSessionPool.blocking(provider, definition.connection().get(),
                    pool.maxSize(), pool.maxIdle(), pool.acquireTimeout());
            own = new DefaultSftpClient(provider, ownPool, context);
            base = own;
        } else if (shared != null) {
            base = shared;
        } else {
            throw new IllegalStateException("SFTP tenant '" + tenantId + "' has no connection of its "
                    + "own, and this client was built without a shared one to confine it on");
        }
        SftpClient view = definition.rootDirectory()
                .<SftpClient>map(root -> RootedSftpClient.forTenant(base, RootDirectory.of(root), tenantId))
                .orElseGet(() -> new TenantSftpClient(base, tenantId));
        return new TenantRegistry.Tenant<>(decorator.apply(tenantId, view), own, ownPool);
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
    public SftpClientSession openSession() {
        return current().openSession();
    }

    @Override
    public <R> R withSession(Function<SftpOperations, R> work) {
        return current().withSession(work);
    }

    @Override
    public void withSessionWithoutResult(Consumer<SftpOperations> work) {
        current().withSessionWithoutResult(work);
    }

    @Override
    public List<RemoteFile> list(String path) {
        return current().list(path);
    }

    @Override
    public void visit(String path, Predicate<RemoteFile> visitor) {
        current().visit(path, visitor);
    }

    @Override
    public RemoteFile stat(String path) {
        return current().stat(path);
    }

    @Override
    public RemoteFile lstat(String path) {
        return current().lstat(path);
    }

    @Override
    public boolean exists(String path) {
        return current().exists(path);
    }

    @Override
    public String realpath(String path) {
        return current().realpath(path);
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        current().symlink(linkPath, targetPath);
    }

    @Override
    public String readlink(String linkPath) {
        return current().readlink(linkPath);
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        current().hardlink(linkPath, targetPath);
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        current().setAttributes(path, update);
    }

    @Override
    public FilesystemStats statvfs(String path) {
        return current().statvfs(path);
    }

    @Override
    public void mkdir(String path) {
        current().mkdir(path);
    }

    @Override
    public void rename(String from, String to) {
        current().rename(from, to);
    }

    @Override
    public void remove(String path) {
        current().remove(path);
    }

    @Override
    public InputStream download(String remotePath) {
        return current().download(remotePath);
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        return current().download(remotePath, target);
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        return current().download(remotePath, target, fromOffset);
    }

    @Override
    public void upload(String remotePath, InputStream data) {
        current().upload(remotePath, data);
    }

    @Override
    public void upload(String remotePath, InputStream data,
            io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode mode) {
        current().upload(remotePath, data, mode);
    }

    @Override
    public void close() {
        tenants.close();
        if (shared != null) {
            shared.close();
        }
    }

    /**
     * One tenant's client, looked up afresh on every call so that it outlives an eviction.
     *
     * <p>A single operation that loses a race with {@link #evictTenant} — it found the tenant, then
     * the tenant's pool closed before it could borrow — is retried once against the tenant as now
     * defined. Nothing had happened yet: a closed pool refuses the borrow before any request is
     * sent. A unit of work is not retried, because by then something may have.
     */
    private final class TenantHandle implements SftpClient {

        private final String tenantId;

        TenantHandle(String tenantId) {
            this.tenantId = tenantId;
        }

        private <R> R call(Function<SftpClient, R> operation) {
            TenantRegistry.Tenant<SftpClient> tenant = tenants.get(tenantId);
            try {
                return operation.apply(tenant.client);
            } catch (IllegalStateException e) {
                if (tenants.wasEvicted(tenantId, tenant)) {
                    return operation.apply(tenants.get(tenantId).client);
                }
                throw e;
            }
        }

        private void run(Consumer<SftpClient> operation) {
            call(client -> {
                operation.accept(client);
                return null;
            });
        }

        @Override
        public SftpClientSession openSession() {
            return tenants.get(tenantId).client.openSession();
        }

        @Override
        public <R> R withSession(Function<SftpOperations, R> work) {
            return tenants.get(tenantId).client.withSession(work);
        }

        @Override
        public void withSessionWithoutResult(Consumer<SftpOperations> work) {
            tenants.get(tenantId).client.withSessionWithoutResult(work);
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
        public List<RemoteFile> list(String path) {
            return call(c -> c.list(path));
        }

        @Override
        public void visit(String path, Predicate<RemoteFile> visitor) {
            run(c -> c.visit(path, visitor));
        }

        @Override
        public RemoteFile stat(String path) {
            return call(c -> c.stat(path));
        }

        @Override
        public RemoteFile lstat(String path) {
            return call(c -> c.lstat(path));
        }

        @Override
        public boolean exists(String path) {
            return call(c -> c.exists(path));
        }

        @Override
        public String realpath(String path) {
            return call(c -> c.realpath(path));
        }

        @Override
        public void symlink(String linkPath, String targetPath) {
            run(c -> c.symlink(linkPath, targetPath));
        }

        @Override
        public String readlink(String linkPath) {
            return call(c -> c.readlink(linkPath));
        }

        @Override
        public void hardlink(String linkPath, String targetPath) {
            run(c -> c.hardlink(linkPath, targetPath));
        }

        @Override
        public void setAttributes(String path, AttributeUpdate update) {
            run(c -> c.setAttributes(path, update));
        }

        @Override
        public FilesystemStats statvfs(String path) {
            return call(c -> c.statvfs(path));
        }

        @Override
        public void mkdir(String path) {
            run(c -> c.mkdir(path));
        }

        @Override
        public void rename(String from, String to) {
            run(c -> c.rename(from, to));
        }

        @Override
        public void remove(String path) {
            run(c -> c.remove(path));
        }

        @Override
        public InputStream download(String remotePath) {
            return call(c -> c.download(remotePath));
        }

        @Override
        public long download(String remotePath, OutputStream target) {
            return call(c -> c.download(remotePath, target));
        }

        @Override
        public long download(String remotePath, OutputStream target, long fromOffset) {
            return call(c -> c.download(remotePath, target, fromOffset));
        }

        @Override
        public void upload(String remotePath, InputStream data) {
            run(c -> c.upload(remotePath, data));
        }

        @Override
        public void upload(String remotePath, InputStream data,
                io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode mode) {
            run(c -> c.upload(remotePath, data, mode));
        }

        /** Owns nothing: the multi-tenant client does. */
        @Override
        public void close() {
        }

        @Override
        public String toString() {
            return "SftpClient[tenant=" + tenantId + "]";
        }
    }
}

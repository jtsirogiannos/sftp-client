package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Bootstrap entry point — the {@code DriverManager} of this library.
 *
 * <p>Discovers providers via {@link ServiceLoader}, so adding {@code sftp-provider-mina} to the
 * classpath is all it takes to make {@code "mina"} resolvable. Usable with no framework at all:
 *
 * <pre>{@code
 * var config = SftpConnectionConfig.builder()
 *         .host("sftp.example.com").username("svc")
 *         .password("...".toCharArray())
 *         .build();
 * try (var client = SftpClients.blocking("mina", config)) {
 *     client.list("/inbound").forEach(System.out::println);
 * }
 * }</pre>
 */
public final class SftpClients {

    private static final Logger log = LoggerFactory.getLogger(SftpClients.class);

    private SftpClients() {
    }

    public static List<String> availableProviders() {
        return ServiceLoader.load(SftpProvider.class).stream()
            .map(p -> p.get().name())
            .sorted()
            .collect(Collectors.toList());
    }

    public static SftpProvider provider(String name) {
        return ServiceLoader.load(SftpProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> p.name().equals(name))
            .findFirst()
            // This message is the most-read line of your docs. Say what's missing and what to add.
            .orElseThrow(() -> new SftpException(
                "No SFTP provider named '" + name + "' on the classpath. Available: "
                    + availableProviders() + ". Add sftp-provider-" + name
                    + " as a dependency.",
                null));
    }

    public static SftpClient blocking(String providerName, SftpConnectionConfig config) {
        SftpProvider provider = provider(providerName);
        log.debug("Creating a blocking client for {} using provider '{}' (available: {})", config,
                providerName, availableProviders());
        return new DefaultSftpClient(provider, BoundedSftpSessionPool.blocking(provider, config));
    }

    /**
     * A reactive client, backed by whichever session contract the provider can actually honour.
     *
     * <p>This {@code instanceof} is the only one in the reactive path, and it is here rather than in
     * the client because it is a question about the <em>provider</em>, asked once. A provider that
     * can open {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession}s gets a client and a
     * pool typed to them, and every operation afterwards is statically async — no capability flag
     * consulted, no session type re-tested per call.
     */
    public static ReactiveSftpClient reactive(String providerName, SftpConnectionConfig config,
                                              Executor worker) {
        SftpProvider provider = provider(providerName);
        if (provider instanceof AsyncSftpProvider asyncProvider) {
            log.debug("Creating an async reactive client for {} using provider '{}'", config,
                providerName);
            return new AsyncReactiveSftpClient(asyncProvider,
                BoundedSftpSessionPool.async(asyncProvider, config), worker);
        }
        log.debug("Creating a reactive client for {} using provider '{}'", config, providerName);
        return new DefaultReactiveSftpClient(provider,
            BoundedSftpSessionPool.blocking(provider, config), worker);
    }
}

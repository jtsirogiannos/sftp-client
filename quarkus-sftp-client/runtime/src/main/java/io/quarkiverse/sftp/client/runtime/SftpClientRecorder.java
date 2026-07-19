package io.quarkiverse.sftp.client.runtime;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;

import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Builds the per-endpoint clients at runtime init.
 *
 * <p>A recorder exists because the build step runs during augmentation, where the real objects
 * cannot be constructed: what it produces instead is a recipe replayed when the application starts.
 * Everything here is therefore deliberately dull — read config, call the same
 * {@code SftpClients} bootstrap a plain-Java user would.
 *
 * <p>Note what is <em>not</em> here: no transfer logic, no threading, no reactive plumbing. Same
 * test as the Spring starter — if this file grows past config translation, the core is not actually
 * framework-neutral.
 */
@Recorder
public class SftpClientRecorder {

    private final RuntimeValue<SftpRuntimeConfig> config;

    /**
     * Run-time config arrives through the constructor, not as a build-step parameter: since Quarkus
     * 3.x a build step cannot consume run-time config at all, because augmentation happens before
     * that config exists. The {@link RuntimeValue} is a placeholder during the build and holds the
     * real mapping by the time any method below runs.
     */
    public SftpClientRecorder(RuntimeValue<SftpRuntimeConfig> config) {
        this.config = config;
    }

    public Function<SyntheticCreationalContext<SftpClient>, SftpClient> blockingClient(
            String name, String provider) {
        return context -> SftpClients.blocking(provider, connectionConfig(config.getValue(), name));
    }

    /**
     * The Mutiny client wraps a neutral reactive one. The wrapper owns the delegate and closes it,
     * so the endpoint gets exactly one pool rather than one per facade.
     */
    public Function<SyntheticCreationalContext<MutinySftpClient>, MutinySftpClient> mutinyClient(
            String name, String provider, Executor worker) {
        return context -> {
            ReactiveSftpClient delegate =
                    SftpClients.reactive(provider, connectionConfig(config.getValue(), name), worker);
            return new MutinySftpClient(delegate);
        };
    }

    private static SftpConnectionConfig connectionConfig(SftpRuntimeConfig config, String name) {
        SftpRuntimeConfig.EndpointRuntimeConfig endpoint = config.endpoints().get(name);
        if (endpoint == null) {
            // The build fixed the endpoint names, so a mismatch means run-time config dropped one.
            throw new IllegalStateException("no runtime configuration for sftp endpoint '" + name
                    + "'; it was present at build time. Check that sftp.endpoints." + name
                    + ".* is set in this environment.");
        }

        SftpConnectionConfig.Builder builder = SftpConnectionConfig.builder()
                .host(endpoint.host())
                .port(endpoint.port())
                .username(endpoint.username())
                .hostKeyStrategy(endpoint.hostKeyStrategy())
                .connectTimeout(endpoint.connectTimeout())
                .requestTimeout(endpoint.requestTimeout());

        endpoint.password().filter(p -> !p.isEmpty()).map(String::toCharArray).ifPresent(builder::password);
        endpoint.privateKey().ifPresent(builder::privateKey);
        endpoint.passphrase().filter(p -> !p.isEmpty()).map(String::toCharArray).ifPresent(builder::passphrase);
        // Only override when set: passing null would wipe the builder's ~/.ssh/known_hosts default,
        // and STRICT (the default strategy) rejects a null known-hosts file.
        endpoint.knownHosts().ifPresent(builder::knownHosts);

        try {
            return builder.build();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "sftp.endpoints." + name + " is misconfigured: " + e.getMessage(), e);
        }
    }
}

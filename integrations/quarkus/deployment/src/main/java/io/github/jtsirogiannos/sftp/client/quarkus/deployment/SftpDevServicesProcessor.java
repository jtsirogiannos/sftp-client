package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem.RunningDevService;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Starts a throwaway SFTP server, in dev and test mode, for any endpoint declared without a host —
 * the same courtesy the datasource extension does with a database container. The endpoint keeps its
 * real provider (mina or jsch); Dev Services only supplies the address and credentials, so the code
 * path a developer exercises is the one that runs in production, against a real server rather than a
 * fake.
 *
 * <p>Activation is deliberately narrow: an endpoint has to be <em>declared</em> — at least one
 * {@code sftp.endpoints.<name>.*} property present — and have no {@code host}. A fully configured
 * endpoint is left alone, and a build with no endpoints starts nothing.
 *
 * <p>Two wrinkles the ROADMAP called out, and how they are handled:
 * <ul>
 *   <li><strong>The mandatory host.</strong> {@code SftpRuntimeConfig.host()} has no default, so a
 *       hostless endpoint would fail to start. The container's host and port are contributed as a
 *       {@link DevServicesResultBuildItem} config source, which is exactly the config that satisfies
 *       it — the same way the datasource dev service supplies the JDBC URL.</li>
 *   <li><strong>Credentials.</strong> The container is created with the endpoint's configured
 *       username and password when present (so an explicit {@code username} still works), and with
 *       {@code sftp}/{@code sftp} otherwise. Whatever it used is contributed back, so the two always
 *       agree regardless of config-source ordering.</li>
 * </ul>
 *
 * <p>The host-key strategy is forced to {@code INSECURE_ACCEPT_ANY}: the container generates fresh
 * keys on every start, so there is nothing to pin — acceptable because this only ever runs in dev and
 * test. In production the step does not run at all ({@code onlyIfNot = IsNormal}).
 */
class SftpDevServicesProcessor {

    private static final Logger log = Logger.getLogger(SftpDevServicesProcessor.class);

    private static final String FEATURE = "sftp-client";
    private static final String ENDPOINT_PREFIX = "sftp.endpoints.";
    private static final int SFTP_PORT = 22;
    /** The single writable directory atmoz/sftp creates inside the chroot; matches the TCK fixture. */
    private static final String UPLOAD_DIR = "upload";
    private static final String DEFAULT_USER = "sftp";
    private static final String DEFAULT_PASSWORD = "sftp";

    /**
     * Containers kept alive across dev-mode live reloads, keyed by endpoint name, with the config
     * signature that produced each so an unchanged endpoint is reused rather than restarted. Static
     * because the deployment classloader persists across reloads; that persistence is the point.
     */
    private static final Map<String, RunningDevService> running = new ConcurrentHashMap<>();
    private static final Map<String, String> signatures = new ConcurrentHashMap<>();

    @BuildStep(onlyIfNot = IsNormal.class)
    List<DevServicesResultBuildItem> startSftpDevServices(SftpDevServicesBuildTimeConfig devServices) {
        if (!devServices.enabled()) {
            stopAll(); // was enabled earlier this dev session, now turned off
            return List.of();
        }

        Config config = ConfigProvider.getConfig();
        TreeSet<String> hostless = hostlessEndpoints(config);
        // A previously-served endpoint that gained a host, or was removed, must give its container back.
        retainOnly(hostless);

        List<DevServicesResultBuildItem> results = new ArrayList<>();
        if (hostless.isEmpty()) {
            return results;
        }
        if (!dockerAvailable()) {
            log.warnf("SFTP Dev Services: Docker is not available, so endpoints %s have no host and"
                    + " will fail to start. Start Docker, give them a host, or disable Dev Services"
                    + " with quarkus.sftp-client.devservices.enabled=false.", hostless);
            return results;
        }

        for (String name : hostless) {
            String user = config.getOptionalValue(ENDPOINT_PREFIX + name + ".username", String.class)
                    .filter(s -> !s.isBlank()).orElse(DEFAULT_USER);
            String password = config.getOptionalValue(ENDPOINT_PREFIX + name + ".password", String.class)
                    .filter(s -> !s.isBlank()).orElse(DEFAULT_PASSWORD);
            String signature = devServices.imageName() + '|' + user + '|' + password;

            RunningDevService existing = running.get(name);
            if (existing != null && signature.equals(signatures.get(name))) {
                results.add(existing.toBuildItem()); // unchanged: reuse the container from the last reload
                continue;
            }
            closeQuietly(existing);
            RunningDevService service = start(name, devServices.imageName(), user, password);
            running.put(name, service);
            signatures.put(name, signature);
            results.add(service.toBuildItem());
        }
        return results;
    }

    /** Declared endpoints — any {@code sftp.endpoints.<name>.*} — that have no host set. */
    private static TreeSet<String> hostlessEndpoints(Config config) {
        TreeSet<String> names = new TreeSet<>();
        for (String property : config.getPropertyNames()) {
            if (!property.startsWith(ENDPOINT_PREFIX)) {
                continue;
            }
            int dot = property.indexOf('.', ENDPOINT_PREFIX.length());
            if (dot > ENDPOINT_PREFIX.length()) {
                names.add(property.substring(ENDPOINT_PREFIX.length(), dot));
            }
        }
        names.removeIf(name -> config
                .getOptionalValue(ENDPOINT_PREFIX + name + ".host", String.class)
                .filter(host -> !host.isBlank())
                .isPresent());
        return names;
    }

    private static RunningDevService start(String name, String image, String user, String password) {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(image))
                .withExposedPorts(SFTP_PORT)
                // atmoz/sftp command is user:pass:[e]:[uid]:[gid]:[dirs]; the trailing dir is the one
                // writable path inside the chroot.
                .withCommand(user + ':' + password + ":::" + UPLOAD_DIR)
                .waitingFor(Wait.forListeningPort());
        container.start();

        String prefix = ENDPOINT_PREFIX + name + '.';
        Map<String, String> config = new LinkedHashMap<>();
        config.put(prefix + "host", container.getHost());
        config.put(prefix + "port", String.valueOf(container.getMappedPort(SFTP_PORT)));
        config.put(prefix + "username", user);
        config.put(prefix + "password", password);
        // Fresh host keys every start — nothing to pin. Dev and test only.
        config.put(prefix + "host-key-strategy", "INSECURE_ACCEPT_ANY");

        log.infof("SFTP Dev Services started %s for endpoint '%s' at %s:%d (user '%s', dir /%s)",
                image, name, container.getHost(), container.getMappedPort(SFTP_PORT), user, UPLOAD_DIR);

        Closeable closeable = container::stop;
        return new RunningDevService(FEATURE, container.getContainerId(), closeable, config);
    }

    private static void retainOnly(TreeSet<String> keep) {
        running.keySet().removeIf(name -> {
            if (keep.contains(name)) {
                return false;
            }
            closeQuietly(running.get(name));
            signatures.remove(name);
            return true;
        });
    }

    private static void stopAll() {
        running.values().forEach(SftpDevServicesProcessor::closeQuietly);
        running.clear();
        signatures.clear();
    }

    private static void closeQuietly(RunningDevService service) {
        if (service == null) {
            return;
        }
        try {
            service.close();
        } catch (IOException e) {
            log.warnf(e, "SFTP Dev Services: failed to stop a container cleanly");
        }
    }

    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (RuntimeException e) {
            return false;
        }
    }
}

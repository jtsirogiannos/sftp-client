package io.github.jtsirogiannos.sftp.client.provider.mina;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.config.hosts.HostConfigEntryResolver;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Pins the two things the provider must <em>not</em> inherit from the developer's SSH environment:
 * the local {@code ~/.ssh} identities and the {@code ~/.ssh/config} file. A default MINA client
 * honours both, which is right for an interactive CLI and wrong for a library — a config addressed
 * to one host must connect to that host and offer only the configured credential.
 *
 * <p>The negative ("did not read a file") cannot be proven by opening a socket, and steering it
 * through {@code user.home} is unreliable because MINA caches the default config path in a lazy
 * holder the first time any connect resolves a host. So this asserts the hardening directly, before
 * a client ever connects. The end-to-end proof that it matters is the Quarkus integration test,
 * which failed on a machine whose {@code ~/.ssh/config} carried a {@code Match} section until this
 * guard went in.
 */
class MinaHermeticClientTest {

    @Test
    void hardenStripsTheLocalSshEnvironment() throws IOException {
        try (SshClient client = MinaSftpProvider.harden(SshClient.setUpDefaultClient())) {
            assertSame(HostConfigEntryResolver.EMPTY, client.getHostConfigEntryResolver(),
                    "~/.ssh/config must not be consulted");
            assertSame(KeyIdentityProvider.EMPTY_KEYS_PROVIDER, client.getKeyIdentityProvider(),
                    "~/.ssh identities must not be offered during authentication");
        }
    }
}

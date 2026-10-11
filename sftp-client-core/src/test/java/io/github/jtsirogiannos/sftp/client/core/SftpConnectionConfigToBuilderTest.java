package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deriving one config from another — the shape of a per-tenant credential over a shared host — keeps
 * every field and shares no secret array. The API module has no test sources of its own, so the
 * test lives with the first consumer of the method.
 */
public class SftpConnectionConfigToBuilderTest {

    private final SftpConnectionConfig base = SftpConnectionConfig.builder()
            .host("sftp.example.com")
            .port(2222)
            .username("shared")
            .password("base-secret".toCharArray())
            .privateKey(Path.of("/keys/id_ed25519"))
            .passphrase("unlock".toCharArray())
            .hostKeyStrategy(HostKeyStrategy.ACCEPT_NEW)
            .knownHosts(Path.of("/etc/ssh/known_hosts"))
            .connectTimeout(Duration.ofSeconds(3))
            .requestTimeout(Duration.ofSeconds(7))
            .build();

    @Test
    public void carriesEveryFieldAcross() {
        SftpConnectionConfig copy = base.toBuilder().build();

        assertThat(copy.host()).isEqualTo("sftp.example.com");
        assertThat(copy.port()).isEqualTo(2222);
        assertThat(copy.username()).isEqualTo("shared");
        assertThat(copy.password()).containsExactly("base-secret".toCharArray());
        assertThat(copy.privateKey()).isEqualTo(Path.of("/keys/id_ed25519"));
        assertThat(copy.passphrase()).containsExactly("unlock".toCharArray());
        assertThat(copy.hostKeyStrategy()).isEqualTo(HostKeyStrategy.ACCEPT_NEW);
        assertThat(copy.knownHosts()).isEqualTo(Path.of("/etc/ssh/known_hosts"));
        assertThat(copy.connectTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(copy.requestTimeout()).isEqualTo(Duration.ofSeconds(7));
    }

    @Test
    public void derivesAVariantWithoutTouchingTheOriginal() {
        SftpConnectionConfig tenant = base.toBuilder()
                .username("acme")
                .password("acme-secret".toCharArray())
                .build();

        assertThat(tenant.username()).isEqualTo("acme");
        assertThat(tenant.password()).containsExactly("acme-secret".toCharArray());
        assertThat(base.username()).isEqualTo("shared");
        assertThat(base.password()).containsExactly("base-secret".toCharArray());
    }

    @Test
    public void sharesNoSecretArrayWithTheOriginal() {
        SftpConnectionConfig.Builder builder = base.toBuilder();
        SftpConnectionConfig copy = builder.build();

        // Wipe everything reachable from the copy's side; the original must be unaffected.
        SftpConnectionConfig.wipe(copy.password());
        builder.password(null);

        assertThat(base.password()).containsExactly("base-secret".toCharArray());
        assertThat(base.passphrase()).containsExactly("unlock".toCharArray());
        assertThat(copy.password()).containsExactly("base-secret".toCharArray());
    }
}

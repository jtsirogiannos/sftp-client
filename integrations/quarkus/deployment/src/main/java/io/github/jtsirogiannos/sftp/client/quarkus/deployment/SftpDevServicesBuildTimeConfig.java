package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Build-time settings for SFTP Dev Services.
 *
 * <p><strong>Why this lives under {@code quarkus.sftp-client.*} and not {@code sftp.*}.</strong> The
 * endpoint config uses {@code @ConfigMapping(prefix = "sftp")}, which claims the whole {@code sftp.*}
 * tree; a {@code sftp.devservices.*} property would then fail to map to any member of that root and
 * break the application at startup. Build-time config roots must sit under {@code quarkus.} in any
 * case, so this is both required and safe — the same split Quarkus uses between
 * {@code quarkus.datasource.devservices.*} and a datasource's runtime URL.
 */
@ConfigMapping(prefix = "quarkus.sftp-client.devservices")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface SftpDevServicesBuildTimeConfig {

    /**
     * Whether Dev Services may start containers. On by default; a container is only started for an
     * endpoint that is declared but has no {@code host}, so turning this off is how you opt an
     * endpoint out of the automatic server without also giving it a real one.
     */
    @WithDefault("true")
    boolean enabled();

    /**
     * The SFTP server image. {@code atmoz/sftp} is OpenSSH with a chrooted user — the shape the TCK
     * already uses — so a provider is exercised against a real server rather than a fake in dev.
     */
    @WithDefault("atmoz/sftp:alpine")
    String imageName();
}

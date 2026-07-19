package io.github.jtsirogiannos.sftp.client.provider.mina;

import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.tck.OpenSshConformanceTestBase;

/**
 * The MINA provider against a real OpenSSH server rather than MINA's own.
 *
 * <p>{@link MinaConformanceTest} has MINA on both ends, which is a closed loop: it cannot show where
 * MINA's client and server share an assumption the rest of the world does not. OpenSSH speaks SFTP
 * v3 only, so this run also exercises the case where the provider's advertised v6 capabilities meet
 * a server that will not grant them.
 */
class MinaOpenSshConformanceTest extends OpenSshConformanceTestBase {

    @Override
    protected SftpProvider createProvider() {
        return new MinaSftpProvider();
    }
}

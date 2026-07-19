package io.github.jtsirogiannos.sftp.client.provider.jsch;

import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.tck.OpenSshConformanceTestBase;

/**
 * The JSch provider against a real OpenSSH server.
 *
 * <p>Arguably the most representative combination in the whole suite: a v3 client talking to the
 * server most SFTP endpoints actually run. {@link JschConformanceTest} pairs it with MINA's server,
 * which is a useful Docker-free default but not what production looks like.
 */
class JschOpenSshConformanceTest extends OpenSshConformanceTestBase {

    @Override
    protected SftpProvider createProvider() {
        return new JschSftpProvider();
    }
}

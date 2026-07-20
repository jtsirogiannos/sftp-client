package io.github.jtsirogiannos.sftp.client.provider.jsch;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * JSch backend, using the maintained {@code com.github.mwiede} fork.
 *
 * <p>The reason this provider exists is not that anyone prefers JSch — it is that a second backend
 * is the only way to find out whether the SPI is a real abstraction or a description of MINA. It
 * pays for itself immediately: see {@code atomicCreateNew} below.
 */
public final class JschSftpProvider implements SftpProvider {

    private static final Logger log = LoggerFactory.getLogger(JschSftpProvider.class);

    /**
     * SFTP v3, stop-and-wait, and two honest "no"s.
     *
     * <p>{@code posixRename = false} because v3 rename fails when the target exists rather than
     * replacing it. {@code atomicCreateNew = false} because JSch's {@code put} exposes only
     * OVERWRITE/RESUME/APPEND — the protocol has {@code SSH_FXF_EXCL}, but this client's API gives
     * no way to send it. Both are reported rather than emulated: a stat-then-create would pass the
     * conformance suite and lose data under concurrency.
     */
    static final SftpCapabilities CAPABILITIES = SftpCapabilities.builder()
            .maxSftpVersion(3)
            .pipelinedStreaming(false)
            .posixRename(false)
            .atomicCreateNew(false)
            .symlinks(true)
            .hardLinks(false)
            .statvfs(false)
            .build();

    @Override
    public String name() {
        return "jsch";
    }

    @Override
    public SftpCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public SftpSession connect(SftpConnectionConfig config) {
        Session session = null;
        try {
            JSch jsch = new JSch();
            applyHostKeyStrategy(jsch, config);
            applyKeyIdentity(jsch, config);

            session = jsch.getSession(config.username(), config.host(), config.port());
            // Per-session, never JSch.setConfig(...): that static setter mutates global state and
            // would let one endpoint's host-key policy leak into every other connection in the JVM.
            session.setConfig("StrictHostKeyChecking", strictness(config));
            applyPassword(session, config);

            int timeoutMillis = Math.toIntExact(config.connectTimeout().toMillis());
            session.connect(timeoutMillis);

            ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(timeoutMillis);
            log.debug("Connected to {}@{}:{} using {} auth; JSch speaks SFTP v3 only",
                    config.username(), config.host(), config.port(),
                    config.privateKey() != null ? "public-key" : "password");
            return new JschSftpSession(session, channel);
        } catch (JSchException | RuntimeException e) {
            log.debug("Connection to {}@{}:{} failed ({}); host-key strategy was {}",
                    config.username(), config.host(), config.port(), e.toString(),
                    config.hostKeyStrategy());
            if (session != null) {
                session.disconnect();
            }
            throw new SftpException("could not connect to " + config.username() + "@"
                    + config.host() + ":" + config.port(), e);
        }
    }

    /**
     * JSch's {@code StrictHostKeyChecking} understands only {@code yes}/{@code no}/{@code ask} — this
     * fork has no {@code accept-new} — so TOFU is implemented against the repository instead of by
     * setting a config string. That is deliberate: emulating {@code ACCEPT_NEW} with {@code ask} plus
     * a {@link UserInfo} that always answers yes would also accept a <em>changed</em> host key, which
     * is precisely the attack the mode is supposed to catch. Distinguishing the two by prompt text
     * would be worse still.
     */
    private static void applyHostKeyStrategy(JSch jsch, SftpConnectionConfig config)
            throws JSchException {
        switch (config.hostKeyStrategy()) {
            // Unknown or changed key: refuse. No prompt, no UserInfo, no way to say yes.
            case STRICT -> jsch.setKnownHosts(config.knownHosts().toString());
            case ACCEPT_NEW -> {
                jsch.setKnownHosts(config.knownHosts().toString());
                // Strictness stays "yes"; the repository below is what turns an unknown host into
                // an accepted-and-recorded one, leaving a changed key still fatal.
                jsch.setHostKeyRepository(new TrustOnFirstUse(jsch.getHostKeyRepository()));
            }
            case INSECURE_ACCEPT_ANY -> {
                // No known-hosts file at all; strictness "no" below accepts whatever answers.
            }
        }
    }

    private static String strictness(SftpConnectionConfig config) {
        return config.hostKeyStrategy() == io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy.INSECURE_ACCEPT_ANY
                ? "no"
                : "yes";
    }

    private static void applyKeyIdentity(JSch jsch, SftpConnectionConfig config) throws JSchException {
        Path privateKey = config.privateKey();
        if (privateKey == null) {
            return;
        }
        char[] passphrase = config.passphrase();
        try {
            if (passphrase == null) {
                jsch.addIdentity(privateKey.toString());
            } else {
                // The byte[] overload, encoded without an intermediate String — a String copy is
                // immortal until GC and would defeat the wipes below.
                byte[] encoded = SftpConnectionConfig.encodeSecret(passphrase);
                try {
                    jsch.addIdentity(privateKey.toString(), encoded);
                } finally {
                    SftpConnectionConfig.wipe(encoded);
                }
            }
        } finally {
            SftpConnectionConfig.wipe(passphrase);
        }
    }

    private static void applyPassword(Session session, SftpConnectionConfig config) {
        char[] password = config.password();
        if (password == null) {
            return;
        }
        try {
            // setPassword(byte[]) via a String-free encoding: the previous version built the byte
            // array through new String(password), creating exactly the immortal heap copy this
            // comment claimed to avoid. JSch copies the array, so ours is safe to wipe.
            byte[] encoded = SftpConnectionConfig.encodeSecret(password);
            try {
                session.setPassword(encoded);
            } finally {
                SftpConnectionConfig.wipe(encoded);
            }
        } finally {
            SftpConnectionConfig.wipe(password);
        }
    }

    /**
     * Trust-on-first-use, implemented where the decision actually is.
     *
     * <p>Unknown host — record the key and accept. Known and matching — accept. <strong>Changed —
     * reject</strong>, which is the whole point of the mode and the case a naive "just say yes"
     * implementation gets wrong.
     */
    private record TrustOnFirstUse(HostKeyRepository delegate) implements HostKeyRepository {

        @Override
        public int check(String host, byte[] key) {
            int result = delegate.check(host, key);
            if (result != NOT_INCLUDED) {
                return result; // OK stays OK; CHANGED stays CHANGED and fails the handshake
            }
            try {
                delegate.add(new HostKey(host, key), null);
                return OK;
            } catch (JSchException e) {
                // Could not record it (unwritable known_hosts, unknown key type): fail closed rather
                // than accept a key we cannot pin for next time.
                return NOT_INCLUDED;
            }
        }

        @Override
        public void add(HostKey hostkey, UserInfo ui) {
            delegate.add(hostkey, ui);
        }

        @Override
        public void remove(String host, String type) {
            delegate.remove(host, type);
        }

        @Override
        public void remove(String host, String type, byte[] key) {
            delegate.remove(host, type, key);
        }

        @Override
        public String getKnownHostsRepositoryID() {
            return delegate.getKnownHostsRepositoryID();
        }

        @Override
        public HostKey[] getHostKey() {
            return delegate.getHostKey();
        }

        @Override
        public HostKey[] getHostKey(String host, String type) {
            return delegate.getHostKey(host, type);
        }
    }
}

package io.github.jtsirogiannos.sftp.client.provider.inmemory;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

/**
 * An SFTP backend with no server behind it.
 *
 * <p>Add it at test scope and ask for it by name; everything above — the pool, both facades, the
 * Spring and Quarkus integrations — behaves exactly as it does against a real server:
 *
 * <pre>{@code
 * try (var client = SftpClients.blocking("inmemory", config)) {
 *     client.upload("/upload/report.csv", data);
 * }
 * }</pre>
 *
 * <p>The {@link SftpConnectionConfig} is ignored apart from being required: there is no host to
 * reach and no credential to check. That is deliberate — a fake that rejected the wrong password
 * would be a worse fake, because the thing under test is your code, not the handshake.
 *
 * <p><strong>State lives on the provider, not the session.</strong> One instance is one filesystem,
 * shared by every session it hands out, so a pooled client sees its own writes. A fresh provider is
 * a fresh filesystem, which is usually what a test wants between cases.
 *
 * <h2>Pretending to be an older server</h2>
 *
 * By default it reports SFTP v6 with everything supported. {@link #withCapabilities} builds one that
 * claims less — and then behaves accordingly, so a v3 fake really does refuse to replace an existing
 * file on rename. Testing that your code copes with a v3 server is otherwise awkward: it means
 * finding a v3 server.
 */
public final class InMemorySftpProvider implements AsyncSftpProvider {

    public static final String NAME = "inmemory";

    /** Everything the SPI can express. A capable modern server, with no surprises. */
    private static final SftpCapabilities FULL = SftpCapabilities.builder()
            .maxSftpVersion(6)
            .pipelinedStreaming(false) // there is no wire to pipeline
            .asyncStreaming(true) // ...and for the same reason, nothing ever parks a thread
            .posixRename(true)
            .atomicCreateNew(true)
            .symlinks(true)
            .hardLinks(true)
            .statvfs(true)
            .build();

    /** What OpenSSH offers: v3, no atomic replacing rename. */
    public static final SftpCapabilities VERSION_3 = SftpCapabilities.builder()
            .maxSftpVersion(3)
            .pipelinedStreaming(false)
            .asyncStreaming(true)
            .posixRename(false)
            .atomicCreateNew(true) // SSH_FXF_EXCL exists in v3
            .symlinks(true)
            .hardLinks(false)
            .statvfs(false)
            .build();

    private final InMemoryFileSystem filesystem = new InMemoryFileSystem();
    private final SftpCapabilities capabilities;

    public InMemorySftpProvider() {
        this(FULL);
    }

    private InMemorySftpProvider(SftpCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    /** A provider that advertises — and enforces — a reduced capability set. */
    public static InMemorySftpProvider withCapabilities(SftpCapabilities capabilities) {
        return new InMemorySftpProvider(capabilities);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SftpCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public SftpSession connect(SftpConnectionConfig config) {
        return new InMemorySftpSession(filesystem, capabilities);
    }

    /**
     * The same session, typed to the contract that says it never parks a thread.
     *
     * <p>Both methods hand back the identical object because for this backend both claims are true
     * at once. A networked provider would not have that luxury — which is exactly why the two
     * contracts are siblings rather than one extending the other.
     */
    @Override
    public AsyncSftpSession connectAsync(SftpConnectionConfig config) {
        return new InMemorySftpSession(filesystem, capabilities);
    }
}

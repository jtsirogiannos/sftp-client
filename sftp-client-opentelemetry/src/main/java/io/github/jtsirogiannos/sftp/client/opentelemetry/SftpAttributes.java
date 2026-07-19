package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;

/**
 * The attribute names this module sets, each labelled with what it actually is.
 *
 * <p><strong>There is no OpenTelemetry semantic convention for SFTP, FTP, SCP, or file transfer as
 * a class of operation.</strong> Searching the semantic-conventions repository for {@code sftp}
 * returns nothing — no model group, no document, not even a discussion issue. So part of what this
 * module emits is necessarily invented, and the honest thing is to mark which part.
 *
 * <p>Names are declared here as literals rather than imported from the semconv artifacts. The stable
 * artifact would only save a few strings, and the incubating one — which is where {@code file.*}
 * lives — tells library authors in its own README not to depend on it and to copy the names instead.
 * Declaring them here also makes the stability of each one visible at the point of use, which
 * importing constants would hide.
 */
final class SftpAttributes {

    // --- Stable. Safe to rely on; these will not be renamed. ---

    /** Stable. The SFTP server's hostname or address. */
    static final AttributeKey<String> SERVER_ADDRESS = AttributeKey.stringKey("server.address");
    /** Stable. */
    static final AttributeKey<Long> SERVER_PORT = AttributeKey.longKey("server.port");
    /** Stable. Lowercase application-layer protocol; {@code sftp} is a legitimate value. */
    static final AttributeKey<String> NETWORK_PROTOCOL_NAME =
        AttributeKey.stringKey("network.protocol.name");
    /**
     * Stable. The negotiated SFTP protocol version — 3 against OpenSSH, 6 where the server agrees.
     * Set only when a session has actually negotiated, never guessed from the provider's best case.
     */
    static final AttributeKey<String> NETWORK_PROTOCOL_VERSION =
        AttributeKey.stringKey("network.protocol.version");
    /** Stable. Canonical class name of whatever escaped, per the recording-errors guidance. */
    static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");

    // --- Experimental. Real registry entries, but subject to change, and no span convention uses
    //     them today. They also carry a local-filesystem flavour (inode, uid, octal mode), so
    //     applying them to a remote path is a defensible reuse rather than an endorsed one. Reusing
    //     the existing names still beats inventing synonyms nobody can query. ---

    /** Experimental ({@code file.*} registry). The remote path the operation acted on. */
    static final AttributeKey<String> FILE_PATH = AttributeKey.stringKey("file.path");
    /** Experimental ({@code file.*} registry). Bytes actually transferred, not bytes requested. */
    static final AttributeKey<Long> FILE_SIZE = AttributeKey.longKey("file.size");

    private SftpAttributes() {
    }
}

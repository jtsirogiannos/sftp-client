package io.github.jtsirogiannos.sftp.client.spi;

/**
 * What a provider can actually do. Every flag here exists because two backends genuinely differ —
 * this record is the alternative to callers writing {@code if (provider instanceof MinaProvider)}.
 *
 * <p>The naming rule, from the README: flags describe observable behaviour, never aspiration.
 * {@link #pipelinedStreaming()} is deliberately not called "async" — MINA's public client API has no
 * zero-thread SFTP path, it just keeps multiple requests in flight so the pulling worker rarely
 * stalls. A caller reading this record is never told a transfer is more asynchronous than it is.
 *
 * @param maxSftpVersion     highest SFTP protocol version negotiated; 3 for JSch, 6 for MINA
 * @param pipelinedStreaming true if reads/writes overlap requests rather than stop-and-wait. Affects
 *                           throughput and the chunk size the core picks — never the API shape
 * @param asyncStreaming     true if the session implements {@link AsyncSftpSession} and can transfer
 *                           without parking a thread. Independent of {@code pipelinedStreaming}, not
 *                           stronger than it: that flag says requests overlap, this one says no worker
 *                           blocks on the reply, and a backend can have either without the other — an
 *                           event-loop provider that issues one request per callback is async and
 *                           stop-and-wait at the same time. Only a provider that owns request-id
 *                           correlation, or one with no wire at all, can set it
 * @param posixRename        true if rename atomically replaces an existing target (v5+ {@code posix-rename}).
 *                           On v3 a rename onto an existing path fails, and the core will not silently emulate it
 * @param atomicCreateNew    true if {@link SftpSession.WriteMode#CREATE_NEW} can be honoured atomically.
 *                           The protocol has had {@code SSH_FXF_EXCL} since v3, but a backend only
 *                           qualifies if its <em>public API</em> exposes it: JSch's {@code put} takes
 *                           OVERWRITE/RESUME/APPEND and nothing else, so it reports false and throws
 *                           {@link UnsupportedOperationException} rather than emulating the flag with
 *                           a stat-then-create race
 * @param symlinks           true if {@code symlink}/{@code readlink} are supported
 * @param hardLinks          true if the {@code hardlink@openssh.com} extension is available
 * @param statvfs            true if filesystem-space queries ({@code statvfs@openssh.com}) are available
 */
public record SftpCapabilities(
    int maxSftpVersion,
    boolean pipelinedStreaming,
    boolean asyncStreaming,
    boolean posixRename,
    boolean atomicCreateNew,
    boolean symlinks,
    boolean hardLinks,
    boolean statvfs) {

    public SftpCapabilities {
        // The TCK asserts this range; a provider reporting v2 or v7 is misconfigured, not exotic.
        if (maxSftpVersion < 3 || maxSftpVersion > 6) {
            throw new IllegalArgumentException(
                "maxSftpVersion must be 3..6, got: " + maxSftpVersion);
        }
        if (posixRename && maxSftpVersion < 5) {
            throw new IllegalArgumentException(
                "posixRename requires SFTP v5 or later, got v" + maxSftpVersion);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Seven positional booleans is a bug waiting to happen; name them.
     */
    public static final class Builder {

        private int maxSftpVersion = 3;
        private boolean pipelinedStreaming;
        private boolean asyncStreaming;
        private boolean posixRename;
        private boolean atomicCreateNew;
        private boolean symlinks;
        private boolean hardLinks;
        private boolean statvfs;

        public Builder atomicCreateNew(boolean atomicCreateNew) {
            this.atomicCreateNew = atomicCreateNew;
            return this;
        }

        private Builder() {
        }

        public Builder maxSftpVersion(int maxSftpVersion) {
            this.maxSftpVersion = maxSftpVersion;
            return this;
        }

        public Builder pipelinedStreaming(boolean pipelinedStreaming) {
            this.pipelinedStreaming = pipelinedStreaming;
            return this;
        }

        public Builder asyncStreaming(boolean asyncStreaming) {
            this.asyncStreaming = asyncStreaming;
            return this;
        }

        public Builder posixRename(boolean posixRename) {
            this.posixRename = posixRename;
            return this;
        }

        public Builder symlinks(boolean symlinks) {
            this.symlinks = symlinks;
            return this;
        }

        public Builder hardLinks(boolean hardLinks) {
            this.hardLinks = hardLinks;
            return this;
        }

        public Builder statvfs(boolean statvfs) {
            this.statvfs = statvfs;
            return this;
        }

        public SftpCapabilities build() {
            return new SftpCapabilities(maxSftpVersion, pipelinedStreaming, asyncStreaming,
                posixRename, atomicCreateNew, symlinks, hardLinks, statvfs);
        }
    }
}

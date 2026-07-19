package io.github.jtsirogiannos.sftp.client.api;

/**
 * Space and inode figures for the filesystem holding a path — the answer to "will this upload fit".
 *
 * <p>From {@code statvfs@openssh.com}, an OpenSSH extension rather than anything in the SFTP drafts,
 * which is why {@link io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities#statvfs()} gates it.
 * Servers that do not advertise the extension cannot answer at all.
 *
 * <p>{@link #freeBytes()} and {@link #availableBytes()} differ by the reserve most filesystems keep
 * for root: an unprivileged upload should be sized against {@code availableBytes}.
 *
 * @param blockSize       bytes per block, as the server reports them
 * @param totalBytes      capacity of the filesystem
 * @param freeBytes       unused space, including any root reserve
 * @param availableBytes  space an unprivileged user can actually use
 * @param totalFiles      total inodes, or 0 when the server does not track them
 * @param freeFiles       unused inodes, or 0 when the server does not track them
 */
public record FilesystemStats(
        long blockSize,
        long totalBytes,
        long freeBytes,
        long availableBytes,
        long totalFiles,
        long freeFiles) {
}

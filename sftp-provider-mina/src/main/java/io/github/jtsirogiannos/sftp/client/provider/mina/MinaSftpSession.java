package io.github.jtsirogiannos.sftp.client.provider.mina;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.extensions.openssh.OpenSSHStatExtensionInfo;
import org.apache.sshd.sftp.client.extensions.openssh.OpenSSHStatPathExtension;
import org.apache.sshd.sftp.common.SftpConstants;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.io.OutputStream;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.Set;

/**
 * MINA SSHD-backed session. Its main job, beyond delegation, is that <strong>no MINA type escapes</strong>:
 * every {@code org.apache.sshd} exception is translated to {@link SftpException} before it can reach a
 * caller. That translation is the whole reason the SPI is worth having, and the TCK asserts it.
 *
 * <p>Owns three things and closes all of them: the {@link SshClient} (its own NIO threads), the
 * {@link ClientSession}, and the {@link SftpClient} channel. One transport per session is wasteful —
 * see the note on {@link MinaSftpProvider#connect} — but it makes ownership unambiguous, which
 * matters more while the pool is still the naive one.
 */
final class MinaSftpSession implements SftpSession {

    private final SshClient sshClient;
    private final ClientSession clientSession;
    private final SftpClient sftp;
    private volatile boolean closed;

    MinaSftpSession(SshClient sshClient, ClientSession clientSession, SftpClient sftp) {
        this.sshClient = sshClient;
        this.clientSession = clientSession;
        this.sftp = sftp;
    }

    @Override
    public void list(String path, Predicate<RemoteFile> visitor) {
        call("list '" + path + "'", () -> {
            // Explicit handle rather than the readDir(String) Iterable: try-with-resources
            // guarantees SSH_FXP_CLOSE even when the visitor stops halfway, where abandoning a lazy
            // iterator would leave the handle open on the server until the session died.
            try (SftpClient.CloseableHandle handle = sftp.openDir(path)) {
                AtomicReference<Boolean> endOfList = new AtomicReference<>();
                while (true) {
                    List<SftpClient.DirEntry> batch = sftp.readDir(handle, endOfList);
                    if (batch == null) {
                        return null; // server signalled EOF
                    }
                    for (SftpClient.DirEntry entry : batch) {
                        String name = entry.getFilename();
                        // MINA's server reports these; the contract says callers never see them.
                        if (".".equals(name) || "..".equals(name)) {
                            continue;
                        }
                        if (!visitor.test(toRemoteFile(name, join(path, name), entry.getAttributes()))) {
                            return null; // handle closed by try-with-resources
                        }
                    }
                    if (Boolean.TRUE.equals(endOfList.get())) {
                        return null;
                    }
                }
            }
        });
    }

    @Override
    public RemoteFile stat(String path) {
        return call("stat '" + path + "'", () -> toRemoteFile(fileName(path), path, sftp.stat(path)));
    }

    @Override
    public RemoteFile lstat(String path) {
        return call("lstat '" + path + "'", () -> toRemoteFile(fileName(path), path, sftp.lstat(path)));
    }

    /** MINA's own order is (link, target), so this passes straight through. */
    @Override
    public void symlink(String linkPath, String targetPath) {
        call("symlink '" + linkPath + "' -> '" + targetPath + "'", () -> {
            sftp.symLink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public String readlink(String linkPath) {
        return call("readlink '" + linkPath + "'", () -> sftp.readLink(linkPath));
    }

    @Override
    public boolean exists(String path) {
        try {
            sftp.stat(path);
            return true;
        } catch (org.apache.sshd.sftp.common.SftpException e) {
            // Only "it isn't there" means false. Permission denied is a real failure and must not be
            // flattened into a boolean, or callers get "no such file" for a path they simply can't see.
            if (e.getStatus() == SftpConstants.SSH_FX_NO_SUCH_FILE
                    || e.getStatus() == SftpConstants.SSH_FX_NO_SUCH_PATH) {
                return false;
            }
            throw translate("exists '" + path + "'", e);
        } catch (IOException e) {
            throw new SftpException("exists '" + path + "' failed", e);
        }
    }

    @Override
    public void mkdir(String path) {
        call("mkdir '" + path + "'", () -> {
            sftp.mkdir(path);
            return null;
        });
    }

    @Override
    public void rename(String from, String to) {
        call("rename '" + from + "' -> '" + to + "'", () -> {
            // Copy modes only where the negotiated version supports them. Atomic/Overwrite are
            // v5+; sending them to a v3 server (OpenSSH speaks nothing else) makes MINA throw
            // UnsupportedOperationException and breaks even a plain move. On v3 the bare rename is
            // all there is, and whether it replaces an existing target is then the server's call -
            // which is what capabilities() now reports honestly.
            if (sftp.getVersion() >= SftpConstants.SFTP_V5) {
                sftp.rename(from, to, SftpClient.CopyMode.Atomic, SftpClient.CopyMode.Overwrite);
            } else {
                sftp.rename(from, to);
            }
            return null;
        });
    }

    @Override
    public void remove(String path) {
        call("remove '" + path + "'", () -> {
            // SSH_FXP_REMOVE is files-only; a directory needs SSH_FXP_RMDIR. The contract here is
            // "a file, or an empty directory", so the provider picks the right request rather than
            // making every caller stat first.
            //
            // lstat, not stat: a symlink pointing at a directory must be unlinked, not rmdir'd.
            if (isDirectory(path)) {
                sftp.rmdir(path);
            } else {
                sftp.remove(path);
            }
            return null;
        });
    }

    private boolean isDirectory(String path) throws IOException {
        SftpClient.Attributes attrs = sftp.lstat(path);
        return attrs.isDirectory() && !attrs.isSymbolicLink();
    }

    @Override
    public InputStream openRead(String path, long offset) {
        return call("openRead '" + path + "' at " + offset, () -> {
            if (offset <= 0) {
                return sftp.read(path);
            }
            // MINA's stream-returning read() always starts at zero; a positioned channel is how it
            // exposes SSH_FXP_READ's offset.
            FileChannel channel = sftp.openRemoteFileChannel(path, SftpClient.OpenMode.Read);
            channel.position(offset);
            return Channels.newInputStream(channel);
        });
    }

    @Override
    public OutputStream openWrite(String path, WriteMode mode) {
        return call("openWrite '" + path + "'", () -> sftp.write(path, openModes(mode)));
    }

    /**
     * Built from the version actually negotiated, not from the provider's best case.
     *
     * <p>Against MINA's own server this is v6 with atomic rename; against OpenSSH it is v3 without.
     * Reporting the provider's static answer in both cases is how a caller ends up asking for an
     * operation the server has no way to perform.
     */
    @Override
    public SftpCapabilities capabilities() {
        int version = sftp.getVersion();
        return SftpCapabilities.builder()
                .maxSftpVersion(Math.min(Math.max(version, 3), 6))
                .pipelinedStreaming(true)
                .posixRename(version >= SftpConstants.SFTP_V5)
                // SSH_FXF_EXCL exists in v3, so exclusive create survives the downgrade.
                .atomicCreateNew(true)
                .symlinks(true)
                // Both are session-dependent, and for different reasons — which is precisely why
                // these flags cannot live on the provider.
                //
                // Hard links: MINA can only issue SFTP v6's native SSH_FXP_LINK. It parses the
                // server's hardlink@openssh.com advertisement but exposes no client call for it, so
                // against a v3 server (OpenSSH is v3-only) the extension being advertised is no use
                // to us. Asking the server was the wrong question; the negotiated version is the
                // right one.
                .hardLinks(version >= SftpConstants.SFTP_V6)
                // statvfs: MINA does implement the OpenSSH extension client-side, so here the
                // server's advertisement is exactly what decides it.
                .statvfs(sftp.getServerExtensions().containsKey("statvfs@openssh.com"))
                .build();
    }

    @Override
    public String realpath(String path) {
        return call("realpath '" + path + "'", () -> sftp.canonicalPath(path));
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        if (update.isEmpty()) {
            return; // nothing to send
        }
        call("setAttributes '" + path + "'", () -> {
            SftpClient.Attributes attrs = new SftpClient.Attributes();
            update.permissions().ifPresent(attrs::perms);
            update.size().ifPresent(attrs::size);
            update.modifiedTime().ifPresent(when -> attrs.modifyTime(when.getEpochSecond()));
            // owner(uid, gid) sets both, so only call it when at least one was asked for, filling
            // the other from the current attributes rather than zeroing it.
            if (update.ownerId().isPresent() || update.groupId().isPresent()) {
                SftpClient.Attributes current = sftp.stat(path);
                attrs.owner(update.ownerId().orElse(current.getUserId()),
                        update.groupId().orElse(current.getGroupId()));
            }
            sftp.setStat(path, attrs);
            return null;
        });
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        requireCapability(capabilities().hardLinks(), "hard links");
        call("hardlink '" + linkPath + "' -> '" + targetPath + "'", () -> {
            // link(link, target, symbolic=false); MINA's order already matches this SPI.
            sftp.link(linkPath, targetPath, false);
            return null;
        });
    }

    @Override
    public FilesystemStats statvfs(String path) {
        requireCapability(capabilities().statvfs(), "statvfs");
        return call("statvfs '" + path + "'", () -> {
            OpenSSHStatPathExtension extension = sftp.getExtension(OpenSSHStatPathExtension.class);
            if (extension == null || !extension.isSupported()) {
                throw new UnsupportedOperationException(
                        "the server does not advertise statvfs@openssh.com");
            }
            OpenSSHStatExtensionInfo info = extension.stat(path);
            return new FilesystemStats(info.f_frsize, info.f_blocks * info.f_frsize,
                    info.f_bfree * info.f_frsize, info.f_bavail * info.f_frsize,
                    info.f_files, info.f_ffree);
        });
    }

    private static void requireCapability(boolean present, String what) {
        if (!present) {
            throw new UnsupportedOperationException(
                    "this session does not support " + what + "; check capabilities() first");
        }
    }

    @Override
    public boolean isValid() {
        // Local state only, per the contract: no round-trip. A failed operation that left the
        // channel intact (a stat on a missing file) must not invalidate the session.
        return !closed && clientSession.isOpen() && sftp.isOpen();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Innermost first, and each independently: a failure closing the channel must not strand
        // the transport or leak the client's NIO threads.
        closeQuietly(sftp);
        closeQuietly(clientSession);
        try {
            sshClient.stop();
        } catch (RuntimeException ignored) {
            // closing is not a failure mode worth propagating
        }
    }

    /**
     * {@code CREATE_NEW} maps to {@code Exclusive}, which is the server-side {@code SSH_FXF_EXCL}
     * flag — the create-or-fail decision happens atomically in one round-trip. A stat-then-create
     * here would be a race, and the TCK is written to catch exactly that shortcut.
     */
    private static Set<SftpClient.OpenMode> openModes(WriteMode mode) {
        return switch (mode) {
            case OVERWRITE -> EnumSet.of(SftpClient.OpenMode.Write, SftpClient.OpenMode.Create,
                    SftpClient.OpenMode.Truncate);
            case CREATE_NEW -> EnumSet.of(SftpClient.OpenMode.Write, SftpClient.OpenMode.Create,
                    SftpClient.OpenMode.Exclusive);
            case APPEND -> EnumSet.of(SftpClient.OpenMode.Write, SftpClient.OpenMode.Create,
                    SftpClient.OpenMode.Append);
        };
    }

    private static RemoteFile toRemoteFile(String name, String path, SftpClient.Attributes attrs) {
        FileTime modified = attrs.getModifyTime();
        // UidGid only, emphatically not OwnerGroup: v6 servers send owner *names*, and MINA's
        // getUserId() then falls back to 0 — which would report every file as owned by root.
        // A confidently wrong uid is worse than UNKNOWN_ID; the probe that caught this saw 0/0
        // from a v6 server whose files belonged to an ordinary user.
        boolean hasIds = attrs.getFlags().contains(SftpClient.Attribute.UidGid);
        return new RemoteFile(
                name,
                path,
                attrs.isDirectory() ? 0 : Math.max(0, attrs.getSize()),
                attrs.isDirectory(),
                attrs.isSymbolicLink(),
                modified == null ? Instant.EPOCH : modified.toInstant(),
                attrs.getPermissions(),
                hasIds ? attrs.getUserId() : RemoteFile.UNKNOWN_ID,
                hasIds ? attrs.getGroupId() : RemoteFile.UNKNOWN_ID);
    }

    private static String join(String dir, String name) {
        return dir.endsWith("/") ? dir + name : dir + "/" + name;
    }

    private static String fileName(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }

    /** Runs a MINA call and guarantees the caller sees only {@link SftpException}. */
    private <T> T call(String what, SftpOperation<T> op) {
        try {
            return op.run();
        } catch (org.apache.sshd.sftp.common.SftpException e) {
            throw translate(what, e);
        } catch (IOException e) {
            throw new SftpException(what + " failed", e);
        }
    }

    /** Carries the server's {@code SSH_FX_*} status across, so callers need not parse messages. */
    private static SftpException translate(String what, org.apache.sshd.sftp.common.SftpException e) {
        return new SftpException(what + " failed: " + e.getMessage(), e, e.getStatus());
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best effort
        }
    }

    @FunctionalInterface
    private interface SftpOperation<T> {
        T run() throws IOException;
    }
}

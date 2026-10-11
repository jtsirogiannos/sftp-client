package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.api.SftpPaths;
import io.github.jtsirogiannos.sftp.client.core.ForwardingSftpClientSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Every operation, confined to a {@link RootDirectory}: paths rewritten on the way in and on the way
 * out.
 *
 * <p>Every primitive is overridden, including those whose only argument is a path, so nothing can
 * reach the delegate unrewritten. The convenience methods on {@link SftpOperations} are deliberately
 * <em>not</em> overridden: they are composed from these primitives, so {@code mkdirs},
 * {@code deleteRecursively} and {@code uploadAtomically} run in the view's namespace and their
 * every step is confined. {@code deleteRecursively("/")} is refused by the default's own guard,
 * which here protects the root directory itself.
 *
 * <h2>Links</h2>
 *
 * <p>A link created through the view cannot point out of it. An absolute target is rewritten into
 * the root like any other path; a relative one is checked lexically against the link's directory
 * and passed through unchanged, so it still resolves where it lies. A link created by some other
 * means can point anywhere, and the server will follow it — which is why a root directory is a
 * namespace and not a security boundary.
 */
final class RootedSftpOperations implements SftpOperations {

    private final SftpOperations delegate;
    private final RootDirectory root;

    RootedSftpOperations(SftpOperations delegate, RootDirectory root) {
        this.delegate = delegate;
        this.root = root;
    }

    /** A session confined the same way, whose lifecycle stays with {@code session}. */
    static SftpClientSession session(SftpClientSession session, RootDirectory root) {
        RootedSftpOperations rooted = new RootedSftpOperations(session, root);
        return new ForwardingSftpClientSession() {
            @Override
            protected SftpOperations delegate() {
                return rooted;
            }

            @Override
            protected SftpClientSession session() {
                return session;
            }

            @Override
            public void visit(String path, Predicate<RemoteFile> visitor) {
                // Not forwarded by the base class; forwarded here so the walk stays streaming.
                rooted.visit(path, visitor);
            }
        };
    }

    @Override
    public List<RemoteFile> list(String path) {
        List<RemoteFile> entries = delegate.list(root.toServer(path));
        List<RemoteFile> translated = new ArrayList<>(entries.size());
        for (RemoteFile entry : entries) {
            translated.add(translate(entry));
        }
        return translated;
    }

    @Override
    public void visit(String path, Predicate<RemoteFile> visitor) {
        delegate.visit(root.toServer(path), entry -> visitor.test(translate(entry)));
    }

    @Override
    public RemoteFile stat(String path) {
        return translate(delegate.stat(root.toServer(path)));
    }

    @Override
    public RemoteFile lstat(String path) {
        return translate(delegate.lstat(root.toServer(path)));
    }

    @Override
    public boolean exists(String path) {
        return delegate.exists(root.toServer(path));
    }

    @Override
    public String realpath(String path) {
        return root.fromServer(SftpPaths.normalize(delegate.realpath(root.toServer(path))), delegate);
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        String link = root.toServer(linkPath);
        String target;
        if (targetPath.startsWith("/")) {
            target = root.toServer(targetPath);
        } else {
            // Checked against where the link will live, then passed through as written, so the
            // server resolves it relative to the link exactly as the caller meant.
            root.toServer(parentOf(root.normalise(linkPath)) + "/" + targetPath);
            target = targetPath;
        }
        delegate.symlink(link, target);
    }

    @Override
    public String readlink(String linkPath) {
        String stored = delegate.readlink(root.toServer(linkPath));
        // A relative target means the same thing inside the view as outside it.
        return stored.startsWith("/") ? root.fromServer(stored, delegate) : stored;
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        delegate.hardlink(root.toServer(linkPath), root.toServer(targetPath));
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        delegate.setAttributes(root.toServer(path), update);
    }

    @Override
    public FilesystemStats statvfs(String path) {
        return delegate.statvfs(root.toServer(path));
    }

    @Override
    public void mkdir(String path) {
        delegate.mkdir(root.toServer(path));
    }

    @Override
    public void rename(String from, String to) {
        delegate.rename(root.toServer(from), root.toServer(to));
    }

    @Override
    public void remove(String path) {
        delegate.remove(root.toServer(path));
    }

    @Override
    public InputStream download(String remotePath) {
        return delegate.download(root.toServer(remotePath));
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        return delegate.download(root.toServer(remotePath), target);
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        return delegate.download(root.toServer(remotePath), target, fromOffset);
    }

    @Override
    public void upload(String remotePath, InputStream data) {
        delegate.upload(root.toServer(remotePath), data);
    }

    @Override
    public void upload(String remotePath, InputStream data, SftpSession.WriteMode mode) {
        delegate.upload(root.toServer(remotePath), data, mode);
    }

    @Override
    public SftpCapabilities capabilities() {
        return delegate.capabilities();
    }

    /**
     * An entry with its path in the view's namespace. The root itself is reported nameless, as a
     * provider reports {@code /}.
     */
    private RemoteFile translate(RemoteFile entry) {
        String path = root.fromServer(entry.path(), delegate);
        String name = "/".equals(path) ? "" : entry.name();
        return new RemoteFile(name, path, entry.size(), entry.directory(), entry.symlink(),
                entry.modified(), entry.permissions(), entry.ownerId(), entry.groupId());
    }

    private static String parentOf(String path) {
        int slash = path.lastIndexOf('/');
        return slash <= 0 ? "/" : path.substring(0, slash);
    }
}

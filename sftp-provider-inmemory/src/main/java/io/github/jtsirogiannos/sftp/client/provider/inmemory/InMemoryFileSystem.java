package io.github.jtsirogiannos.sftp.client.provider.inmemory;

import io.github.jtsirogiannos.sftp.client.api.SftpException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The store behind {@link InMemorySftpProvider}: a flat map of absolute path to node.
 *
 * <p>Flat rather than a tree because SFTP addresses everything by absolute path anyway, and a
 * directory listing is a prefix scan. The parts that need care are the parts a naive map gets wrong,
 * and each of them is a conformance test:
 *
 * <ul>
 *   <li><strong>Symlinks are nodes, not aliases.</strong> {@code stat} resolves them and {@code
 *       lstat} does not, so the link has to exist as an entry in its own right.</li>
 *   <li><strong>Hard links share the node object.</strong> Two paths, one {@link Node}, so writing
 *       through one is visible through the other and neither is a symlink.</li>
 *   <li><strong>Loops must terminate.</strong> A symlink chain is followed with a hop limit rather
 *       than recursion that a malicious fixture could run forever.</li>
 * </ul>
 *
 * <p>Guarded by a lock: sessions are single-owner, but one filesystem is shared by every session a
 * provider hands out, and tests do run concurrently.
 */
final class InMemoryFileSystem {

    private static final int MAX_SYMLINK_HOPS = 16;

    /** Plausible figures for statvfs; nothing here is really constrained by disk. */
    static final long TOTAL_BYTES = 64L * 1024 * 1024 * 1024;
    static final long FREE_BYTES = 32L * 1024 * 1024 * 1024;

    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    InMemoryFileSystem() {
        nodes.put("/", Node.directory());
        // The conformance suite and the shipped fixtures both write under /upload, matching what
        // EmbeddedSftpServer provides. Creating it here keeps a fake and a real server swappable.
        nodes.put("/upload", Node.directory());
    }

    <R> R read(PathOperation<R> operation) {
        lock.lock();
        try {
            return operation.run();
        } finally {
            lock.unlock();
        }
    }

    // --- lookups ---

    /** The node at {@code path} without following a trailing symlink. */
    Node lstat(String path) {
        Node node = nodes.get(normalise(path));
        if (node == null) {
            throw noSuchFile(path);
        }
        return node;
    }

    /** The node at {@code path}, following symlinks. */
    Node stat(String path) {
        return lstat(resolveLinks(path));
    }

    boolean exists(String path) {
        return nodes.containsKey(normalise(path));
    }

    /** The path a symlink chain ends at; unchanged when nothing along it is a link. */
    String resolveLinks(String path) {
        String current = normalise(path);
        for (int hop = 0; hop < MAX_SYMLINK_HOPS; hop++) {
            Node node = nodes.get(current);
            if (node == null || node.kind != Node.Kind.SYMLINK) {
                return current;
            }
            current = normalise(node.linkTarget);
        }
        throw new SftpException("too many symbolic links resolving '" + path + "'");
    }

    List<String> childPaths(String directory) {
        String resolved = resolveLinks(directory);
        Node node = nodes.get(resolved);
        if (node == null) {
            throw noSuchFile(directory);
        }
        if (node.kind != Node.Kind.DIRECTORY) {
            throw new SftpException("not a directory: " + directory, null,
                    SftpException.SSH_FX_FAILURE);
        }
        String prefix = resolved.endsWith("/") ? resolved : resolved + "/";
        List<String> children = new ArrayList<>();
        for (String candidate : new ArrayList<>(nodes.keySet())) {
            if (candidate.startsWith(prefix) && !candidate.substring(prefix.length()).contains("/")) {
                children.add(candidate);
            }
        }
        return children;
    }

    // --- mutations ---

    void put(String path, Node node) {
        nodes.put(normalise(path), node);
    }

    void createDirectory(String path) {
        String normalised = normalise(path);
        if (nodes.containsKey(normalised)) {
            throw new SftpException("already exists: " + path, null,
                    SftpException.SSH_FX_FILE_ALREADY_EXISTS);
        }
        requireParent(normalised);
        nodes.put(normalised, Node.directory());
    }

    void remove(String path) {
        String normalised = normalise(path);
        Node node = nodes.get(normalised);
        if (node == null) {
            throw noSuchFile(path);
        }
        if (node.kind == Node.Kind.DIRECTORY && !childPaths(normalised).isEmpty()) {
            throw new SftpException("directory not empty: " + path, null,
                    SftpException.SSH_FX_FAILURE);
        }
        nodes.remove(normalised);
    }

    /**
     * @param replaceExisting what {@code posixRename} means; when false an occupied target fails,
     *                        which is how SFTP v3 behaves
     */
    void rename(String from, String to, boolean replaceExisting) {
        String source = normalise(from);
        String target = normalise(to);
        Node node = nodes.get(source);
        if (node == null) {
            throw noSuchFile(from);
        }
        if (nodes.containsKey(target) && !replaceExisting) {
            throw new SftpException("already exists: " + to, null,
                    SftpException.SSH_FX_FILE_ALREADY_EXISTS);
        }
        requireParent(target);
        nodes.remove(source);
        nodes.put(target, node);
    }

    /** Both paths end up pointing at the same node, which is what makes it a hard link. */
    void hardlink(String linkPath, String targetPath) {
        Node target = nodes.get(resolveLinks(targetPath));
        if (target == null) {
            throw noSuchFile(targetPath);
        }
        String link = normalise(linkPath);
        if (nodes.containsKey(link)) {
            throw new SftpException("already exists: " + linkPath, null,
                    SftpException.SSH_FX_FILE_ALREADY_EXISTS);
        }
        requireParent(link);
        nodes.put(link, target);
    }

    private void requireParent(String normalisedPath) {
        int slash = normalisedPath.lastIndexOf('/');
        String parent = slash <= 0 ? "/" : normalisedPath.substring(0, slash);
        Node node = nodes.get(parent);
        if (node == null || node.kind != Node.Kind.DIRECTORY) {
            throw new SftpException("no such directory: " + parent, null,
                    SftpException.SSH_FX_NO_SUCH_FILE);
        }
    }

    static SftpException noSuchFile(String path) {
        return new SftpException("no such file: " + path, null, SftpException.SSH_FX_NO_SUCH_FILE);
    }

    /** Collapses {@code //}, {@code .} and {@code ..}; the answer {@code realpath} gives. */
    static String normalise(String path) {
        String working = path.startsWith("/") ? path : "/" + path;
        List<String> segments = new ArrayList<>();
        for (String segment : working.split("/")) {
            if (segment.isEmpty() || ".".equals(segment)) {
                continue;
            }
            if ("..".equals(segment)) {
                if (!segments.isEmpty()) {
                    segments.remove(segments.size() - 1);
                }
                continue;
            }
            segments.add(segment);
        }
        return segments.isEmpty() ? "/" : "/" + String.join("/", segments);
    }

    @FunctionalInterface
    interface PathOperation<R> {
        R run();
    }

    /** A file, directory or symlink. Shared by reference when hard-linked. */
    static final class Node {

        enum Kind { FILE, DIRECTORY, SYMLINK }

        Kind kind;
        byte[] content;
        String linkTarget;
        int permissions;
        Instant modified = Instant.EPOCH;
        int ownerId;
        int groupId;

        static Node directory() {
            Node node = new Node();
            node.kind = Kind.DIRECTORY;
            node.permissions = 0755;
            return node;
        }

        static Node file(byte[] content) {
            Node node = new Node();
            node.kind = Kind.FILE;
            node.content = content;
            node.permissions = 0644;
            return node;
        }

        static Node symlink(String target) {
            Node node = new Node();
            node.kind = Kind.SYMLINK;
            node.linkTarget = target;
            node.permissions = 0777;
            return node;
        }

        long size() {
            return kind == Kind.FILE ? content.length : 0;
        }
    }
}

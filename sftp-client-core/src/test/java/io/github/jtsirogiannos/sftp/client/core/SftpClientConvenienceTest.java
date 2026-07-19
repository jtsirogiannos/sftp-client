package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The convenience methods on {@link SftpOperations} are default methods composed from the primitives, so
 * what can go wrong is the composition: the order of a recursive delete, whether a symlink is
 * followed out of the tree, whether {@code mkdirs} copes with a parent that already exists.
 *
 * <p>Tested against an in-memory filesystem rather than a server. Core may not depend on a provider
 * — that separation is the whole point of the module — and the underlying operations are already
 * conformance-tested against two real servers. What is untested until here is the orchestration.
 */
public class SftpClientConvenienceTest {

    private InMemoryClient client;

    @BeforeMethod
    public void setUp() {
        client = new InMemoryClient();
        client.dirs.add("/upload");
    }

    // --- mkdirs ---

    @Test
    public void mkdirsCreatesEveryMissingParent() {
        client.mkdirs("/upload/2026/07/18");

        assertThat(client.exists("/upload/2026")).isTrue();
        assertThat(client.exists("/upload/2026/07")).isTrue();
        assertThat(client.exists("/upload/2026/07/18")).isTrue();
    }

    @Test
    public void mkdirsIsIdempotent() {
        client.mkdirs("/upload/a/b");
        int created = client.mkdirCalls;

        client.mkdirs("/upload/a/b");

        assertThat(client.mkdirCalls)
                .describedAs("a second call should create nothing")
                .isEqualTo(created);
    }

    @Test
    public void mkdirsSkipsSegmentsThatAlreadyExist() {
        client.mkdirs("/upload/a");
        client.mkdirCalls = 0;

        client.mkdirs("/upload/a/b/c");

        assertThat(client.mkdirCalls)
                .describedAs("only the two missing segments should be created")
                .isEqualTo(2);
    }

    @Test
    public void mkdirsToleratesATrailingSlash() {
        client.mkdirs("/upload/trailing/");
        assertThat(client.exists("/upload/trailing")).isTrue();
    }

    /** A concurrent creator winning the race is success, not failure. */
    @Test
    public void mkdirsSurvivesLosingACreateRace() {
        client.createRaceOn = "/upload/contended";

        client.mkdirs("/upload/contended/child");

        assertThat(client.exists("/upload/contended/child")).isTrue();
    }

    @Test
    public void mkdirsPropagatesAGenuineFailure() {
        client.failMkdirOn = "/upload/denied";
        assertThatThrownBy(() -> client.mkdirs("/upload/denied/child"))
                .isInstanceOf(SftpException.class);
    }

    // --- deleteRecursively ---

    @Test
    public void deleteRecursivelyClearsTheWholeTree() {
        client.mkdirs("/upload/tree/nested");
        client.putFile("/upload/tree/a.txt", "a");
        client.putFile("/upload/tree/nested/b.txt", "b");

        int removed = client.deleteRecursively("/upload/tree");

        assertThat(removed).describedAs("2 files + 2 directories").isEqualTo(4);
        assertThat(client.exists("/upload/tree")).isFalse();
        assertThat(client.exists("/upload")).describedAs("the parent must survive").isTrue();
    }

    @Test
    public void deleteRecursivelyOnAPlainFileRemovesJustIt() {
        client.putFile("/upload/single.txt", "x");
        assertThat(client.deleteRecursively("/upload/single.txt")).isEqualTo(1);
        assertThat(client.exists("/upload/single.txt")).isFalse();
    }

    @Test
    public void deleteRecursivelyOnAMissingPathDoesNothing() {
        assertThat(client.deleteRecursively("/upload/never-existed")).isZero();
    }

    /**
     * The one that would cause an incident: descending into a symlink deletes files outside the tree
     * the caller named.
     */
    @Test
    public void deleteRecursivelyRemovesSymlinksWithoutFollowingThem() {
        client.mkdirs("/elsewhere");
        client.putFile("/elsewhere/precious.txt", "do not delete me");
        client.mkdirs("/upload/tree");
        client.putSymlinkToDirectory("/upload/tree/link", "/elsewhere");

        int removed = client.deleteRecursively("/upload/tree");

        assertThat(removed).describedAs("the link and its parent, not the link's contents").isEqualTo(2);
        assertThat(client.exists("/elsewhere/precious.txt"))
                .describedAs("followed the symlink and deleted outside the tree")
                .isTrue();
    }

    @Test
    public void deleteRecursivelyRefusesTheRemoteRoot() {
        assertThatThrownBy(() -> client.deleteRecursively("/"))
                .isInstanceOf(SftpException.class)
                .hasMessageContaining("refusing");
    }

    // --- local file transfers and filtering ---

    @Test
    public void downloadsToALocalFileAndUploadsBack() throws Exception {
        client.putFile("/upload/data.txt", "round trip");
        Path temp = Files.createTempFile("sftp-convenience", ".txt");
        try {
            long copied = client.download("/upload/data.txt", temp);

            assertThat(copied).isEqualTo(10);
            assertThat(Files.readString(temp)).isEqualTo("round trip");

            client.upload(temp, "/upload/copy.txt");
            assertThat(client.readFile("/upload/copy.txt")).isEqualTo("round trip");
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    @Test
    public void uploadFromAMissingLocalFileFailsAsSftpException() {
        assertThatThrownBy(() -> client.upload(Path.of("/definitely/not/here.txt"), "/upload/x.txt"))
                .isInstanceOf(SftpException.class)
                .hasMessageContaining("could not read");
    }

    @Test
    public void listFiltersEntries() {
        client.putFile("/upload/keep.csv", "1");
        client.putFile("/upload/skip.txt", "2");

        List<RemoteFile> csv = client.list("/upload", f -> f.name().endsWith(".csv"));

        assertThat(csv).extracting(RemoteFile::name).containsExactly("keep.csv");
    }

    /** A minimal in-memory server: enough to exercise composition, and nothing more. */
    private static final class InMemoryClient implements SftpOperations {

        final Map<String, byte[]> files = new LinkedHashMap<>();
        final Set<String> dirs = new LinkedHashSet<>();
        final Set<String> symlinks = new LinkedHashSet<>();
        final Map<String, String> linkTargets = new LinkedHashMap<>();
        int mkdirCalls;
        String failMkdirOn;
        String createRaceOn;

        void putFile(String path, String content) {
            files.put(path, content.getBytes(StandardCharsets.UTF_8));
        }

        String readFile(String path) {
            return new String(files.get(path), StandardCharsets.UTF_8);
        }

        void putSymlinkToDirectory(String path, String target) {
            symlinks.add(path);
            linkTargets.put(path, target);
            dirs.add(target);
        }

        private static String nameOf(String path) {
            return path.substring(path.lastIndexOf('/') + 1);
        }

        /**
         * Follows symlinks, as opening a directory on a real server does: listing a link to a
         * directory yields the <em>target's</em> entries. Without this the fake makes recursion into
         * a symlink look harmless, and the test that is supposed to catch it proves nothing.
         */
        @Override
        public List<RemoteFile> list(String path) {
            String resolved = symlinks.contains(path) ? linkTargets.get(path) : path;
            String prefix = resolved.endsWith("/") ? resolved : resolved + "/";
            List<RemoteFile> entries = new ArrayList<>();
            for (String candidate : new ArrayList<>(dirs)) {
                if (isDirectChild(candidate, prefix)) {
                    entries.add(entry(candidate));
                }
            }
            for (String candidate : new ArrayList<>(files.keySet())) {
                if (isDirectChild(candidate, prefix)) {
                    entries.add(entry(candidate));
                }
            }
            for (String candidate : new ArrayList<>(symlinks)) {
                if (isDirectChild(candidate, prefix)) {
                    entries.add(entry(candidate));
                }
            }
            return entries;
        }

        private static boolean isDirectChild(String candidate, String prefix) {
            return candidate.startsWith(prefix) && !candidate.substring(prefix.length()).contains("/");
        }

        private RemoteFile entry(String path) {
            String name = path.substring(path.lastIndexOf('/') + 1);
            boolean symlink = symlinks.contains(path);
            boolean directory = !symlink && dirs.contains(path);
            long size = files.containsKey(path) ? files.get(path).length : 0;
            return new RemoteFile(name, path, size, directory, symlink, Instant.EPOCH, 0644);
        }

        /**
         * Follows symlinks, exactly as SSH_FXP_STAT does. That is the whole reason lstat exists —
         * and modelling it faithfully is what turns "deleteRecursively must not follow links" from
         * an assertion into a real test.
         */
        @Override
        public RemoteFile stat(String path) {
            if (!exists(path)) {
                throw new SftpException("no such file: " + path, null,
                        SftpException.SSH_FX_NO_SUCH_FILE);
            }
            if (symlinks.contains(path)) {
                RemoteFile target = entry(linkTargets.get(path));
                // Reported as the target: a caller cannot tell it went through a link.
                return new RemoteFile(nameOf(path), path, target.size(), target.directory(), false,
                        Instant.EPOCH, 0644);
            }
            return entry(path);
        }

        @Override
        public RemoteFile lstat(String path) {
            if (!exists(path)) {
                throw new SftpException("no such file: " + path, null,
                        SftpException.SSH_FX_NO_SUCH_FILE);
            }
            return entry(path);
        }

        @Override
        public void symlink(String linkPath, String targetPath) {
            symlinks.add(linkPath);
            linkTargets.put(linkPath, targetPath);
        }

        @Override
        public String readlink(String linkPath) {
            String target = linkTargets.get(linkPath);
            if (target == null) {
                throw new SftpException("not a symlink: " + linkPath);
            }
            return target;
        }

        @Override
        public boolean exists(String path) {
            return files.containsKey(path) || dirs.contains(path) || symlinks.contains(path);
        }

        @Override
        public void mkdir(String path) {
            mkdirCalls++;
            if (path.equals(failMkdirOn)) {
                throw new SftpException("permission denied: " + path, null,
                        SftpException.SSH_FX_PERMISSION_DENIED);
            }
            if (path.equals(createRaceOn)) {
                // Someone else created it a moment ago; the server reports it already exists.
                dirs.add(path);
                throw new SftpException("already exists: " + path, null,
                        SftpException.SSH_FX_FILE_ALREADY_EXISTS);
            }
            if (exists(path)) {
                throw new SftpException("already exists: " + path, null,
                        SftpException.SSH_FX_FILE_ALREADY_EXISTS);
            }
            dirs.add(path);
        }

        @Override
        public void remove(String path) {
            if (!exists(path)) {
                throw new SftpException("no such file: " + path, null,
                        SftpException.SSH_FX_NO_SUCH_FILE);
            }
            files.remove(path);
            dirs.remove(path);
            symlinks.remove(path);
        }

        @Override
        public void rename(String from, String to) {
            byte[] content = files.remove(from);
            if (content != null) {
                files.put(to, content);
            }
        }

        @Override
        public InputStream download(String remotePath) {
            return new ByteArrayInputStream(files.get(remotePath));
        }

        @Override
        public long download(String remotePath, OutputStream target) {
            byte[] content = files.get(remotePath);
            if (content == null) {
                throw new SftpException("no such file: " + remotePath, null,
                        SftpException.SSH_FX_NO_SUCH_FILE);
            }
            try {
                target.write(content);
                return content.length;
            } catch (java.io.IOException e) {
                throw new SftpException("write failed", e);
            }
        }

        @Override
        public void upload(String remotePath, InputStream data) {
            try {
                files.put(remotePath, data.readAllBytes());
            } catch (java.io.IOException e) {
                throw new SftpException("read failed", e);
            }
        }

        @Override
        public void upload(String remotePath, InputStream data, SftpSession.WriteMode mode) {
            upload(remotePath, data);
        }

        @Override
        public String realpath(String path) {
            return path;
        }

        @Override
        public void setAttributes(String path,
                io.github.jtsirogiannos.sftp.client.api.AttributeUpdate update) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void hardlink(String linkPath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.github.jtsirogiannos.sftp.client.api.FilesystemStats statvfs(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long download(String remotePath, OutputStream target, long fromOffset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(3).symlinks(true).build();
        }

    }
}

package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive facade's default methods spell paths the same way the blocking ones do — relative
 * stays relative, and the deletion guard looks at what a path means rather than how it is written.
 * Against a recording stub, because the difference between {@code "a"} and {@code "/a"} vanishes on a
 * test server whose login directory is {@code /}.
 */
public class ReactivePathHandlingTest {

    @Test
    public void deleteRecursivelyRefusesEverySpellingOfTheRootByFailingTheStage() {
        RecordingClient client = new RecordingClient();
        for (String root : List.of("/", "/.", "//", "/upload/..")) {
            CompletableFuture<Integer> stage = client.deleteRecursively(root).toCompletableFuture();
            assertThat(stage).describedAs(root).isCompletedExceptionally();
            assertThat(stage.handle((ok, failure) -> failure).join())
                    .isInstanceOf(SftpException.class)
                    .hasMessageContaining("refusing to delete the remote root");
        }
        assertThat(client.calls).describedAs("nothing may reach the server").isEmpty();
    }

    @Test
    public void deleteRecursivelyRefusesTheLoginDirectory() {
        RecordingClient client = new RecordingClient();
        for (String home : List.of(".", "a/..", "..")) {
            assertThat(client.deleteRecursively(home).toCompletableFuture())
                    .describedAs(home)
                    .isCompletedExceptionally();
        }
        assertThat(client.calls).isEmpty();
    }

    @Test
    public void mkdirsKeepsARelativePathRelative() {
        RecordingClient client = new RecordingClient();

        client.mkdirs("reports/2026/10").toCompletableFuture().join();

        assertThat(client.dirs).containsExactlyInAnyOrder("reports", "reports/2026", "reports/2026/10");
    }

    @Test
    public void uploadAtomicallyKeepsTheScratchFileBesideARelativeDestination() {
        RecordingClient client = new RecordingClient();

        client.uploadAtomically("orders.csv", subscriber -> subscriber.onSubscribe(
                new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                        subscriber.onComplete();
                    }

                    @Override
                    public void cancel() {
                    }
                })).toCompletableFuture().join();

        assertThat(client.uploads).hasSize(1);
        assertThat(client.uploads.get(0)).startsWith(".orders.csv.").endsWith(".part");
    }

    /** Directories and uploads are remembered; everything off the paths under test is refused. */
    private static final class RecordingClient implements ReactiveSftpOperations {

        final Set<String> dirs = ConcurrentHashMap.newKeySet();
        final List<String> uploads = new CopyOnWriteArrayList<>();
        final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public CompletionStage<Boolean> exists(String path) {
            calls.add("exists " + path);
            return CompletableFuture.completedFuture(dirs.contains(path));
        }

        @Override
        public CompletionStage<Void> mkdir(String path) {
            calls.add("mkdir " + path);
            dirs.add(path);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
                SftpSession.WriteMode mode) {
            uploads.add(remotePath);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> rename(String from, String to) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(6).posixRename(true).build();
        }

        @Override
        public CompletionStage<List<RemoteFile>> list(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<RemoteFile> stat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> remove(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<RemoteFile> lstat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<String> realpath(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> symlink(String linkPath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<String> readlink(String linkPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<FilesystemStats> statvfs(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
            throw new UnsupportedOperationException();
        }

    }
}

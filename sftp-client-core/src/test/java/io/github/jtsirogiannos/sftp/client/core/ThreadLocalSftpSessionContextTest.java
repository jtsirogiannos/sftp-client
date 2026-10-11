package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.context.SessionBindings;
import io.github.jtsirogiannos.sftp.client.context.SessionKey;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The default context, and the key and snapshot types every context is built on. */
public class ThreadLocalSftpSessionContextTest {

    private final SftpSessionContext context = ThreadLocalSftpSessionContext.instance();
    private final Object owner = new Object();
    private final SessionKey<String> key = SessionKey.of(String.class, owner);

    @Test
    public void bindsForTheExtentOfTheCallAndRestoresAfterwards() {
        assertThat(context.current(key)).isEmpty();

        String seen = context.callWithin(key, "outer", () -> {
            String inner = context.callWithin(key, "inner", () -> context.current(key).orElseThrow());
            assertThat(inner).isEqualTo("inner");
            return context.current(key).orElseThrow();
        });

        assertThat(seen).describedAs("the nested binding was undone").isEqualTo("outer");
        assertThat(context.current(key)).isEmpty();
        assertThat(context.capture().isEmpty()).isTrue();
    }

    @Test
    public void restoresOnTheWayOutOfAFailure() {
        assertThatThrownBy(() -> context.callWithin(key, "bound", () -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(context.current(key)).isEmpty();
    }

    @Test
    public void anotherThreadSeesNothing() throws Exception {
        String elsewhere = context.callWithin(key, "here", () -> {
            try {
                return CompletableFuture.supplyAsync(() -> context.current(key).orElse("nothing"))
                        .get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        assertThat(elsewhere).isEqualTo("nothing");
    }

    @Test
    public void aCapturedSnapshotReplaysOnAnotherThread() throws Exception {
        SessionBindings captured = context.callWithin(key, "carried", context::capture);

        String replayed = CompletableFuture.supplyAsync(
                        () -> context.callWith(captured, () -> context.current(key).orElse("lost")))
                .get(5, TimeUnit.SECONDS);

        assertThat(replayed).isEqualTo("carried");
    }

    @Test
    public void keysCompareOwnersByIdentityAndTellTenantsApart() {
        record Config(String host) {
        }
        Config one = new Config("same");
        Config two = new Config("same");
        assertThat(one).isEqualTo(two);

        assertThat(SessionKey.of(String.class, one)).isNotEqualTo(SessionKey.of(String.class, two));
        assertThat(SessionKey.of(String.class, one)).isEqualTo(SessionKey.of(String.class, one));
        assertThat(SessionKey.of(String.class, one).forTenant("acme"))
                .isNotEqualTo(SessionKey.of(String.class, one))
                .isNotEqualTo(SessionKey.of(String.class, one).forTenant("globex"))
                .isEqualTo(SessionKey.of(String.class, one).forTenant("acme"));
        assertThat(SessionKey.of(String.class, one)).isNotEqualTo(SessionKey.of(Integer.class, one));
    }

    @Test
    public void tenantsBoundTogetherStaySeparate() {
        SessionKey<String> acme = key.forTenant("acme");
        SessionKey<String> globex = key.forTenant("globex");

        context.callWithin(acme, "acme-session", () -> context.callWithin(globex, "globex-session", () -> {
            assertThat(context.current(acme)).contains("acme-session");
            assertThat(context.current(globex)).contains("globex-session");
            assertThat(context.current(key)).isEmpty();
            return null;
        }));
    }
}

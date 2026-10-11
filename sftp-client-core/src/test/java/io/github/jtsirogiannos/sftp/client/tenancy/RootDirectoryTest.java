package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The mapping rules of a confined view, as a table: what goes in, and what is refused. */
public class RootDirectoryTest {

    private final RootDirectory root = RootDirectory.of("/tenants/acme/");

    @DataProvider
    public Object[][] mapped() {
        return new Object[][] {
            {"/", "/tenants/acme"},
            {".", "/tenants/acme"},
            {"/inbound", "/tenants/acme/inbound"},
            {"inbound", "/tenants/acme/inbound"},
            {"inbound/", "/tenants/acme/inbound"},
            {"//inbound//orders.csv", "/tenants/acme/inbound/orders.csv"},
            {"/inbound/./orders.csv", "/tenants/acme/inbound/orders.csv"},
            {"/inbound/../outbound", "/tenants/acme/outbound"},
            {"a/b/../../c", "/tenants/acme/c"},
            {"/a*b?.csv", "/tenants/acme/a*b?.csv"},
            {"/name with spaces", "/tenants/acme/name with spaces"},
            {"/.hidden", "/tenants/acme/.hidden"},
            {"/...", "/tenants/acme/..."},
        };
    }

    @Test(dataProvider = "mapped")
    public void mapsIntoTheRoot(String inView, String onServer) {
        assertThat(root.toServer(inView)).isEqualTo(onServer);
    }

    @DataProvider
    public Object[][] refused() {
        return new Object[][] {
            {"", "empty"},
            {"..", "climbs"},
            {"/..", "climbs"},
            {"/../other-tenant", "climbs"},
            {"a/../..", "climbs"},
            {"/inbound/../../other", "climbs"},
            {"/inbound/.\\./.\\./other", "backslash"},
            {"\\..\\other", "backslash"},
            {"/inbound/file\0.csv", "NUL"},
            {"~", "'~'"},
            {"~/inbound", "'~'"},
            {"~root", "'~'"},
        };
    }

    @Test(dataProvider = "refused")
    public void refusesAnythingThatCouldNameAPathOutside(String inView, String why) {
        assertThatThrownBy(() -> root.toServer(inView))
                .isInstanceOfSatisfying(SftpException.class,
                        e -> assertThat(e.isPermissionDenied()).isTrue())
                .hasMessageContaining(why)
                .describedAs("the message is in the view's terms; the server-side root stays private")
                .hasMessageNotContaining("/tenants/acme");
    }

    @Test
    public void rejectsRootsThatConfineNothingOrCannotBeHonoured() {
        assertThatThrownBy(() -> RootDirectory.of("/")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RootDirectory.of("/a/..")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RootDirectory.of("tenants/acme"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RootDirectory.of("/tenants\\acme"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(RootDirectory.of("/tenants//acme/./").path()).isEqualTo("/tenants/acme");
    }

    @Test
    public void mapsServerPathsBackAndRefusesThoseOutside() {
        assertThat(root.fromServer("/tenants/acme", null)).isEqualTo("/");
        assertThat(root.fromServer("/tenants/acme/inbound/a.csv", null)).isEqualTo("/inbound/a.csv");
        assertThat(root.normalise("inbound/../outbound/")).isEqualTo("/outbound");

        // A sibling whose name merely starts with the root's is outside, not inside.
        try (SftpClient server = SftpClients.blocking("inmemory", config())) {
            server.mkdirs("/tenants/acme");
            assertThatThrownBy(() -> root.fromServer("/tenants/acme-evil/x", server))
                    .isInstanceOfSatisfying(SftpException.class,
                            e -> assertThat(e.isPermissionDenied()).isTrue());
        }
    }

    static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
                .host("localhost")
                .username("test")
                .password("secret".toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
    }
}

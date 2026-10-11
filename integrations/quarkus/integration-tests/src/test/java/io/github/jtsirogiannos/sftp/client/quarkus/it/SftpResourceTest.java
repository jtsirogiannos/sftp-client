package io.github.jtsirogiannos.sftp.client.quarkus.it;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;

import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Drives the application over HTTP against a live SFTP server.
 *
 * <p>{@link SftpResourceIT} re-runs every one of these against the packaged artifact — the runner
 * jar normally, a native binary under {@code -Pnative} — which is why nothing here injects a bean or
 * touches the server directly. Everything goes through the wire.
 */
@QuarkusTest
@WithTestResource(EmbeddedSftpServerResource.class)
class SftpResourceTest {

    /**
     * The provider has to be found via {@code ServiceLoader}. In a native image that only works
     * because the extension registers a {@code ServiceProviderBuildItem} — without it the binary
     * builds cleanly and fails here with "No SFTP provider named 'mina'".
     */
    @Test
    void resolvesTheProviderAndReportsItsCapabilities() {
        given().when().get("/sftp/capabilities")
                .then().statusCode(200)
                .body(containsString("v6"))
                .body(containsString("pipelined=true"))
                .body(containsString("atomicCreateNew=true"));
    }

    @Test
    void roundTripsAFile() {
        given().body("hello from the integration test").contentType("text/plain")
                .when().post("/sftp/files/it.txt")
                .then().statusCode(200).body(equalTo("uploaded"));

        given().when().get("/sftp/files/it.txt")
                .then().statusCode(200).body(equalTo("hello from the integration test"));
    }

    @Test
    void listsUploadedFiles() {
        given().body("x").contentType("text/plain")
                .when().post("/sftp/files/listed.txt")
                .then().statusCode(200);

        given().when().get("/sftp/list")
                .then().statusCode(200).body(containsString("listed.txt"));
    }

    /** The Mutiny facade end to end: Uni, the neutral publisher underneath, and the pool. */
    @Test
    void servesTheReactiveClient() {
        given().body("y").contentType("text/plain")
                .when().post("/sftp/files/reactive.txt")
                .then().statusCode(200);

        given().when().get("/sftp/reactive/exists/reactive.txt")
                .then().statusCode(200).body(equalTo("true"));

        given().when().get("/sftp/reactive/exists/absent.txt")
                .then().statusCode(200).body(equalTo("false"));
    }

    /**
     * Tenancy and a declarative session over HTTP: each request's calls confined to the tenant its
     * header names, and made over one connection — the endpoint's only one.
     */
    @Test
    void confinesEachRequestToItsTenantInOneSession() {
        given().when().post("/tenant/onboard/acme").then().statusCode(200);
        given().when().post("/tenant/onboard/globex").then().statusCode(200);

        given().header("X-Tenant", "acme").body("acme's").contentType("text/plain")
                .when().post("/tenant/files/orders.csv")
                .then().statusCode(200).body(equalTo("session=true,exists=true"));

        given().header("X-Tenant", "acme").when().get("/tenant/files")
                .then().statusCode(200).body(containsString("/inbound/orders.csv"));
        given().header("X-Tenant", "globex").when().get("/tenant/files")
                .then().statusCode(200).body(not(containsString("orders.csv")));

        given().header("X-Tenant", "acme").when().get("/tenant/reactive/exists/orders.csv")
                .then().statusCode(200).body(equalTo("true"));
        given().header("X-Tenant", "globex").when().get("/tenant/reactive/exists/orders.csv")
                .then().statusCode(200).body(equalTo("false"));
    }
}

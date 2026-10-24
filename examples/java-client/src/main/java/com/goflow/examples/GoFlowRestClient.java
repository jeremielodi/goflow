package com.goflow.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * A small GoFlow REST client: logs in, keeps the JWT fresh, and sends JSON.
 *
 * GoFlow's access token lives 15 minutes and its refresh token 7 days, so a
 * long-running client must refresh: {@link #accessToken()} refreshes the
 * token a few minutes before it expires (and logs in again if the refresh
 * token is gone too), and a 401 triggers one refresh and a retry.
 *
 * Thread-safe: share one instance across a worker's threads.
 */
public class GoFlowRestClient {

    /** Refresh the access token this long before its 15-minute lifetime ends. */
    private static final Duration ACCESS_TOKEN_LIFETIME = Duration.ofMinutes(15);
    private static final Duration REFRESH_MARGIN = Duration.ofMinutes(3);

    private final String baseUrl;
    private final String email;
    private final String password;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final ObjectMapper json = new ObjectMapper();

    private String accessToken;
    private String refreshToken;
    private Instant accessTokenExpiry = Instant.EPOCH;

    public GoFlowRestClient(String baseUrl, String email, String password) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.email = email;
        this.password = password;
    }

    /** Reads GOFLOW_URL, GOFLOW_USER and GOFLOW_PASSWORD (defaults: a local dev server). */
    public static GoFlowRestClient fromEnv() {
        return new GoFlowRestClient(
                env("GOFLOW_URL", "http://127.0.0.1:8080"),
                env("GOFLOW_USER", "admin@goflow.com"),
                env("GOFLOW_PASSWORD", "admin123"));
    }

    static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    public ObjectMapper json() {
        return json;
    }

    // ── Authentication ──────────────────────────────────────────────────────

    /** A valid access token, refreshed or re-obtained when needed. */
    public synchronized String accessToken() {
        if (accessToken == null || Instant.now().isAfter(accessTokenExpiry.minus(REFRESH_MARGIN))) {
            if (refreshToken == null || !refresh()) {
                login();
            }
        }
        return accessToken;
    }

    private void login() {
        JsonNode res = send("POST", "/auth/login", Map.of("email", email, "password", password), false);
        storeTokens(res);
    }

    /** POST /auth/refresh; false when the refresh token is no longer accepted. */
    private boolean refresh() {
        try {
            storeTokens(send("POST", "/auth/refresh", Map.of("refresh_token", refreshToken), false));
            return true;
        } catch (GoFlowException e) {
            return false;
        }
    }

    private void storeTokens(JsonNode res) {
        accessToken = res.path("access_token").asText(null);
        refreshToken = res.path("refresh_token").asText(refreshToken);
        if (accessToken == null) {
            throw new GoFlowException(0, "login response has no access_token: " + res);
        }
        accessTokenExpiry = Instant.now().plus(ACCESS_TOKEN_LIFETIME);
    }

    private synchronized void forceRefresh() {
        accessTokenExpiry = Instant.EPOCH;
    }

    // ── JSON requests ───────────────────────────────────────────────────────

    public JsonNode get(String path) {
        return send("GET", path, null, true);
    }

    public JsonNode post(String path, Object body) {
        return send("POST", path, body, true);
    }

    public JsonNode put(String path, Object body) {
        return send("PUT", path, body, true);
    }

    public JsonNode delete(String path) {
        return send("DELETE", path, null, true);
    }

    /** Builds "?a=1&b=2" from pairs, URL-encoding the values. */
    public static String query(String... pairs) {
        StringJoiner q = new StringJoiner("&", "?", "");
        for (int i = 0; i + 1 < pairs.length; i += 2) {
            q.add(pairs[i] + "=" + URLEncoder.encode(pairs[i + 1], StandardCharsets.UTF_8));
        }
        return q.toString();
    }

    private JsonNode send(String method, String path, Object body, boolean authenticated) {
        HttpResponse<String> res = exchange(method, path, body, authenticated);
        if (res.statusCode() == 401 && authenticated) {
            forceRefresh();
            res = exchange(method, path, body, true);
        }
        if (res.statusCode() >= 400) {
            throw new GoFlowException(res.statusCode(), method + " " + path + " → " + res.statusCode() + ": " + res.body());
        }
        try {
            return res.body().isBlank() ? json.nullNode() : json.readTree(res.body());
        } catch (IOException e) {
            throw new GoFlowException(res.statusCode(), method + " " + path + " returned non-JSON: " + res.body());
        }
    }

    private HttpResponse<String> exchange(String method, String path, Object body, boolean authenticated) {
        try {
            HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Accept", "application/json");
            if (authenticated) {
                req.header("Authorization", "Bearer " + accessToken());
            }
            if (body != null) {
                req.header("Content-Type", "application/json")
                        .method(method, HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
            } else {
                req.method(method, HttpRequest.BodyPublishers.noBody());
            }
            return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new GoFlowException(0, method + " " + path + " failed: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GoFlowException(0, method + " " + path + " interrupted");
        }
    }

    // ── Deployment (multipart/form-data) ────────────────────────────────────

    /** POST /engine-rest/deployment/create with one BPMN file. */
    public JsonNode deploy(String fileName, byte[] bpmn, String deploymentName) {
        String boundary = "----goflow" + UUID.randomUUID();
        byte[] head = ("--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"deployment-name\"\r\n\r\n" + deploymentName + "\r\n"
                + "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"resources\"; filename=\"" + fileName + "\"\r\n"
                + "Content-Type: application/xml\r\n\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] tail = ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[head.length + bpmn.length + tail.length];
        System.arraycopy(head, 0, payload, 0, head.length);
        System.arraycopy(bpmn, 0, payload, head.length, bpmn.length);
        System.arraycopy(tail, 0, payload, head.length + bpmn.length, tail.length);

        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/engine-rest/deployment/create"))
                    .header("Authorization", "Bearer " + accessToken())
                    .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                    .build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() >= 400) {
                throw new GoFlowException(res.statusCode(), "deploy → " + res.statusCode() + ": " + res.body());
            }
            return json.readTree(res.body());
        } catch (IOException e) {
            throw new GoFlowException(0, "deploy failed: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GoFlowException(0, "deploy interrupted");
        }
    }

    /** An HTTP error from GoFlow (status 0: no response at all). */
    public static class GoFlowException extends RuntimeException {
        private final int status;

        public GoFlowException(int status, String message) {
            super(message);
            this.status = status;
        }

        public int status() {
            return status;
        }
    }
}

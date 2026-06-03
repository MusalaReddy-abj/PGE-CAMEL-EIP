package com.pge.krakencis.security;

import com.pge.krakencis.logging.StructuredLogger;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Generates and caches a static HS256 JWT Bearer token for outbound HTTP calls
 * routed through Kong gateway.
 *
 * <h3>Token structure</h3>
 * <pre>
 * Header:  { "alg": "HS256", "typ": "JWT" }
 * Payload: { "iss": "jwt-issuer", "exp": 2910000000 }
 * Signature: HMAC-SHA256(base64url(header) + "." + base64url(payload), secretKey)
 * </pre>
 *
 * <h3>Usage</h3>
 * <pre>
 * String authHeader = jwtTokenProvider.bearerHeader();
 * // → "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...."
 * </pre>
 *
 * <h3>Security note</h3>
 * The secret key is externalised to {@code application.yml} and should be
 * overridden via an environment variable ({@code KONG_JWT_SECRET}) or a
 * secrets manager in production. Never commit the raw key to source control.
 */
@Component
public class JwtTokenProvider {

    private static final StructuredLogger log = StructuredLogger.of(JwtTokenProvider.class);

    private static final String ALGORITHM = "HmacSHA256";

    @Value("${kong.jwt.secret:Kraken@2026!ExternalConsumer#JWT_Kong$Secure92xLpZ}")
    private String secretKey;

    @Value("${kong.jwt.issuer:jwt-issuer}")
    private String issuer;

    @Value("${kong.jwt.expiry:2910000000}")
    private long expiry;

    /** Cached token — generated once at startup (static expiry). */
    private String cachedToken;

    @PostConstruct
    void init() {
        cachedToken = buildToken();
        log.info("jwtTokenGenerated", null,
            "issuer",    issuer,
            "algorithm", "HS256",
            "expiry",    expiry);
    }

    /**
     * Returns the pre-built {@code Authorization} header value ready for use
     * in outbound HTTP requests:
     * <pre>Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9....</pre>
     */
    public String bearerHeader() {
        return "Bearer " + cachedToken;
    }

    /** Returns the raw JWT token string without the "Bearer " prefix. */
    public String token() {
        return cachedToken;
    }

    // ── JWT construction ──────────────────────────────────────────────────────

    private String buildToken() {
        String header  = base64Url("{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        String payload = base64Url(
            "{\"iss\":\"" + issuer + "\",\"exp\":" + expiry + "}");

        String signingInput = header + "." + payload;
        String signature    = sign(signingInput);

        return signingInput + "." + signature;
    }

    private String sign(String data) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(
                secretKey.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            byte[] raw = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to sign JWT token", e);
        }
    }

    private static String base64Url(String json) {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }
}

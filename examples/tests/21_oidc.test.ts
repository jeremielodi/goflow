/**
 * Suite 21 — OIDC Authentication (Camunda 8 Phase 8)
 *
 * GoFlow can accept bearer tokens from an external OIDC provider (Keycloak,
 * Auth0, etc.) as an alternative to its own JWT login — see
 * pkg/authentication/oidc.go (JWKS-based RS256 validation + auto-
 * provisioning) and pkg/middleware/oidc_user.go (ResolveOIDCUser).
 *
 * That validation logic (JWKS fetch/cache, issuer/audience checks, expiry,
 * tampered-signature rejection, unknown-kid rejection) is covered by Go
 * unit tests in pkg/authentication/oidc_test.go, which spin up a real RSA
 * keypair + a local JWKS server via httptest — no real IdP needed for that.
 *
 * This docker-compose stack does NOT set OIDC_ISSUER/OIDC_JWKS_URL, so
 * OIDC is disabled here (NewOIDCService returns nil) exactly like it would
 * be for any GoFlow deployment that hasn't configured an IdP. What this
 * suite verifies end-to-end against the running server is:
 *   A) With OIDC disabled, an RS256-signed bearer token (which is neither a
 *      valid internal JWT nor validatable OIDC, since there's no provider
 *      configured to check it against) is rejected with 401 — the fallback
 *      path added for OIDC doesn't change behavior when unconfigured.
 *   B) Regular JWT login/authentication is unaffected by the OIDC code path
 *      existing (regression check; also covered implicitly by every other
 *      suite, but asserted explicitly here since it's the property Phase 8
 *      must not break).
 *
 * Full E2E against a real IdP (token issued by an actual Keycloak/Auth0,
 * GoFlow configured with its real OIDC_ISSUER/OIDC_JWKS_URL/OIDC_AUDIENCE)
 * is a manual verification step, as noted in the roadmap plan itself —
 * standing up a dev IdP isn't practical inside this automated suite.
 */
import { GoFlowClient, runSuite, assert } from './client';

const client = new GoFlowClient();

// A structurally valid RS256 JWT (header.payload.signature, all base64url)
// that isn't signed by anything GoFlow trusts — with OIDC disabled there's
// no JWKS to even check it against, so it must be rejected the same way an
// internal-JWT-shaped garbage token always was.
const FOREIGN_RS256_TOKEN =
  'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImZvcmVpZ24ta2V5In0.' +
  'eyJzdWIiOiJzb21lb25lLWVsc2UiLCJpc3MiOiJodHRwczovL25vdC1nb2Zsb3cuZXhhbXBsZS5jb20iLCJleHAiOjk5OTk5OTk5OTl9.' +
  'ZmFrZS1zaWduYXR1cmUtdGhhdC1kb2VzLW5vdC12YWxpZGF0ZQ';

async function run() {
  await runSuite({
    name: '21 — OIDC Authentication',
    tests: [

      // ── A: foreign/unvalidatable bearer token is rejected ───────────────────
      {
        name: '[A] An RS256 bearer token with no matching provider configured is rejected (401)',
        async fn() {
          try {
            await client.api.get('/users/me', {
              headers: { Authorization: `Bearer ${FOREIGN_RS256_TOKEN}` },
            });
            assert(false, 'expected a 401 for an unrecognized bearer token');
          } catch (e: any) {
            assert(e.response?.status === 401, `expected 401, got ${e.response?.status}`);
          }
        },
      },

      // ── B: regular JWT auth still works with the OIDC fallback path present ──
      {
        name: '[B] Regular JWT login/auth is unaffected by the (disabled) OIDC code path',
        async fn() {
          await client.loginAsSuperUser();
          const res = await client.api.get('/users/me');
          assert(res.data.user?.email === 'admin@goflow.com', `expected admin@goflow.com, got ${res.data.user?.email}`);
        },
      },

    ],
  });
}

run().catch(console.error);

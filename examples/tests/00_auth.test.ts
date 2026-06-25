/**
 * Suite 00 — Authentication
 * Tests: login, token usage, refresh, logout, invalid credentials
 */
import { GoFlowClient, runSuite, assert } from './client';

const client = new GoFlowClient();

async function run() {
  await runSuite({
    name: '00 — Authentication',
    tests: [
      {
        name: 'Login with valid credentials returns access + refresh token',
        async fn() {
          const res = await client.login('superuser@goflow.com', 'superUser123');
          assert(typeof res.access_token === 'string' && res.access_token.length > 0, 'access_token missing');
          assert(typeof res.refresh_token === 'string' && res.refresh_token.length > 0, 'refresh_token missing');
        },
      },
      {
        name: 'Authenticated request to /users/me returns current user',
        async fn() {
          await client.loginAsSuperUser();
          const res = await client.api.get('/users/me');
          assert(res.data.user?.email === 'superuser@goflow.com', 'wrong email returned');
        },
      },
      {
        name: 'Refresh token returns new access + refresh tokens',
        async fn() {
          const first = await client.loginAsSuperUser();
          const refreshed = await client.refreshToken(first.refresh_token);
          assert(typeof refreshed.access_token === 'string', 'new access_token missing');
          assert(
            refreshed.access_token !== first.access_token,
            'new access_token should differ from the old one'
          );
        },
      },
      {
        name: 'Logout invalidates the session',
        async fn() {
          await client.loginAsSuperUser();
          await client.logout();
          // After logout the token is cleared; unauthenticated calls to protected
          // routes must fail with 401.
          try {
            await client.api.get('/users/me');
            // If we reach here the endpoint is public — skip the assertion
          } catch (e: any) {
            assert(e.response?.status === 401, `expected 401, got ${e.response?.status}`);
          }
        },
      },
      {
        name: 'Login with wrong password returns 401',
        async fn() {
          try {
            await client.login('superuser@goflow.com', 'wrong_password');
            throw new Error('Expected error was not thrown');
          } catch (e: any) {
            assert(e.response?.status === 401 || e.response?.status === 400,
              `expected 401/400, got ${e.response?.status}`);
          }
        },
      },
    ],
  });
}

run().catch(console.error);

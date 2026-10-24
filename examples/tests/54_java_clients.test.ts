/**
 * Suite 54 — Java clients (examples/java-client, see docs/java-client.md)
 *
 * Runs the Java examples against the GoFlow under test, through the Maven
 * Wrapper (it downloads Maven on first use; a JDK 17+ must be installed).
 * Each example drives one order through java-order.bpmn, checks every step
 * and prints "<Name> OK"; any failed check exits non-zero.
 *
 * Tests:
 *   A) The project compiles (downloads the client libraries on first run).
 *   B) RestExample: plain REST (java.net.http + Jackson).
 *   C) ExternalTaskWorkerExample with HTTP Basic: Camunda 7 External Task Client.
 *   D) ExternalTaskWorkerExample with a JWT.
 *   E) ZeebeClientExample: Camunda 8 Java client over gRPC.
 *   F) Camunda7OpenApiExample: the Camunda 7 REST client generated from the OpenAPI spec.
 *   G) examples/java-client-spring: a Camunda 7 application on the engine's Java API
 *      (RuntimeService, TaskService…), through the REST client Spring Boot starter.
 *
 * Without a JDK the suite fails rather than skipping, so a missing Java
 * never passes for a working one.
 */
import * as path from 'path';
import { spawn } from 'child_process';
import { BASE_URL, runSuite, assert } from './client';

const PROJECT = path.join(__dirname, '..', 'java-client');
const SPRING_PROJECT = path.join(__dirname, '..', 'java-client-spring');
const GRPC = process.env.GOFLOW_GRPC ?? `${new URL(BASE_URL).hostname}:26500`;

function mvnw(args: string[], env: Record<string, string> = {}, timeoutMs = 300_000, project = PROJECT): Promise<{ code: number; out: string }> {
  const windows = process.platform === 'win32';
  return new Promise(resolve => {
    // cmd.exe may not look in the working directory: give the wrapper's full path.
    const wrapper = windows ? `"${path.join(project, 'mvnw.cmd')}"` : './mvnw';
    const child = spawn(wrapper, ['-q', '-B', ...args], {
      cwd: project,
      shell: windows,
      env: { ...process.env, GOFLOW_URL: BASE_URL, GOFLOW_GRPC: GRPC, ...env },
    });
    let out = '';
    child.stdout.on('data', d => (out += d));
    child.stderr.on('data', d => (out += d));
    const timer = setTimeout(() => child.kill(), timeoutMs);
    child.on('close', code => {
      clearTimeout(timer);
      resolve({ code: code ?? 1, out });
    });
  });
}

async function runExample(mainClass: string, env: Record<string, string> = {}) {
  const { code, out } = await mvnw(['exec:java', `-Dexec.mainClass=com.goflow.examples.${mainClass}`], env);
  const tail = out.trim().split('\n').slice(-15).join('\n');
  assert(code === 0 && out.includes(`${mainClass} OK`), `${mainClass} failed (exit ${code}):\n${tail}`);
}

async function run() {
  await runSuite({
    name: '54 — Java clients (REST, Camunda 7 External Task Client, Camunda 8 gRPC)',
    tests: [
      {
        name: '[A] examples/java-client compiles',
        async fn() {
          const { code, out } = await mvnw(['compile'], {}, 600_000);
          assert(code === 0, `compilation failed (is a JDK 17+ installed?):\n${out.trim().split('\n').slice(-15).join('\n')}`);
        },
      },
      {
        name: '[B] RestExample: plain REST end to end',
        fn: () => runExample('RestExample'),
      },
      {
        name: '[C] ExternalTaskWorkerExample: Camunda 7 client, HTTP Basic',
        fn: () => runExample('ExternalTaskWorkerExample', { GOFLOW_WORKER_AUTH: 'basic' }),
      },
      {
        name: '[D] ExternalTaskWorkerExample: Camunda 7 client, JWT',
        fn: () => runExample('ExternalTaskWorkerExample', { GOFLOW_WORKER_AUTH: 'jwt' }),
      },
      {
        name: '[E] ZeebeClientExample: Camunda 8 Java client over gRPC',
        fn: () => runExample('ZeebeClientExample'),
      },
      {
        name: '[F] Camunda7OpenApiExample: Camunda 7 OpenAPI REST client',
        fn: () => runExample('Camunda7OpenApiExample'),
      },
      {
        name: '[G] Camunda 7 Java API (RuntimeService, TaskService…) through the Spring Boot starter',
        async fn() {
          const { code, out } = await mvnw(['spring-boot:run'], {}, 600_000, SPRING_PROJECT);
          const tail = out.trim().split('\n').slice(-15).join('\n');
          assert(code === 0 && out.includes('Camunda7JavaApiExample OK'), `Spring example failed (exit ${code}):\n${tail}`);
        },
      },
    ],
  });
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});

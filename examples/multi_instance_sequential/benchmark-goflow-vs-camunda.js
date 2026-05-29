// benchmark-goflow-vs-camunda.js (fixed worker initialization)
// Run: node benchmark-goflow-vs-camunda.js

const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');

// ============================================================
// CONFIGURATION
// ============================================================

const CONFIG = {
    // GoFlow Configuration
    goflow: {
        name: 'GoFlow',
        baseUrl: 'http://localhost:8080',
        processKey: 'SequentialMultiInstanceProcess',
        bpmnFile: './SequentialMultiInstance.bpmn',
        asyncEndpoint: '/async/process-definitions/SequentialMultiInstanceProcess/start',
        syncEndpoint: '/engine-rest/v2/process-definitions/SequentialMultiInstanceProcess/start',
        deploymentEndpoint: '/engine-rest/v2/deployment/create'
    },
    
    // Camunda 7 Configuration
    camunda: {
        name: 'Camunda 7',
        baseUrl: 'http://localhost:8182',
        auth: {
            username: 'demo',
            password: 'demo'
        },
        processKey: 'BenchmarkProcess',
        topicName: 'benchmark_task',
        deploymentEndpoint: '/engine-rest/deployment/create',
        startEndpoint: '/engine-rest/process-definition/key/BenchmarkProcess/start',
        externalTaskBaseUrl: 'http://localhost:8182/engine-rest'
    },
    
    // Test parameters
    warmupRuns: 3,
    testRuns: 10,
    concurrentLevels: [1, 5, 10, 20, 50],
    sustainedDuration: 10000,
    sustainedConcurrency: 10,
    requestTimeout: 30000,
    delayBetweenTests: 2000,
    quietMode: true
};

// ============================================================
// CAMUNDA 7 COMPATIBLE BPMN
// ============================================================

const CAMUNDA_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" 
                   xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" 
                   xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" 
                   xmlns:di="http://www.omg.org/spec/DD/20100524/DI" 
                   xmlns:camunda="http://camunda.org/schema/1.0/bpmn" 
                   targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="BenchmarkProcess" name="Benchmark Process" isExecutable="true" camunda:historyTimeToLive="30">
    <bpmn:startEvent id="StartEvent_1" name="Start">
      <bpmn:outgoing>Flow_1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="Flow_1" sourceRef="StartEvent_1" targetRef="ServiceTask_1" />
    <bpmn:serviceTask id="ServiceTask_1" name="Process Task" camunda:type="external" camunda:topic="benchmark_task">
      <bpmn:incoming>Flow_1</bpmn:incoming>
      <bpmn:outgoing>Flow_2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="Flow_2" sourceRef="ServiceTask_1" targetRef="EndEvent_1" />
    <bpmn:endEvent id="EndEvent_1" name="End">
      <bpmn:incoming>Flow_2</bpmn:incoming>
    </bpmn:endEvent>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="BenchmarkProcess">
      <bpmndi:BPMNShape id="StartEvent_1_di" bpmnElement="StartEvent_1">
        <dc:Bounds x="100" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="ServiceTask_1_di" bpmnElement="ServiceTask_1">
        <dc:Bounds x="200" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="EndEvent_1_di" bpmnElement="EndEvent_1">
        <dc:Bounds x="350" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_1_di" bpmnElement="Flow_1">
        <di:waypoint x="136" y="118" />
        <di:waypoint x="200" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_2_di" bpmnElement="Flow_2">
        <di:waypoint x="300" y="120" />
        <di:waypoint x="350" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

// ============================================================
// AXIOS INSTANCES
// ============================================================

const goflowApi = axios.create({
    baseURL: CONFIG.goflow.baseUrl,
    timeout: CONFIG.requestTimeout,
    headers: { 'Content-Type': 'application/json' }
});

const camundaApi = axios.create({
    baseURL: CONFIG.camunda.baseUrl,
    timeout: CONFIG.requestTimeout,
    headers: { 'Content-Type': 'application/json' },
    auth: CONFIG.camunda.auth
});

// ============================================================
// UTILITIES
// ============================================================

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

let camundaWorker = null;
let camundaWorkerStarted = false;
let originalConsoleLog = console.log;
let workerLogsSuppressed = false;

const log = {
    section: (title) => {
        console.log('\n' + '='.repeat(80));
        console.log(` ${title}`);
        console.log('='.repeat(80));
    },
    info: (msg) => console.log(`📌 ${msg}`),
    success: (msg) => console.log(`✅ ${msg}`),
    error: (msg) => console.log(`❌ ${msg}`),
    warning: (msg) => console.log(`⚠️ ${msg}`),
    result: (label, value, unit = '') => console.log(`   ${label}: ${value}${unit}`)
};

function suppressWorkerLogs() {
    if (CONFIG.quietMode && !workerLogsSuppressed) {
        console.log = function(...args) {
            const msg = args.join('');
            if (!msg.includes('✓') && !msg.includes('completed') && !msg.includes('polling')) {
                originalConsoleLog.apply(console, args);
            }
        };
        workerLogsSuppressed = true;
    }
}

function restoreConsole() {
    if (workerLogsSuppressed) {
        console.log = originalConsoleLog;
        workerLogsSuppressed = false;
    }
}

// ============================================================
// SIMPLE CAMUNDA WORKER (no external library)
// ============================================================

let workerRunning = false;
let workerStop = false;

async function startSimpleCamundaWorker() {
    if (workerRunning) return;
    
    log.info('Starting Camunda 7 external task worker...');
    suppressWorkerLogs();
    
    workerRunning = true;
    workerStop = false;
    
    const pollTasks = async () => {
        while (!workerStop) {
            try {
                const response = await camundaApi.post('/engine-rest/external-task/fetchAndLock', {
                    workerId: 'benchmark-worker',
                    maxTasks: 10,
                    topics: [{
                        topicName: CONFIG.camunda.topicName,
                        lockDuration: 10000
                    }]
                });
                
                const tasks = response.data || [];
                for (const task of tasks) {
                    await camundaApi.post(`/engine-rest/external-task/${task.id}/complete`, {
                        workerId: 'benchmark-worker'
                    });
                }
            } catch (error) {
                // Silently ignore
            }
            await sleep(500);
        }
    };
    
    pollTasks();
    log.success('Camunda 7 worker started');
    await sleep(1000);
}

async function stopSimpleCamundaWorker() {
    if (workerRunning) {
        log.info('Stopping Camunda 7 worker...');
        workerStop = true;
        workerRunning = false;
        await sleep(1000);
        restoreConsole();
        log.success('Camunda 7 worker stopped');
    }
}

// ============================================================
// DEPLOYMENT
// ============================================================

async function deployToGoflow() {
    log.info('Deploying to GoFlow...');
    
    if (!fs.existsSync(CONFIG.goflow.bpmnFile)) {
        throw new Error(`BPMN file not found: ${CONFIG.goflow.bpmnFile}`);
    }

    const form = new FormData();
    form.append('resources', fs.createReadStream(CONFIG.goflow.bpmnFile));
    form.append('deployment-name', 'Benchmark Test');
    form.append('deployment-source', 'benchmark');

    try {
        const response = await goflowApi.post(CONFIG.goflow.deploymentEndpoint, form, {
            headers: { ...form.getHeaders() }
        });
        log.success(`GoFlow: ${response.data.name} (ID: ${response.data.id})`);
        return true;
    } catch (error) {
        log.error(`GoFlow deployment failed: ${error.message}`);
        return false;
    }
}

async function deployToCamunda() {
    log.info('Deploying to Camunda 7...');
    
    const camundaBpmnPath = './CamundaSimpleProcess.bpmn';
    fs.writeFileSync(camundaBpmnPath, CAMUNDA_BPMN);

    const form = new FormData();
    form.append('deployment-name', 'Benchmark Test');
    form.append('resources', fs.createReadStream(camundaBpmnPath));

    try {
        const response = await camundaApi.post(CONFIG.camunda.deploymentEndpoint, form, {
            headers: { ...form.getHeaders() },
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        });
        log.success(`Camunda 7: ${response.data.name} (ID: ${response.data.id})`);
        await sleep(2000);
        return true;
    } catch (error) {
        log.error(`Camunda 7 deployment failed: ${error.message}`);
        return false;
    }
}

// ============================================================
// CAMUNDA START REQUEST
// ============================================================

async function testCamundaStart(variables = {}) {
    const start = Date.now();
    try {
        const requestBody = {
            variables: {}
        };
        
        for (const [key, value] of Object.entries(variables)) {
            requestBody.variables[key] = {
                value: value,
                type: typeof value === 'boolean' ? 'Boolean' : 
                       typeof value === 'number' ? 'Double' : 'String'
            };
        }
        
        await camundaApi.post(CONFIG.camunda.startEndpoint, requestBody);
        return { success: true, duration: Date.now() - start };
    } catch (error) {
        return { success: false, duration: Date.now() - start, error: error.message };
    }
}

// ============================================================
// TEST FUNCTIONS
// ============================================================

async function testSingleRequest(api, endpoint, variables = {}, isCamunda = false) {
    const start = Date.now();
    try {
        if (isCamunda) {
            const result = await testCamundaStart(variables);
            return { success: result.success, duration: result.duration };
        } else {
            await api.post(endpoint, { variables });
            return { success: true, duration: Date.now() - start };
        }
    } catch {
        return { success: false, duration: Date.now() - start };
    }
}

async function runLatencyTest(api, endpoint, name, isCamunda = false) {
    log.info(`${name}: Running ${CONFIG.testRuns} requests...`);
    
    for (let i = 0; i < CONFIG.warmupRuns; i++) {
        await testSingleRequest(api, endpoint, { testId: `warmup-${i}` }, isCamunda);
    }
    
    const results = [];
    for (let i = 0; i < CONFIG.testRuns; i++) {
        const result = await testSingleRequest(api, endpoint, { 
            testId: `latency-${i}-${Date.now()}`
        }, isCamunda);
        if (result.success) {
            results.push(result.duration);
        }
    }
    
    if (results.length === 0) {
        log.warning(`${name}: No successful requests`);
        return null;
    }
    
    results.sort((a, b) => a - b);
    return {
        avg: results.reduce((a, b) => a + b, 0) / results.length,
        p95: results[Math.floor(results.length * 0.95)] || 0,
        p99: results[Math.floor(results.length * 0.99)] || 0
    };
}

async function runConcurrentTest(api, endpoint, concurrency, name, isCamunda = false) {
    const start = Date.now();
    const promises = [];
    const results = { success: 0, failed: 0, durations: [] };
    
    for (let i = 0; i < concurrency; i++) {
        const reqStart = Date.now();
        promises.push(
            (async () => {
                if (isCamunda) {
                    const result = await testCamundaStart({ testId: `concurrent-${concurrency}-${i}` });
                    if (result.success) {
                        results.success++;
                        results.durations.push(Date.now() - reqStart);
                    } else {
                        results.failed++;
                    }
                } else {
                    try {
                        await api.post(endpoint, { variables: { testId: `concurrent-${concurrency}-${i}` } });
                        results.success++;
                        results.durations.push(Date.now() - reqStart);
                    } catch {
                        results.failed++;
                    }
                }
            })()
        );
    }
    
    await Promise.all(promises);
    const totalTime = Date.now() - start;
    const totalRequests = results.success + results.failed;
    
    if (results.durations.length === 0) {
        return { concurrency, requestsPerSecond: 0, successRate: 0, avgResponse: 0 };
    }
    
    results.durations.sort((a, b) => a - b);
    
    return {
        concurrency,
        requestsPerSecond: (totalRequests / (totalTime / 1000)).toFixed(2),
        successRate: ((results.success / totalRequests) * 100).toFixed(2),
        avgResponse: (results.durations.reduce((a, b) => a + b, 0) / results.durations.length).toFixed(2)
    };
}

async function runSustainedLoadTest(api, endpoint, duration, concurrency, name, isCamunda = false) {
    log.info(`${name}: Running sustained load for ${duration/1000}s with ${concurrency} concurrent...`);
    
    const startTime = Date.now();
    const results = { success: 0, failed: 0, durations: [] };
    let running = true;
    
    const makeRequest = async () => {
        if (!running) return;
        const reqStart = Date.now();
        
        if (isCamunda) {
            const result = await testCamundaStart({ testId: `sustained-${Date.now()}` });
            if (result.success) {
                results.success++;
                results.durations.push(Date.now() - reqStart);
            } else {
                results.failed++;
            }
        } else {
            try {
                await api.post(endpoint, { variables: { testId: `sustained-${Date.now()}` } });
                results.success++;
                results.durations.push(Date.now() - reqStart);
            } catch {
                results.failed++;
            }
        }
        
        if (running) {
            setImmediate(makeRequest);
        }
    };
    
    for (let i = 0; i < concurrency; i++) {
        makeRequest();
    }
    
    await sleep(duration);
    running = false;
    await sleep(2000);
    
    const totalTime = Date.now() - startTime;
    const totalRequests = results.success + results.failed;
    
    if (results.durations.length === 0) {
        return { requestsPerSecond: 0, successRate: 0, avgResponse: 0 };
    }
    
    results.durations.sort((a, b) => a - b);
    
    return {
        requestsPerSecond: (totalRequests / (totalTime / 1000)).toFixed(2),
        successRate: ((results.success / totalRequests) * 100).toFixed(2),
        avgResponse: (results.durations.reduce((a, b) => a + b, 0) / results.durations.length).toFixed(2)
    };
}

// ============================================================
// HEALTH CHECKS
// ============================================================

async function checkGoFlow() {
    try {
        await goflowApi.get('/');
        log.success('GoFlow detected on port 8080');
        return true;
    } catch {
        log.error('GoFlow not detected on port 8080');
        return false;
    }
}

async function checkCamunda() {
    try {
        await camundaApi.get('/engine-rest/process-definition');
        log.success('Camunda 7 detected on port 8182');
        return true;
    } catch {
        log.error('Camunda 7 not detected on port 8182');
        return false;
    }
}

// ============================================================
// MAIN BENCHMARK
// ============================================================

async function runBenchmark() {
    log.section('🚀 GOFLOW vs CAMUNDA 7 BENCHMARK');
    log.info(`Test Runs: ${CONFIG.testRuns} per test`);
    log.info(`Concurrency Levels: ${CONFIG.concurrentLevels.join(', ')}`);
    log.info(`Sustained Load: ${CONFIG.sustainedDuration/1000}s at ${CONFIG.sustainedConcurrency} concurrent`);
    
    const goflowAvailable = await checkGoFlow();
    const camundaAvailable = await checkCamunda();
    
    if (!goflowAvailable && !camundaAvailable) {
        log.error('No engines available!');
        return;
    }
    
    if (goflowAvailable) await deployToGoflow();
    if (camundaAvailable) {
        await deployToCamunda();
        await startSimpleCamundaWorker();
        await sleep(3000);
    }
    
    const results = {
        goflow: { sync: {}, async: {}, sustained: {} },
        camunda: { sync: {} }
    };
    
    // Latency Tests
    log.section('📊 SINGLE REQUEST LATENCY (lower is better)');
    
    if (goflowAvailable) {
        const syncResult = await runLatencyTest(goflowApi, CONFIG.goflow.syncEndpoint, 'GoFlow Sync', false);
        if (syncResult) {
            results.goflow.sync = syncResult;
            log.result('GoFlow Sync', `${syncResult.avg.toFixed(2)}ms avg (p95: ${syncResult.p95}ms)`);
        }
        
        const asyncResult = await runLatencyTest(goflowApi, CONFIG.goflow.asyncEndpoint, 'GoFlow Async', false);
        if (asyncResult) {
            results.goflow.async = asyncResult;
            log.result('GoFlow Async', `${asyncResult.avg.toFixed(2)}ms avg (p95: ${asyncResult.p95}ms)`);
        }
    }
    
    if (camundaAvailable) {
        const camundaResult = await runLatencyTest(null, null, 'Camunda 7', true);
        if (camundaResult) {
            results.camunda.sync = camundaResult;
            log.result('Camunda 7', `${camundaResult.avg.toFixed(2)}ms avg (p95: ${camundaResult.p95}ms)`);
        }
    }
    
    // Concurrent Tests
    log.section('📊 CONCURRENT REQUESTS (higher req/s is better)');
    console.log('\nConcurrency | GoFlow Sync | GoFlow Async | Camunda 7');
    console.log('─'.repeat(65));
    
    for (const concurrency of CONFIG.concurrentLevels) {
        const row = [concurrency.toString().padEnd(11)];
        
        if (goflowAvailable) {
            const syncResult = await runConcurrentTest(goflowApi, CONFIG.goflow.syncEndpoint, concurrency, 'GoFlow Sync', false);
            results.goflow.sync[`c${concurrency}`] = syncResult;
            row.push(`${syncResult.requestsPerSecond} req/s`.padEnd(12));
            
            const asyncResult = await runConcurrentTest(goflowApi, CONFIG.goflow.asyncEndpoint, concurrency, 'GoFlow Async', false);
            results.goflow.async[`c${concurrency}`] = asyncResult;
            row.push(`${asyncResult.requestsPerSecond} req/s`.padEnd(12));
        } else {
            row.push('N/A'.padEnd(12));
            row.push('N/A'.padEnd(12));
        }
        
        if (camundaAvailable) {
            const camundaResult = await runConcurrentTest(null, null, concurrency, 'Camunda 7', true);
            results.camunda.sync[`c${concurrency}`] = camundaResult;
            row.push(`${camundaResult.requestsPerSecond} req/s`);
        } else {
            row.push('N/A');
        }
        
        console.log(row.join(' | '));
        await sleep(CONFIG.delayBetweenTests);
    }
    
    // Sustained Load Tests
    log.section(`📊 SUSTAINED LOAD (${CONFIG.sustainedDuration/1000}s at ${CONFIG.sustainedConcurrency} concurrent)`);
    
    if (goflowAvailable) {
        log.info('GoFlow Sync:');
        const syncSustained = await runSustainedLoadTest(goflowApi, CONFIG.goflow.syncEndpoint, CONFIG.sustainedDuration, CONFIG.sustainedConcurrency, 'GoFlow Sync', false);
        results.goflow.sustained.sync = syncSustained;
        log.result('  Throughput', `${syncSustained.requestsPerSecond} req/s`);
        log.result('  Success Rate', `${syncSustained.successRate}%`);
        log.result('  Avg Response', `${syncSustained.avgResponse}ms`);
        
        log.info('GoFlow Async:');
        const asyncSustained = await runSustainedLoadTest(goflowApi, CONFIG.goflow.asyncEndpoint, CONFIG.sustainedDuration, CONFIG.sustainedConcurrency, 'GoFlow Async', false);
        results.goflow.sustained.async = asyncSustained;
        log.result('  Throughput', `${asyncSustained.requestsPerSecond} req/s`);
        log.result('  Success Rate', `${asyncSustained.successRate}%`);
        log.result('  Avg Response', `${asyncSustained.avgResponse}ms`);
    }
    
    if (camundaAvailable) {
        log.info('Camunda 7:');
        const camundaSustained = await runSustainedLoadTest(null, null, CONFIG.sustainedDuration, CONFIG.sustainedConcurrency, 'Camunda 7', true);
        results.camunda.sustained = camundaSustained;
        log.result('  Throughput', `${camundaSustained.requestsPerSecond} req/s`);
        log.result('  Success Rate', `${camundaSustained.successRate}%`);
        log.result('  Avg Response', `${camundaSustained.avgResponse}ms`);
    }
    
    if (camundaAvailable) {
        await stopSimpleCamundaWorker();
    }
    
    // Winner Analysis
    log.section('🏆 WINNER ANALYSIS');
    
    if (goflowAvailable && results.camunda.sync && results.camunda.sync.avg) {
        const goflowLatency = results.goflow.async.avg || Infinity;
        const camundaLatency = results.camunda.sync.avg;
        
        if (goflowLatency < camundaLatency) {
            const improvement = ((camundaLatency - goflowLatency) / camundaLatency * 100).toFixed(1);
            log.success(`GoFlow Async is ${improvement}% FASTER for single requests!`);
        } else if (camundaLatency < goflowLatency) {
            const improvement = ((goflowLatency - camundaLatency) / goflowLatency * 100).toFixed(1);
            log.success(`Camunda 7 is ${improvement}% FASTER for single requests!`);
        }
        
        const goflowThroughput = parseFloat(results.goflow.async.c50?.requestsPerSecond) || 0;
        const camundaThroughput = parseFloat(results.camunda.sync.c50?.requestsPerSecond) || 0;
        
        if (goflowThroughput > camundaThroughput && goflowThroughput > 0) {
            const improvement = ((goflowThroughput - camundaThroughput) / camundaThroughput * 100).toFixed(1);
            log.success(`GoFlow Async handles ${improvement}% MORE REQUESTS per second at 50 concurrency!`);
        } else if (camundaThroughput > goflowThroughput && camundaThroughput > 0) {
            const improvement = ((camundaThroughput - goflowThroughput) / goflowThroughput * 100).toFixed(1);
            log.success(`Camunda 7 handles ${improvement}% MORE REQUESTS per second at 50 concurrency!`);
        }
    } else if (goflowAvailable) {
        log.success('GoFlow benchmark completed successfully!');
    }
    
    // Summary Table
    log.section('📊 SUMMARY');
    console.log('\n┌──────────────┬─────────────┬─────────────┬─────────────┐');
    console.log('│    Metric    │ GoFlow Sync │ GoFlow Async │ Camunda 7  │');
    console.log('├──────────────┼─────────────┼─────────────┼─────────────┤');
    
    const syncLatency = results.goflow.sync.avg ? `${results.goflow.sync.avg.toFixed(1)}ms` : 'N/A';
    const asyncLatency = results.goflow.async.avg ? `${results.goflow.async.avg.toFixed(1)}ms` : 'N/A';
    const camundaLatency = results.camunda.sync.avg ? `${results.camunda.sync.avg.toFixed(1)}ms` : 'N/A';
    console.log(`│ Latency (avg) │ ${syncLatency.padEnd(11)} │ ${asyncLatency.padEnd(11)} │ ${camundaLatency.padEnd(11)} │`);
    
    const syncThroughput = results.goflow.sync.c50?.requestsPerSecond || 'N/A';
    const asyncThroughput = results.goflow.async.c50?.requestsPerSecond || 'N/A';
    const camundaThroughput = results.camunda.sync.c50?.requestsPerSecond || 'N/A';
    console.log(`│ Throughput    │ ${syncThroughput.toString().padEnd(11)} │ ${asyncThroughput.toString().padEnd(11)} │ ${camundaThroughput.toString().padEnd(11)} │`);
    
    const syncSuccess = results.goflow.sustained.sync?.successRate || 'N/A';
    const asyncSuccess = results.goflow.sustained.async?.successRate || 'N/A';
    const camundaSuccess = results.camunda.sustained?.successRate || 'N/A';
    console.log(`│ Success Rate  │ ${syncSuccess.toString().padEnd(11)} │ ${asyncSuccess.toString().padEnd(11)} │ ${camundaSuccess.toString().padEnd(11)} │`);
    
    console.log('└──────────────┴─────────────┴─────────────┴─────────────┘');
    
    log.section('🎉 BENCHMARK COMPLETE');
}

// ============================================================
// RUN
// ============================================================

async function main() {
    try {
        await runBenchmark();
    } catch (error) {
        console.error('Benchmark error:', error.message);
    }
}

main();
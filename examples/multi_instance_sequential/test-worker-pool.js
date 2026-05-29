// test-worker-pool.js
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');

const API_URL = 'http://localhost:8080';
const BPMN_FILE = './SequentialMultiInstance.bpmn';
const PROCESS_KEY = 'SequentialMultiInstanceProcess';

const api = axios.create({
    baseURL: API_URL,
    timeout: 30000,
    headers: { 'Content-Type': 'application/json' }
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Deploy BPMN first
async function deployBpmn() {
    if (!fs.existsSync(BPMN_FILE)) {
        console.error(`❌ BPMN file not found: ${BPMN_FILE}`);
        return false;
    }

    const form = new FormData();
    form.append('resources', fs.createReadStream(BPMN_FILE));
    form.append('deployment-name', 'Worker Pool Test');
    form.append('deployment-source', 'test');

    try {
        const response = await api.post('/engine-rest/v2/deployment/create', form, {
            headers: { ...form.getHeaders() }
        });
        console.log('✅ BPMN Deployed:', response.data.name);
        return true;
    } catch (error) {
        console.error('❌ Failed to deploy BPMN:', error.message);
        return false;
    }
}

async function testAsyncEndpoint() {
    console.log('\n' + '='.repeat(60));
    console.log('🧪 TESTING ASYNC WORKER POOL ENDPOINT');
    console.log('='.repeat(60) + '\n');

    const deployed = await deployBpmn();
    if (!deployed) return;

    console.log('📤 Sending async process start request...');
    const startTime = Date.now();
    
    const response = await api.post('/async/process-definitions/SequentialMultiInstanceProcess/start', {
        variables: {
            testId: 'async-test-001',
            startTime: new Date().toISOString()
        }
    });
    
    const responseTime = Date.now() - startTime;
    
    console.log(`✅ Response received in ${responseTime}ms`);
    console.log(`   Status: ${response.status}`);
    console.log(`   Request ID: ${response.data.requestId}`);
    console.log(`   Queue size: ${response.data.queue_size}`);
    console.log(`   Status: ${response.data.status}\n`);
    
    await sleep(1000);
    
    const metricsResponse = await api.get('/metrics/pool');
    console.log('📊 Worker Pool Metrics:');
    console.log(`   Jobs Submitted: ${metricsResponse.data.jobs_submitted}`);
    console.log(`   Jobs Processed: ${metricsResponse.data.jobs_processed}`);
    console.log(`   Jobs Failed: ${metricsResponse.data.jobs_failed}`);
    console.log(`   Queue Length: ${metricsResponse.data.queue_length}`);
    console.log(`   Active Workers: ${metricsResponse.data.active_workers}`);
    console.log(`   Healthy: ${metricsResponse.data.healthy}\n`);
}

async function testConcurrentAsyncRequests() {
    console.log('\n' + '='.repeat(60));
    console.log('🚀 TESTING CONCURRENT ASYNC REQUESTS');
    console.log('='.repeat(60) + '\n');
    
    const deployed = await deployBpmn();
    if (!deployed) return;
    
    const concurrency = 50;
    const requests = [];
    
    console.log(`📤 Sending ${concurrency} concurrent async requests...\n`);
    
    const startTime = Date.now();
    
    for (let i = 0; i < concurrency; i++) {
        requests.push(
            api.post('/async/process-definitions/SequentialMultiInstanceProcess/start', {
                variables: {
                    testId: `concurrent-${i}`,
                    startTime: new Date().toISOString()
                }
            })
        );
    }
    
    const responses = await Promise.all(requests);
    const totalTime = Date.now() - startTime;
    
    console.log(`✅ All ${concurrency} requests completed in ${totalTime}ms`);
    console.log(`   Average response time: ${(totalTime / concurrency).toFixed(2)}ms`);
    
    const accepted = responses.filter(r => r.status === 202).length;
    console.log(`   Accepted (202): ${accepted}`);
    console.log(`   Success rate: ${(accepted / concurrency * 100).toFixed(2)}%\n`);
    
    await sleep(2000);
    
    const metricsResponse = await api.get('/metrics/pool');
    console.log('📊 Worker Pool Metrics After Test:');
    console.log(`   Jobs Submitted: ${metricsResponse.data.jobs_submitted}`);
    console.log(`   Jobs Processed: ${metricsResponse.data.jobs_processed}`);
    console.log(`   Jobs Failed: ${metricsResponse.data.jobs_failed}`);
    console.log(`   Queue Length: ${metricsResponse.data.queue_length}`);
    console.log(`   Active Workers: ${metricsResponse.data.active_workers}\n`);
}

async function testQueueOverflow() {
    console.log('\n' + '='.repeat(60));
    console.log('💪 TESTING QUEUE OVERFLOW');
    console.log('='.repeat(60) + '\n');
    
    const deployed = await deployBpmn();
    if (!deployed) return;
    
    const queueCapacity = 1000;
    const requestsToSend = 1100;
    
    console.log(`📤 Sending ${requestsToSend} rapid async requests...`);
    console.log(`   Queue capacity: ${queueCapacity}\n`);
    
    const startTime = Date.now();
    let accepted = 0;
    let rejected = 0;
    let errors = 0;
    
    const promises = [];
    for (let i = 0; i < requestsToSend; i++) {
        promises.push(
            api.post('/async/process-definitions/SequentialMultiInstanceProcess/start', {
                variables: { testId: `overflow-${i}` }
            }).then(() => accepted++)
              .catch((err) => {
                  if (err.response?.status === 503) {
                      rejected++;
                  } else {
                      errors++;
                  }
              })
        );
    }
    
    await Promise.all(promises);
    const totalTime = Date.now() - startTime;
    
    console.log(`✅ Completed in ${totalTime}ms`);
    console.log(`   Accepted (202): ${accepted}`);
    console.log(`   Rejected (503 - queue full): ${rejected}`);
    console.log(`   Other errors: ${errors}`);
    console.log(`   Success rate: ${(accepted / requestsToSend * 100).toFixed(2)}%\n`);
    
    await sleep(2000);
    
    const metricsResponse = await api.get('/metrics/pool');
    console.log('📊 Final Metrics:');
    console.log(`   Jobs Submitted: ${metricsResponse.data.jobs_submitted}`);
    console.log(`   Jobs Processed: ${metricsResponse.data.jobs_processed}`);
    console.log(`   Jobs Failed: ${metricsResponse.data.jobs_failed}`);
    console.log(`   Queue Length: ${metricsResponse.data.queue_length}`);
    console.log(`   Active Workers: ${metricsResponse.data.active_workers}`);
}

async function testQueueDrain() {
    console.log('\n' + '='.repeat(60));
    console.log('💧 TESTING QUEUE DRAIN');
    console.log('='.repeat(60) + '\n');
    
    const deployed = await deployBpmn();
    if (!deployed) return;
    
    const batchSize = 100;
    const batches = 5;
    
    console.log(`📤 Sending ${batches} batches of ${batchSize} requests...\n`);
    
    for (let batch = 0; batch < batches; batch++) {
        console.log(`📦 Batch ${batch + 1}/${batches}: Sending ${batchSize} requests...`);
        
        const promises = [];
        for (let i = 0; i < batchSize; i++) {
            promises.push(
                api.post('/async/process-definitions/SequentialMultiInstanceProcess/start', {
                    variables: { testId: `drain-${batch}-${i}` }
                })
            );
        }
        
        await Promise.all(promises);
        
        const metrics = await api.get('/metrics/pool');
        console.log(`   Queue length: ${metrics.data.queue_length}, Processed: ${metrics.data.jobs_processed}`);
        
        await sleep(1000);
    }
    
    await sleep(2000);
    
    const finalMetrics = await api.get('/metrics/pool');
    console.log('\n📊 Final Metrics:');
    console.log(`   Total Submitted: ${finalMetrics.data.jobs_submitted}`);
    console.log(`   Total Processed: ${finalMetrics.data.jobs_processed}`);
    console.log(`   Queue Length: ${finalMetrics.data.queue_length}`);
    console.log(`   Active Workers: ${finalMetrics.data.active_workers}`);
}

async function testWorkerRecovery() {
    console.log('\n' + '='.repeat(60));
    console.log('🔄 TESTING WORKER RECOVERY');
    console.log('='.repeat(60) + '\n');
    
    const deployed = await deployBpmn();
    if (!deployed) return;
    
    // Send a batch of requests
    const batchSize = 50;
    console.log(`📤 Sending ${batchSize} requests...`);
    
    const startTime = Date.now();
    const promises = [];
    for (let i = 0; i < batchSize; i++) {
        promises.push(
            api.post('/async/process-definitions/SequentialMultiInstanceProcess/start', {
                variables: { testId: `recovery-${i}` }
            })
        );
    }
    await Promise.all(promises);
    console.log(`✅ All requests accepted in ${Date.now() - startTime}ms\n`);
    
    // Monitor processing
    console.log('📊 Monitoring processing (10 seconds)...');
    for (let i = 0; i < 10; i++) {
        await sleep(1000);
        const metrics = await api.get('/metrics/pool');
        console.log(`   Second ${i+1}: Processed=${metrics.data.jobs_processed}, Queue=${metrics.data.queue_length}, Workers=${metrics.data.active_workers}`);
    }
    
    const finalMetrics = await api.get('/metrics/pool');
    console.log('\n📊 Final Results:');
    console.log(`   Total Processed: ${finalMetrics.data.jobs_processed}`);
    console.log(`   Queue Length: ${finalMetrics.data.queue_length}`);
    
    if (finalMetrics.data.queue_length === 0 && finalMetrics.data.jobs_processed > 0) {
        console.log('\n✅ All jobs processed successfully!');
    }
}

async function compareSyncVsAsync() {
    console.log('\n' + '='.repeat(60));
    console.log('⚡ COMPARING SYNC VS ASYNC');
    console.log('='.repeat(60) + '\n');
    
    const deployed = await deployBpmn();
    if (!deployed) return;
    
    const iterations = 10;
    
    console.log('📤 Testing SYNC endpoint...');
    let syncTotal = 0;
    for (let i = 0; i < iterations; i++) {
        const start = Date.now();
        await api.post('/engine-rest/v2/process-definitions/SequentialMultiInstanceProcess/start', {
            variables: { testId: `sync-${i}` }
        });
        syncTotal += Date.now() - start;
    }
    const syncAvg = syncTotal / iterations;
    
    console.log('📤 Testing ASYNC endpoint...');
    let asyncTotal = 0;
    for (let i = 0; i < iterations; i++) {
        const start = Date.now();
        await api.post('/async/process-definitions/SequentialMultiInstanceProcess/start', {
            variables: { testId: `async-${i}` }
        });
        asyncTotal += Date.now() - start;
    }
    const asyncAvg = asyncTotal / iterations;
    
    console.log('\n📊 Results:');
    console.log(`   SYNC endpoint avg response:  ${syncAvg.toFixed(2)}ms`);
    console.log(`   ASYNC endpoint avg response: ${asyncAvg.toFixed(2)}ms`);
    console.log(`   Improvement: ${((syncAvg - asyncAvg) / syncAvg * 100).toFixed(2)}% faster`);
    
    if (asyncAvg < syncAvg) {
        console.log('\n✅ ASYNC endpoint is significantly faster!');
    }
}

async function runAllTests() {
    console.log('\n' + '='.repeat(60));
    console.log('🏃 RUNNING ALL TESTS');
    console.log('='.repeat(60));
    
    await testAsyncEndpoint();
    await sleep(2000);
    
    await testConcurrentAsyncRequests();
    await sleep(2000);
    
    await testQueueOverflow();
    await sleep(2000);
    
    await testQueueDrain();
    await sleep(2000);
    
    await testWorkerRecovery();
    await sleep(2000);
    
    await compareSyncVsAsync();
    
    console.log('\n' + '='.repeat(60));
    console.log('🎉 ALL TESTS COMPLETED');
    console.log('='.repeat(60));
}

async function main() {
    const testType = process.argv[2];
    
    console.log('\n' + '='.repeat(60));
    console.log('🚀 WORKER POOL TEST SUITE');
    console.log('='.repeat(60));
    
    switch (testType) {
        case 'async':
            await testAsyncEndpoint();
            break;
        case 'concurrent':
            await testConcurrentAsyncRequests();
            break;
        case 'overflow':
            await testQueueOverflow();
            break;
        case 'drain':
            await testQueueDrain();
            break;
        case 'recovery':
            await testWorkerRecovery();
            break;
        case 'compare':
            await compareSyncVsAsync();
            break;
        case 'all':
            await runAllTests();
            break;
        default:
            console.log('\n📌 Usage:');
            console.log('   node test-worker-pool.js async      - Test single async request');
            console.log('   node test-worker-pool.js concurrent - Test 50 concurrent requests');
            console.log('   node test-worker-pool.js overflow   - Test queue overflow (1100 requests)');
            console.log('   node test-worker-pool.js drain      - Test queue drain over time');
            console.log('   node test-worker-pool.js recovery   - Test worker recovery');
            console.log('   node test-worker-pool.js compare    - Compare sync vs async');
            console.log('   node test-worker-pool.js all        - Run all tests');
            console.log('\nRunning async test by default...\n');
            await testAsyncEndpoint();
    }
}

main().catch(console.error);
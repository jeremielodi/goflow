// load-test.js
// Performance and load testing for GoFlow server
//
// Run:
//   node load-test.js                    # Run all tests
//   node load-test.js simple             # Simple sequential test
//   node load-test.js parallel           # Parallel requests test
//   node load-test.js stress             # Stress test
//   node load-test.js duration           # Duration test (60 seconds)

const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');
const { Worker } = require('worker_threads');
const os = require('os');

const API_URL = 'http://localhost:8080';
const BPMN_FILE = './SequentialMultiInstance.bpmn';
const PROCESS_KEY = 'SequentialMultiInstanceProcess';

const api = axios.create({
    baseURL: API_URL,
    timeout: 60000,
    headers: { 'Content-Type': 'application/json' }
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ============================================================
// TEST CONFIGURATION
// ============================================================

const CONFIG = {
    // Test durations (ms)
    simpleDuration: 30000,
    stressDuration: 60000,
    
    // Concurrent request levels
    concurrencyLevels: [1, 5, 10, 20, 50, 100],
    
    // Stress test settings
    initialConcurrency: 1,
    maxConcurrency: 200,
    stepSize: 10,
    stepDuration: 5000, // 5 seconds per level
    
    // Timeout settings
    requestTimeout: 30000,
    deployTimeout: 10000,
    
    // Output
    verbose: false,
    saveResults: true
};

// ============================================================
// RESULTS COLLECTOR
// ============================================================

class ResultsCollector {
    constructor(testName) {
        this.testName = testName;
        this.startTime = null;
        this.endTime = null;
        this.results = [];
        this.errors = [];
        this.totalRequests = 0;
        this.successfulRequests = 0;
        this.failedRequests = 0;
        this.responseTimes = [];
    }

    start() {
        this.startTime = Date.now();
    }

    stop() {
        this.endTime = Date.now();
    }

    addResult(duration, success, error = null) {
        this.totalRequests++;
        if (success) {
            this.successfulRequests++;
            this.responseTimes.push(duration);
        } else {
            this.failedRequests++;
            this.errors.push({ time: new Date(), error: error?.message });
        }
        this.results.push({ duration, success, error: error?.message });
    }

    getStats() {
        const sortedTimes = [...this.responseTimes].sort((a, b) => a - b);
        const totalDuration = (this.endTime - this.startTime) / 1000;
        
        return {
            testName: this.testName,
            duration: totalDuration,
            totalRequests: this.totalRequests,
            successful: this.successfulRequests,
            failed: this.failedRequests,
            successRate: ((this.successfulRequests / this.totalRequests) * 100).toFixed(2) + '%',
            requestsPerSecond: (this.totalRequests / totalDuration).toFixed(2),
            
            responseTime: {
                min: sortedTimes[0] || 0,
                max: sortedTimes[sortedTimes.length - 1] || 0,
                avg: (this.responseTimes.reduce((a, b) => a + b, 0) / this.responseTimes.length || 0).toFixed(2),
                median: sortedTimes[Math.floor(sortedTimes.length / 2)] || 0,
                p95: sortedTimes[Math.floor(sortedTimes.length * 0.95)] || 0,
                p99: sortedTimes[Math.floor(sortedTimes.length * 0.99)] || 0
            }
        };
    }

    print() {
        const stats = this.getStats();
        console.log('\n' + '='.repeat(60));
        console.log(`📊 TEST RESULTS: ${stats.testName}`);
        console.log('='.repeat(60));
        console.log(`Duration:           ${stats.duration.toFixed(2)}s`);
        console.log(`Total Requests:     ${stats.totalRequests}`);
        console.log(`Successful:         ${stats.successful}`);
        console.log(`Failed:             ${stats.failed}`);
        console.log(`Success Rate:       ${stats.successRate}`);
        console.log(`Requests/sec:       ${stats.requestsPerSecond}`);
        console.log('\nResponse Times (ms):');
        console.log(`  Min:    ${stats.responseTime.min}`);
        console.log(`  Max:    ${stats.responseTime.max}`);
        console.log(`  Avg:    ${stats.responseTime.avg}`);
        console.log(`  Median: ${stats.responseTime.median}`);
        console.log(`  P95:    ${stats.responseTime.p95}`);
        console.log(`  P99:    ${stats.responseTime.p99}`);
        console.log('='.repeat(60) + '\n');
    }

    saveToFile() {
        if (!CONFIG.saveResults) return;
        
        const stats = this.getStats();
        const filename = `load-test-results-${this.testName}-${Date.now()}.json`;
        fs.writeFileSync(filename, JSON.stringify(stats, null, 2));
        console.log(`📁 Results saved to: ${filename}`);
    }
}

// ============================================================
// API CLIENT
// ============================================================

class GoFlowClient {
    static async deployBpmn(filePath, deploymentName) {
        if (!fs.existsSync(filePath)) {
            throw new Error(`BPMN file not found: ${filePath}`);
        }

        const form = new FormData();
        form.append('resources', fs.createReadStream(filePath));
        form.append('deployment-name', deploymentName);
        form.append('deployment-source', 'load-test');

        const response = await api.post('/engine-rest/v2/deployment/create', form, {
            headers: { ...form.getHeaders() },
            timeout: CONFIG.deployTimeout
        });

        return response.data;
    }

    static async startProcess(processKey, variables = {}) {
        const startTime = Date.now();
        try {
            const response = await api.post(`/engine-rest/v2/process-definitions/${processKey}/start`, { 
                variables 
            }, {
                timeout: CONFIG.requestTimeout
            });
            return { success: true, data: response.data, duration: Date.now() - startTime };
        } catch (error) {
            return { success: false, error, duration: Date.now() - startTime };
        }
    }

    static async getProcessInstance(instanceId) {
        try {
            const response = await api.get(`/engine-rest/v2/process-instances/${instanceId}`);
            return response.data;
        } catch (error) {
            if (error.response?.status === 404) return null;
            throw error;
        }
    }

    static async healthCheck() {
        try {
            const response = await api.get('/');
            return response.status === 200;
        } catch (error) {
            return false;
        }
    }
}

// ============================================================
// SIMPLE PERFORMANCE TEST
// ============================================================

async function simplePerformanceTest() {
    const collector = new ResultsCollector('Simple Performance Test');
    
    console.log('\n🔧 Running simple performance test...');
    console.log(`   Duration: ${CONFIG.simpleDuration / 1000}s`);
    console.log(`   Concurrent requests: 1\n`);
    
    // Deploy BPMN once
    await GoFlowClient.deployBpmn(BPMN_FILE, 'Load Test');
    
    collector.start();
    
    const endTime = Date.now() + CONFIG.simpleDuration;
    let requestCount = 0;
    
    while (Date.now() < endTime) {
        const result = await GoFlowClient.startProcess(PROCESS_KEY, {
            testId: `load-test-${requestCount++}`,
            startTime: new Date().toISOString()
        });
        
        collector.addResult(result.duration, result.success, result.error);
        
        // Small delay between requests
        await sleep(100);
    }
    
    collector.stop();
    collector.print();
    collector.saveToFile();
    
    return collector.getStats();
}

// ============================================================
// PARALLEL REQUESTS TEST
// ============================================================

async function runConcurrentRequests(concurrency, duration, collector) {
    let activeRequests = 0;
    let completed = false;
    let requestId = 0;
    
    const makeRequest = async () => {
        if (completed) return;
        
        activeRequests++;
        const result = await GoFlowClient.startProcess(PROCESS_KEY, {
            testId: `concurrent-${Date.now()}-${requestId++}`,
            startTime: new Date().toISOString()
        });
        collector.addResult(result.duration, result.success, result.error);
        activeRequests--;
        
        // Continue making requests
        if (!completed) {
            makeRequest();
        }
    };
    
    // Start concurrent workers
    const workers = [];
    for (let i = 0; i < concurrency; i++) {
        workers.push(makeRequest());
    }
    
    // Wait for duration
    await sleep(duration);
    completed = true;
    
    // Wait for all requests to complete
    await Promise.all(workers);
}

async function parallelRequestsTest() {
    const collector = new ResultsCollector('Parallel Requests Test');
    
    console.log('\n🚀 Running parallel requests test...');
    console.log(`   Testing concurrency levels: ${CONFIG.concurrencyLevels.join(', ')}\n`);
    
    // Deploy BPMN once
    await GoFlowClient.deployBpmn(BPMN_FILE, 'Load Test');
    
    const results = [];
    
    for (const concurrency of CONFIG.concurrencyLevels) {
        console.log(`\n📊 Testing concurrency: ${concurrency} requests`);
        
        const testCollector = new ResultsCollector(`Concurrency ${concurrency}`);
        testCollector.start();
        
        await runConcurrentRequests(concurrency, 10000, testCollector); // 10 seconds per level
        
        testCollector.stop();
        testCollector.print();
        
        results.push({
            concurrency,
            stats: testCollector.getStats()
        });
        
        // Wait between tests
        await sleep(2000);
    }
    
    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 PARALLEL REQUESTS SUMMARY');
    console.log('='.repeat(60));
    console.log('Concurrency | Requests/sec | Success Rate | Avg Response (ms)');
    console.log('-' .repeat(60));
    for (const r of results) {
        console.log(`${r.concurrency.toString().padEnd(11)} | ${r.stats.requestsPerSecond.toString().padEnd(12)} | ${r.stats.successRate.padEnd(12)} | ${r.stats.responseTime.avg}`);
    }
    console.log('='.repeat(60));
    
    return results;
}

// ============================================================
// STRESS TEST (Increasing load)
// ============================================================

async function stressTest() {
    const collector = new ResultsCollector('Stress Test');
    
    console.log('\n💪 Running stress test...');
    console.log(`   Starting concurrency: ${CONFIG.initialConcurrency}`);
    console.log(`   Max concurrency: ${CONFIG.maxConcurrency}`);
    console.log(`   Step size: ${CONFIG.stepSize}`);
    console.log(`   Step duration: ${CONFIG.stepDuration / 1000}s\n`);
    
    await GoFlowClient.deployBpmn(BPMN_FILE, 'Load Test');
    
    collector.start();
    
    let currentConcurrency = CONFIG.initialConcurrency;
    let step = 0;
    
    while (currentConcurrency <= CONFIG.maxConcurrency) {
        console.log(`\n📈 Step ${++step}: Testing concurrency = ${currentConcurrency}`);
        
        const stepCollector = new ResultsCollector(`Stress Step ${step}`);
        stepCollector.start();
        
        await runConcurrentRequests(currentConcurrency, CONFIG.stepDuration, stepCollector);
        
        stepCollector.stop();
        stepCollector.print();
        
        // Check if failure rate is too high
        const stats = stepCollector.getStats();
        if (parseFloat(stats.successRate) < 50) {
            console.log(`\n⚠️ Failure rate exceeded 50% at concurrency ${currentConcurrency}`);
            console.log(`   Maximum sustainable concurrency: ${currentConcurrency - CONFIG.stepSize}`);
            break;
        }
        
        currentConcurrency += CONFIG.stepSize;
        
        // Cool down between steps
        await sleep(2000);
    }
    
    collector.stop();
    collector.print();
    collector.saveToFile();
    
    return collector.getStats();
}

// ============================================================
// DURATION TEST (Sustained load)
// ============================================================

async function durationTest() {
    const duration = 60000; // 60 seconds
    const concurrency = 20; // Fixed concurrency
    
    const collector = new ResultsCollector(`Duration Test (${duration / 1000}s @ ${concurrency} concurrent)`);
    
    console.log(`\n⏱️ Running duration test...`);
    console.log(`   Duration: ${duration / 1000}s`);
    console.log(`   Concurrency: ${concurrency}\n`);
    
    await GoFlowClient.deployBpmn(BPMN_FILE, 'Load Test');
    
    collector.start();
    await runConcurrentRequests(concurrency, duration, collector);
    collector.stop();
    
    collector.print();
    collector.saveToFile();
    
    return collector.getStats();
}

// ============================================================
// CONNECTION POOL TEST
// ============================================================

async function connectionPoolTest() {
    const collector = new ResultsCollector('Connection Pool Test');
    
    console.log('\n🔌 Testing connection pool...');
    console.log('   Testing rapid sequential requests\n');
    
    await GoFlowClient.deployBpmn(BPMN_FILE, 'Load Test');
    
    collector.start();
    
    const requestCount = 1000;
    const batchSize = 50;
    const batchDelay = 100;
    
    for (let i = 0; i < requestCount; i += batchSize) {
        const batch = [];
        const batchStart = Date.now();
        
        for (let j = 0; j < batchSize && i + j < requestCount; j++) {
            batch.push(GoFlowClient.startProcess(PROCESS_KEY, {
                testId: `pool-test-${i + j}`,
                startTime: new Date().toISOString()
            }));
        }
        
        const results = await Promise.all(batch);
        for (const result of results) {
            collector.addResult(result.duration, result.success, result.error);
        }
        
        const batchDuration = Date.now() - batchStart;
        console.log(`   Batch ${Math.floor(i / batchSize) + 1}: ${results.length} requests in ${batchDuration}ms`);
        
        if (batchDelay > 0) await sleep(batchDelay);
    }
    
    collector.stop();
    collector.print();
    collector.saveToFile();
    
    return collector.getStats();
}

// ============================================================
// HEALTH CHECK
// ============================================================

async function checkServerHealth() {
    console.log('\n🏥 Checking server health...');
    
    const isHealthy = await GoFlowClient.healthCheck();
    if (!isHealthy) {
        console.error('❌ Server is not responding!');
        console.error('   Make sure your GoFlow server is running on port 8080');
        process.exit(1);
    }
    
    console.log('✅ Server is healthy\n');
}

// ============================================================
// MAIN
// ============================================================

async function main() {
    const testType = process.argv[2];
    
    console.log('\n' + '='.repeat(60));
    console.log('🚀 GOFLOW LOAD TEST SUITE');
    console.log('='.repeat(60));
    console.log(`Server: ${API_URL}`);
    console.log(`CPU Cores: ${os.cpus().length}`);
    console.log(`Total Memory: ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(2)} GB`);
    console.log('='.repeat(60));
    
    await checkServerHealth();
    
    try {
        switch (testType) {
            case 'simple':
                await simplePerformanceTest();
                break;
                
            case 'parallel':
                await parallelRequestsTest();
                break;
                
            case 'stress':
                await stressTest();
                break;
                
            case 'duration':
                await durationTest();
                break;
                
            case 'pool':
                await connectionPoolTest();
                break;
                
            case 'all':
                await simplePerformanceTest();
                await sleep(2000);
                await parallelRequestsTest();
                await sleep(2000);
                await connectionPoolTest();
                await sleep(2000);
                await stressTest();
                break;
                
            default:
                console.log('\n📌 Usage:');
                console.log('   node load-test.js simple    - Simple sequential test (30s)');
                console.log('   node load-test.js parallel  - Test different concurrency levels');
                console.log('   node load-test.js stress    - Increasing load until failure');
                console.log('   node load-test.js duration  - Sustained load test (60s)');
                console.log('   node load-test.js pool      - Test connection pool with rapid requests');
                console.log('   node load-test.js all       - Run all tests');
                console.log('\nRunning simple test by default...\n');
                await simplePerformanceTest();
        }
        
        console.log('\n✅ Load test completed!\n');
        
    } catch (error) {
        console.error('\n❌ Load test failed:', error.message);
        process.exit(1);
    }
}

// ============================================================
// RUN
// ============================================================

if (require.main === module) {
    main().catch(console.error);
}

module.exports = {
    simplePerformanceTest,
    parallelRequestsTest,
    stressTest,
    durationTest,
    connectionPoolTest
};
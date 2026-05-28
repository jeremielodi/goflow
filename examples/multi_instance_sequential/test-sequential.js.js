// sequential-multi-instance.test.js
// Complete test for Sequential Multi-Instance BPMN feature
//
// Run:
//   node sequential-multi-instance.test.js

const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');

const API_URL = 'http://localhost:8080';
const BPMN_FILE = './SequentialMultiInstance.bpmn';
const PROCESS_KEY = 'SequentialMultiInstanceProcess';
const TOPIC_NAME = 'process_item';
const EXPECTED_ITEMS = 3;

const api = axios.create({
    baseURL: API_URL,
    timeout: 30000,
    headers: { 'Content-Type': 'application/json' }
});

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ============================================================
// LOGGING HELPERS
// ============================================================

const log = {
    section: (title) => {
        console.log('\n' + '='.repeat(60));
        console.log(` ${title}`);
        console.log('='.repeat(60));
    },
    info: (msg) => console.log(`📌 ${msg}`),
    success: (msg) => console.log(`✅ ${msg}`),
    error: (msg) => console.log(`❌ ${msg}`),
    warning: (msg) => console.log(`⚠️ ${msg}`),
    progress: (current, total) => console.log(`📊 Progress: ${current}/${total} items processed`),
    item: (index, value) => console.log(`📦 Processing item ${index}/${EXPECTED_ITEMS}: ${value}`)
};

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
        form.append('deployment-source', 'test');

        const response = await api.post('/engine-rest/v2/deployment/create', form, {
            headers: { ...form.getHeaders() }
        });

        log.success(`Deployed: ${response.data.name} (ID: ${response.data.id})`);
        return response.data;
    }

    static async startProcess(processKey, variables = {}) {
        const response = await api.post(`/engine-rest/v2/process-definitions/${processKey}/start`, { variables });
        log.info(`Process started with instance ID: ${response.data.processInstanceId}`);
        return response.data;
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

    static async getJobs(processInstanceId = null, topic = null) {
        try {
            const params = {};
            if (processInstanceId) params.processInstanceId = processInstanceId;
            if (topic) params.topic = topic;
            const response = await api.get('/jobs', { params });
            return response.data || [];
        } catch (error) {
            return [];
        }
    }
}

// ============================================================
// EXTERNAL TASK WORKER
// ============================================================

class SequentialWorker {
    constructor(topicName = 'process_item') {
        this.workerId = `worker-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
        this.topicName = topicName;
        this.polling = true;
        this.processedItems = [];
        this.completedCount = 0;
        this.lockDuration = 10000;
    }

    async start() {
        log.info(`Starting worker: ${this.workerId}`);

        while (this.polling) {
            try {
                const response = await api.post('/engine-rest/external-task/fetchAndLock', {
                    workerId: this.workerId,
                    maxTasks: 1,
                    topics: [{
                        topicName: this.topicName,
                        lockDuration: this.lockDuration
                    }]
                });

                const tasks = response.data || [];
                
                for (const task of tasks) {
                    await this.processTask(task);
                }

                await sleep(500);
            } catch (error) {
                // Silently ignore fetch errors
                await sleep(1000);
            }
        }
    }

    async processTask(task) {
        const vars = task.variables || {};
        const itemValue = vars.elementValue?.value || vars.elementValue || 
                          `Item ${this.completedCount + 1}`;
        
        this.completedCount++;
        this.processedItems.push(itemValue);
        
        log.item(this.completedCount, itemValue);
        
        // Simulate work
        await sleep(1000);

        await api.post(`/engine-rest/external-task/${task.id}/complete`, {
            workerId: this.workerId,
            variables: {
                result: { value: `Processed: ${itemValue}`, type: 'string' },
                processedAt: { value: new Date().toISOString(), type: 'string' }
            }
        });

        log.success(`Item ${itemValue} completed (${this.completedCount}/${EXPECTED_ITEMS})`);
    }

    async stop() {
        this.polling = false;
        await sleep(500);
        log.info(`Worker stopped. Processed: ${this.processedItems.length} items`);
    }

    getStats() {
        return {
            processedCount: this.completedCount,
            processedItems: this.processedItems
        };
    }
}

// ============================================================
// MAIN TEST
// ============================================================

async function runSequentialMultiInstanceTest() {
    const startTime = Date.now();
    let worker = null;
    let processInstanceId = null;
    
    try {
        log.section('SEQUENTIAL MULTI-INSTANCE TEST');
        log.info(`Expected: ${EXPECTED_ITEMS} items processed sequentially`);
        
        // Step 1: Deploy BPMN from file
        log.section('DEPLOYMENT');
        if (!fs.existsSync(BPMN_FILE)) {
            log.error(`BPMN file not found: ${BPMN_FILE}`);
            log.info(`Please ensure ${BPMN_FILE} exists in the current directory`);
            return false;
        }
        await GoFlowClient.deployBpmn(BPMN_FILE, 'Sequential Multi-Instance Test');
        
        // Step 2: Start worker
        log.section('WORKER STARTUP');
        worker = new SequentialWorker(TOPIC_NAME);
        worker.start();
        await sleep(500);
        
        // Step 3: Start process
        log.section('PROCESS START');
        const processData = await GoFlowClient.startProcess(PROCESS_KEY, {});
        processInstanceId = processData.processInstanceId;
        
        // Step 4: Monitor progress
        log.section('MONITORING PROGRESS');
        log.info('Waiting for sequential execution...\n');
        
        let lastCompleted = 0;
        let maxWaitTime = 45000;
        let elapsed = 0;
        const checkInterval = 2000;
        
        while (elapsed < maxWaitTime) {
            await sleep(checkInterval);
            elapsed += checkInterval;
            
            const jobs = await GoFlowClient.getJobs(processInstanceId, TOPIC_NAME);
            const completedJobs = jobs.filter(j => j.status === 'completed');
            
            if (completedJobs.length > lastCompleted) {
                lastCompleted = completedJobs.length;
                log.progress(lastCompleted, EXPECTED_ITEMS);
            }
            
            if (lastCompleted >= EXPECTED_ITEMS) {
                log.success(`All ${EXPECTED_ITEMS} items processed!`);
                break;
            }
        }
        
        // Step 5: Wait for process completion
        await sleep(2000);
        
        // Step 6: Report results
        log.section('TEST RESULTS');
        const duration = (Date.now() - startTime) / 1000;
        
        // Check process instance
        const processInstance = await GoFlowClient.getProcessInstance(processInstanceId);
        if (!processInstance) {
            log.success(`Process completed successfully`);
        } else {
            log.warning(`Process status: ${processInstance.status}`);
        }
        
        // Check jobs
        const finalJobs = await GoFlowClient.getJobs(processInstanceId, TOPIC_NAME);
        const completedCount = finalJobs.filter(j => j.status === 'completed').length;
        log.info(`Jobs created: ${finalJobs.length}`);
        log.info(`Jobs completed: ${completedCount}`);
        
        if (worker && worker.getStats().processedCount === EXPECTED_ITEMS) {
            log.success(`TEST PASSED! (${duration}s)`);
            log.info(`Items processed: ${worker.getStats().processedItems.join(' → ')}`);
            return true;
        } else {
            log.error(`TEST FAILED! Expected ${EXPECTED_ITEMS} items, got ${worker?.getStats().processedCount || 0}`);
            return false;
        }
        
    } catch (error) {
        log.section('TEST ERROR');
        log.error(error.message);
        if (error.response?.data) {
            console.error(error.response.data);
        }
        return false;
        
    } finally {
        if (worker) {
            await worker.stop();
        }
        
        log.section('TEST COMPLETE');
        console.log('');
    }
}

// ============================================================
// RUN
// ============================================================

if (require.main === module) {
    runSequentialMultiInstanceTest()
        .then(success => {
            process.exit(success ? 0 : 1);
        })
        .catch(error => {
            console.error('Fatal error:', error);
            process.exit(1);
        });
}

module.exports = { runSequentialMultiInstanceTest, GoFlowClient, SequentialWorker };
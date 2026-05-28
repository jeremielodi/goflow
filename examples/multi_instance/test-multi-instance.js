const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');

const API_URL = 'http://localhost:8080';

const api = axios.create({
    baseURL: API_URL,
    headers: { 'Content-Type': 'application/json' }
});

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

class GoFlowClient {
    static async deployBpmn(filePath, deploymentName = null) {
        if (!fs.existsSync(filePath)) {
            throw new Error(`File not found: ${filePath}`);
        }

        const form = new FormData();
        form.append('resources', fs.createReadStream(filePath));
        form.append('deployment-name', deploymentName || path.basename(filePath, '.bpmn'));
        form.append('deployment-source', 'camunda-modeler');

        const res = await api.post('/engine-rest/v2/deployment/create', form, {
            headers: { ...form.getHeaders() }
        });

        console.log('✅ Deployed:', res.data.name, '- ID:', res.data.id);
        return res.data;
    }

    static async startProcess(processKey, variables = {}) {
        const res = await api.post(`/engine-rest/v2/process-definitions/${processKey}/start`, { variables });
        console.log('🚀 Process started - Instance:', res.data.processInstanceId);
        return res.data;
    }

    static async getProcessInstance(instanceId) {
        try {
            const res = await api.get(`/engine-rest/v2/process-instances/${instanceId}`);
            return res.data;
        } catch (error) {
            if (error.response?.status === 404) return null;
            throw error;
        }
    }

    static async getJobs(processInstanceId = null, jobType = null) {
        try {
            const params = {};
            if (processInstanceId) params.processInstanceId = processInstanceId;
            if (jobType) params.topic = jobType;
            const res = await api.get('/jobs', { params });
            return res.data || [];
        } catch (error) {
            return [];
        }
    }
}

// External Task Worker for multi-instance
class ExternalTaskWorker {
    constructor() {
        this.polling = true;
        this.workerId = `worker-${Date.now()}`;
        this.processedItems = [];
    }

    async start() {
        console.log(`🚀 Starting external task worker (${this.workerId})...\n`);

        while (this.polling) {
            try {
                const response = await api.post('/engine-rest/external-task/fetchAndLock', {
                    workerId: this.workerId,
                    maxTasks: 10,
                    topics: [{
                        topicName: 'process_item',
                        lockDuration: 10000
                    }]
                });

                for (const task of response.data) {
                    const vars = task.variables || {};
                    const itemValue = vars.loopIndex?.value || vars.loopIndex || 
                                      vars.elementValue?.value || vars.elementValue || 
                                      `Item ${this.processedItems.length + 1}`;
                    
                    this.processedItems.push(itemValue);
                    console.log(`\n📦 [WORKER] Processing item: ${itemValue}`);
                    console.log(`   Task ID: ${task.id}`);
                    console.log(`   Items processed: ${this.processedItems.length}/3`);

                    await api.post(`/engine-rest/external-task/${task.id}/complete`, {
                        workerId: this.workerId,
                        variables: {
                            result: { value: `Processed: ${itemValue}`, type: 'string' },
                            processedAt: { value: new Date().toISOString(), type: 'string' }
                        }
                    });
                    console.log(`   ✅ Item ${itemValue} processed\n`);
                }
            } catch (error) {
                // Silently ignore
            }
            await sleep(1000);
        }
    }

    stop() {
        this.polling = false;
        console.log(`\n📊 Total items processed: ${this.processedItems.length}`);
        if (this.processedItems.length === 3) {
            console.log('✅ All 3 items processed successfully!');
        }
    }
}

// Test: Parallel Multi-Instance
async function testParallelMultiInstance() {
    console.log('\n' + '='.repeat(60));
    console.log('🧪 TEST: Parallel Multi-Instance (3 items)');
    console.log('='.repeat(60) + '\n');

    // Deploy BPMN
    await GoFlowClient.deployBpmn('./MultiInstanceTest.bpmn', 'Multi-Instance Test');

    // Start worker
    const worker = new ExternalTaskWorker();
    worker.start();

    // Start process
    const processData = await GoFlowClient.startProcess('MultiInstanceProcess', {
        startTime: new Date().toISOString()
    });

    const instanceId = processData.processInstanceId;
    console.log(`📋 Process Instance ID: ${instanceId}\n`);

    // Monitor progress
    let lastCompletedCount = 0;
    const maxWaitTime = 30000; // 30 seconds
    const startTime = Date.now();

    while ((Date.now() - startTime) < maxWaitTime) {
        await sleep(2000);
        
        const jobs = await GoFlowClient.getJobs(instanceId, 'process_item');
        const completedJobs = jobs.filter(j => j.status === 'completed');
        
        if (completedJobs.length > lastCompletedCount) {
            console.log(`📊 Progress: ${completedJobs.length}/3 items processed`);
            lastCompletedCount = completedJobs.length;
        }
        
        if (completedJobs.length >= 3) {
            console.log('\n🎉 All 3 items processed!');
            break;
        }
    }

    // Check process completion
    await sleep(3000);
    const processInstance = await GoFlowClient.getProcessInstance(instanceId);
    
    console.log('\n' + '='.repeat(60));
    console.log('📊 FINAL RESULTS');
    console.log('='.repeat(60));
    
    const finalJobs = await GoFlowClient.getJobs(instanceId, 'process_item');
    console.log(`Total jobs created: ${finalJobs.length}`);
    console.log(`Completed: ${finalJobs.filter(j => j.status === 'completed').length}`);
    
    if (!processInstance) {
        console.log('✅ Process completed successfully!');
    } else {
        console.log(`⚠️ Process status: ${processInstance.status}`);
    }

    worker.stop();
    console.log('='.repeat(60) + '\n');
}

// Run tests
if (require.main === module) {
    const testType = process.argv[2];
    
    if (testType === 'worker') {
        const worker = new ExternalTaskWorker();
        worker.start();
    } else {
        console.log('\n📌 Usage:');
        console.log('   node test-multi-instance.js worker    - Start external worker only');
        console.log('   node test-multi-instance.js           - Run parallel multi-instance test');
        console.log('\nRunning parallel multi-instance test...\n');
        testParallelMultiInstance();
    }
}

module.exports = { GoFlowClient, ExternalTaskWorker };
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

    static async getTasks(processInstanceId = null) {
        const params = processInstanceId ? { processInstanceId } : {};
        const res = await api.get('/tasks', { params });
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

    static async getTimers(processInstanceId = null) {
        try {
            const params = processInstanceId ? { processInstanceId } : {};
            const res = await api.get('/timers', { params });
            return res.data || [];
        } catch (error) {
            if (error.response?.status === 404) {
                return [];
            }
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
            console.error('Failed to get jobs:', error.message);
            return [];
        }
    }
}

// External Task Worker for escalate_issue
class ExternalTaskWorker {
    constructor() {
        this.polling = true;
        this.workerId = `worker-${Date.now()}`;
        this.cycleCount = 0;
    }

    async start() {
        console.log(`🚀 Starting external task worker (${this.workerId})...\n`);

        while (this.polling) {
            try {
                const response = await api.post('/engine-rest/external-task/fetchAndLock', {
                    workerId: this.workerId,
                    maxTasks: 10,
                    topics: [{
                        topicName: 'escalate_issue',
                        lockDuration: 10000
                    }]
                });

                for (const task of response.data) {
                    this.cycleCount++;
                    console.log(`\n🚨 [CYCLE ${this.cycleCount}] Escalate Issue triggered!`);
                    console.log(`   Task ID: ${task.id}`);
                    console.log(`   Time: ${new Date().toISOString()}`);

                    await api.post(`/engine-rest/external-task/${task.id}/complete`, {
                        workerId: this.workerId,
                        variables: {
                            escalated: { value: true, type: 'boolean' },
                            cycleNumber: { value: this.cycleCount, type: 'integer' },
                            escalationTime: { value: new Date().toISOString(), type: 'string' }
                        }
                    });
                    console.log(`   ✅ Escalation ${this.cycleCount} completed\n`);
                }
            } catch (error) {
                // Silently ignore
            }
            await sleep(1000);
        }
    }

    stop() {
        this.polling = false;
        console.log(`\n📊 Total cycles processed: ${this.cycleCount}`);
    }
}

// Test: Timer Cycle with 3 repetitions every 10 seconds
async function testTimerCycle() {
    console.log('\n' + '='.repeat(60));
    console.log('🧪 TEST: Timer Cycle (R3/PT10S) - 3 cycles every 10 seconds');
    console.log('='.repeat(60) + '\n');

    // Deploy BPMN
    await GoFlowClient.deployBpmn('./TimerCycleTest.bpmn', 'Timer Cycle Test');

    // Start process
    const processData = await GoFlowClient.startProcess('TimerCycleProcess', {
        orderId: 'CYCLE-TEST-001',
        amount: 1000,
        startTime: new Date().toISOString()
    });

    const instanceId = processData.processInstanceId;
    console.log(`📋 Process Instance ID: ${instanceId}\n`);

    // Wait for user task to be created
    console.log('⏳ Waiting for user task to be created...');
    let userTask = null;
    for (let i = 0; i < 10; i++) {
        await sleep(2000);
        const tasks = await GoFlowClient.getTasks(instanceId);
        userTask = tasks.find(t => t.taskDefinitionKey === 'Task_WaitForTimer');
        if (userTask) {
            console.log(`✅ User task created: ${userTask.id}\n`);
            break;
        }
    }

    if (!userTask) {
        console.log('❌ User task not found!');
        return;
    }

    // Start worker
    const worker = new ExternalTaskWorker();
    worker.start();

    // Monitor cycles using jobs API
    console.log('⏰ Monitoring for timer cycles (60 seconds)...\n');
    console.log('Expected: 3 escalations every 10 seconds\n');

    let lastCompletedCount = 0;
    
    for (let i = 1; i <= 12; i++) {
        await sleep(5000);
        
        const elapsed = i * 5;
        
        // Get completed jobs count
        const jobs = await GoFlowClient.getJobs(instanceId, 'escalate_issue');
        const completedCount = jobs.filter(j => j.status === 'completed').length;
        
        if (completedCount > lastCompletedCount) {
            console.log(`\n🚨 [CYCLE ${completedCount}] Escalate Issue completed at ${elapsed}s!`);
            lastCompletedCount = completedCount;
        }
        
        const timers = await GoFlowClient.getTimers(instanceId);
        const activeTimers = timers.filter(t => !t.isTriggered);
        const processInstance = await GoFlowClient.getProcessInstance(instanceId);
        
        console.log(`📊 ${elapsed}s: Completed cycles: ${completedCount}/3, Active timers: ${activeTimers.length}, Process active: ${!!processInstance}`);
        
        if (completedCount >= 3) {
            console.log(`\n🎉 All 3 cycles completed after ${elapsed}s!`);
            break;
        }
    }

    // Wait a bit and stop worker
    await sleep(2000);
    worker.stop();
    
    // Final verification
    const finalJobs = await GoFlowClient.getJobs(instanceId, 'escalate_issue');
    const finalCompleted = finalJobs.filter(j => j.status === 'completed').length;
    
    console.log('\n' + '='.repeat(60));
    console.log('📊 FINAL RESULTS');
    console.log('='.repeat(60));
    console.log(`Total escalate_issue jobs: ${finalJobs.length}`);
    console.log(`Completed: ${finalCompleted}`);
    
    if (finalCompleted === 3) {
        console.log('\n✅ TEST PASSED: All 3 timer cycles completed successfully! 🎉');
    } else {
        console.log(`\n❌ TEST FAILED: Expected 3 cycles, got ${finalCompleted}`);
    }
    console.log('='.repeat(60) + '\n');
}

// Run tests
if (require.main === module) {
    const testType = process.argv[2];
    
    if (testType === 'worker') {
        const worker = new ExternalTaskWorker();
        worker.start();
    } else if (testType === 'cycle') {
        testTimerCycle();
    } else {
        console.log('\n📌 Usage:');
        console.log('   node test-timer-cycle.js worker   - Start external worker only');
        console.log('   node test-timer-cycle.js cycle    - Test timer cycle (3x every 10s)');
        console.log('\nRunning timer cycle test by default...\n');
        testTimerCycle();
    }
}

module.exports = { GoFlowClient, ExternalTaskWorker };
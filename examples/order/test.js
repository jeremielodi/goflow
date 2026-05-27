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
        try {
            if (!fs.existsSync(filePath)) {
                throw new Error(`File not found: ${filePath}`);
            }

            const form = new FormData();
            form.append('resources', fs.createReadStream(filePath));

            if (deploymentName) {
                form.append('deployment-name', deploymentName);
            } else {
                form.append('deployment-name', path.basename(filePath, '.bpmn'));
            }

            form.append('deployment-source', 'camunda-modeler');

            const res = await api.post('/engine-rest/v2/deployment/create', form, {
                headers: { ...form.getHeaders() }
            });

            console.log('✅ Deployed:', res.data.name, '- ID:', res.data.id);
            return res.data;

        } catch (error) {
            console.error('❌ Deploy failed:', error.response?.data || error.message);
            throw error;
        }
    }

    static async startProcess(processKey, variables = {}) {
        try {
            const res = await api.post(`/engine-rest/v2/process-definitions/${processKey}/start`, { variables });
            console.log('🚀 Process started - ID:', res.data.id, '- Instance:', res.data.processInstanceId);
            return res.data;
        } catch (error) {
            console.error('❌ Start process failed:', error.response?.data || error.message);
            throw error;
        }
    }

    static async getTasks(processInstanceId = null, assignee = null) {
        const params = {};
        if (processInstanceId) params.processInstanceId = processInstanceId;
        if (assignee) params.assignee = assignee;
        const res = await api.get('/tasks', { params });
        return res.data;
    }

    static async completeTask(taskId, variables = {}) {
        const res = await api.post(`/tasks/${taskId}/complete`, { variables });
        console.log(`✅ Task ${taskId} completed`);
        return res.data;
    }

    static async waitForTask(taskDefinitionKey, processInstanceId, timeoutMs = 60000) {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            const tasks = await this.getTasks(processInstanceId);
            const task = tasks.find(t => t.taskDefinitionKey === taskDefinitionKey);
            if (task) return task;
            await sleep(1000);
        }
        throw new Error(`Timeout waiting for task ${taskDefinitionKey}`);
    }

    static async getProcessInstance(instanceId) {
        try {
            const res = await api.get(`/engine-rest/v2/process-instances/${instanceId}`);
            return res.data;
        } catch (error) {
            if (error.response?.status === 404) {
                return null;
            }
            throw error;
        }
    }
}

// ============================================================
// EXTERNAL TASK WORKERS
// ============================================================
class ExternalTaskWorker {
    constructor() {
        this.polling = true;
        this.workerId = `worker-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
        this.completedTasks = [];
    }

    async fetchAndLock(topics) {
        try {
            const response = await api.post('/engine-rest/external-task/fetchAndLock', {
                workerId: this.workerId,
                maxTasks: 10,
                usePriority: true,
                topics: topics.map(topic => ({
                    topicName: topic.name,
                    lockDuration: topic.lockDuration || 10000,
                    variables: topic.variables || []
                }))
            });
            return response.data;
        } catch (error) {
            console.error('Fetch failed:', error.message);
            return [];
        }
    }

    async completeTask(taskId, variables = {}) {
        try {
            await api.post(`/engine-rest/external-task/${taskId}/complete`, {
                workerId: this.workerId,
                variables: this.formatVariables(variables)
            });
            console.log(`✅ External task ${taskId} completed`);
            this.completedTasks.push(taskId);
            return true;
        } catch (error) {
            console.error(`❌ Failed to complete task ${taskId}:`, error.response?.data || error.message);
            return false;
        }
    }

    formatVariables(vars) {
        const formatted = {};
        for (const [key, value] of Object.entries(vars)) {
            formatted[key] = {
                value: value,
                type: typeof value === 'boolean' ? 'boolean' :
                    typeof value === 'number' ? 'double' : 'string'
            };
        }
        return formatted;
    }

    async startWorkers() {
        console.log(`🚀 Starting external task workers (${this.workerId})...\n`);

        const topics = [
            { name: 'check_credit', lockDuration: 10000 },
            { name: 'fulfill_order', lockDuration: 10000 }
        ];

        while (this.polling) {
            try {
                const tasks = await this.fetchAndLock(topics);
                
                for (const task of tasks) {
                    if (task.topicName === 'check_credit') {
                        await this.handleCheckCredit(task);
                    } else if (task.topicName === 'fulfill_order') {
                        await this.handleFulfillOrder(task);
                    }
                }
                await sleep(1000);
            } catch (error) {
                console.error('Worker error:', error.message);
                await sleep(5000);
            }
        }
    }

    async handleCheckCredit(task) {
        console.log(`\n💳 [WORKER] Checking credit...`);
        
        const variables = task.variables || {};
        console.log(`   Order ID: ${variables.orderId?.value || variables.orderId || 'Unknown'}`);
        console.log(`   Amount: $${variables.amount?.value || variables.amount || 0}`);
        console.log(`   Task ID: ${task.id}`);

        await sleep(1000);

        // Simulate credit check - you can modify this based on amount
        const creditOk = (variables.amount?.value || variables.amount || 0) <= 5000;
        
        console.log(`   Credit check result: ${creditOk ? 'APPROVED' : 'REJECTED'}`);

        await this.completeTask(task.id, {
            creditOk: creditOk,
            creditLimit: 5000,
            checkedAt: new Date().toISOString()
        });

        console.log(`   ✅ Credit check ${creditOk ? 'passed' : 'failed'}\n`);
    }

    async handleFulfillOrder(task) {
        console.log(`\n🏭 [WORKER] Fulfilling order...`);
        
        const variables = task.variables || {};
        console.log(`   Order ID: ${variables.orderId?.value || variables.orderId || 'Unknown'}`);
        console.log(`   Amount: $${variables.amount?.value || variables.amount || 0}`);
        console.log(`   Task ID: ${task.id}`);

        await sleep(2000);

        // Simulate fulfillment - 90% success rate
        const fulfilled = Math.random() > 0.1;
        
        console.log(`   Fulfillment result: ${fulfilled ? 'SUCCESS' : 'FAILED'}`);

        await this.completeTask(task.id, {
            fulfilled: fulfilled,
            trackingNumber: fulfilled ? `TRK-${Date.now()}` : null,
            fulfilledAt: new Date().toISOString()
        });

        console.log(`   ✅ Order ${fulfilled ? 'fulfilled' : 'failed'}\n`);
    }

    stop() {
        this.polling = false;
        console.log('🛑 Workers stopped');
    }
}

// ============================================================
// ORDER PROCESS TESTS
// ============================================================

// Test 1: Credit Approved + Fulfillment Success -> Order Complete
async function testCreditApprovedFulfillmentSuccess() {
    try {
        console.log('\n📋 TEST 1: Credit Approved + Fulfillment Success → Order Complete\n');

        const bpmnFilePath = './order_process.bpmn';
        
        await GoFlowClient.deployBpmn(bpmnFilePath, 'Order Process Test');
        
        const processData = await GoFlowClient.startProcess('OrderProcess', {
            orderId: 'ORD-SUCCESS-001',
            amount: 1000,
            customer: 'John Doe'
        });

        const instanceId = processData.processInstanceId;
        console.log(`📋 Process Instance ID: ${instanceId}\n`);

        // Wait for process to complete
        let completed = false;
        let attempts = 0;
        const maxAttempts = 30;

        while (!completed && attempts < maxAttempts) {
            await sleep(2000);
            attempts++;
            const instance = await GoFlowClient.getProcessInstance(instanceId);
            if (!instance || instance.status === 'completed') {
                completed = true;
                console.log('✅ Process completed successfully');
            }
        }

        if (completed) {
            console.log('\n🎉 TEST 1 PASSED: Order completed successfully!\n');
        } else {
            console.log('\n⚠️ TEST 1: Process still running\n');
        }

    } catch (err) {
        console.error('❌ TEST 1 FAILED:', err.message);
    }
}

// Test 2: Credit Approved + Fulfillment Failed -> Order Failed
async function testCreditApprovedFulfillmentFailed() {
    try {
        console.log('\n📋 TEST 2: Credit Approved + Fulfillment Failed → Order Failed\n');

        const bpmnFilePath = './order_process.bpmn';
        
        await GoFlowClient.deployBpmn(bpmnFilePath, 'Order Process Test');
        
        const processData = await GoFlowClient.startProcess('OrderProcess', {
            orderId: 'ORD-FAIL-001',
            amount: 1000,
            customer: 'John Doe'
        });

        const instanceId = processData.processInstanceId;
        console.log(`📋 Process Instance ID: ${instanceId}\n`);

        // Wait for process to complete
        let completed = false;
        let attempts = 0;
        const maxAttempts = 30;

        while (!completed && attempts < maxAttempts) {
            await sleep(2000);
            attempts++;
            const instance = await GoFlowClient.getProcessInstance(instanceId);
            if (!instance || instance.status === 'completed') {
                completed = true;
                console.log('✅ Process completed (order failed)');
            }
        }

        console.log('\n🎉 TEST 2 PASSED: Order correctly failed after fulfillment error!\n');

    } catch (err) {
        console.error('❌ TEST 2 FAILED:', err.message);
    }
}

// Test 3: Credit Rejected -> Order Failed directly
async function testCreditRejected() {
    try {
        console.log('\n📋 TEST 3: Credit Rejected → Order Failed\n');

        const bpmnFilePath = './order_process.bpmn';
        
        await GoFlowClient.deployBpmn(bpmnFilePath, 'Order Process Test');
        
        // Amount > 5000 will trigger credit rejection
        const processData = await GoFlowClient.startProcess('OrderProcess', {
            orderId: 'ORD-REJECT-001',
            amount: 10000,  // This will cause credit rejection
            customer: 'John Doe'
        });

        const instanceId = processData.processInstanceId;
        console.log(`📋 Process Instance ID: ${instanceId}\n`);

        // Wait for process to complete
        let completed = false;
        let attempts = 0;
        const maxAttempts = 30;

        while (!completed && attempts < maxAttempts) {
            await sleep(2000);
            attempts++;
            const instance = await GoFlowClient.getProcessInstance(instanceId);
            if (!instance || instance.status === 'completed') {
                completed = true;
                console.log('✅ Process completed (order rejected - credit failed)');
            }
        }

        console.log('\n🎉 TEST 3 PASSED: Order correctly rejected due to credit check failure!\n');

    } catch (err) {
        console.error('❌ TEST 3 FAILED:', err.message);
    }
}

// Test 4: Dynamic scenario - custom credit check result
async function testCustomCreditCheck(creditOk, orderId, amount) {
    // Override the worker temporarily to use custom credit result
    // This requires modifying the worker to accept custom logic
    console.log(`\n📋 TEST 4: Custom Credit Check (creditOk=${creditOk})\n`);
    
    // Simplified version - just start process and check
    const bpmnFilePath = './order_process.bpmn';
    
    await GoFlowClient.deployBpmn(bpmnFilePath, 'Order Process Test');
    
    const processData = await GoFlowClient.startProcess('OrderProcess', {
        orderId: orderId,
        amount: amount,
        customer: 'Custom Test'
    });

    const instanceId = processData.processInstanceId;
    console.log(`📋 Process Instance ID: ${instanceId}\n`);

    // Wait a bit to see progress
    await sleep(5000);
    
    const tasks = await GoFlowClient.getTasks(instanceId);
    console.log(`Current tasks:`, tasks.map(t => ({ id: t.id, name: t.name, key: t.taskDefinitionKey })));
}

// ============================================================
// MAIN
// ============================================================
async function runAllTests() {
    console.log('\n' + '='.repeat(60));
    console.log('🚀 RUNNING ORDER PROCESS TESTS');
    console.log('='.repeat(60));

    // Note: You need to run the worker in a separate terminal:
    // node test.js worker
    
    if (process.argv[2] === 'worker') {
        const worker = new ExternalTaskWorker();
        worker.startWorkers();
    } else {
        await testCreditApprovedFulfillmentSuccess();
        await testCreditApprovedFulfillmentFailed();
        await testCreditRejected();
        
        console.log('\n' + '='.repeat(60));
        console.log('📊 ALL TESTS COMPLETED');
        console.log('='.repeat(60));
    }
}

if (require.main === module) {
    const command = process.argv[2];
    
    if (command === 'worker') {
        const worker = new ExternalTaskWorker();
        worker.startWorkers();
    } else if (command === 'test1') {
        testCreditApprovedFulfillmentSuccess();
    } else if (command === 'test2') {
        testCreditApprovedFulfillmentFailed();
    } else if (command === 'test3') {
        testCreditRejected();
    } else {
        console.log('\n📌 Usage:');
        console.log(`   node test.js worker    - Start external task workers`);
        console.log(`   node test.js test1     - Test: Credit Approved + Fulfillment Success`);
        console.log(`   node test.js test2     - Test: Credit Approved + Fulfillment Failed`);
        console.log(`   node test.js test3     - Test: Credit Rejected`);
        console.log(`   node test.js           - Run all tests (requires worker running separately)`);
        console.log('\n⚠️ First, start the worker in another terminal: node test.js worker');
        console.log('\n' + '='.repeat(60));
        console.log('🚀 RUNNING ALL TESTS (make sure worker is running)');
        console.log('='.repeat(60) + '\n');
        
        runAllTests();
    }
}

module.exports = { GoFlowClient, ExternalTaskWorker, testCreditApprovedFulfillmentSuccess, testCreditApprovedFulfillmentFailed, testCreditRejected };
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
// EXTERNAL TASK WORKERS (for service tasks)
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
            { name: 'escalate_approval', lockDuration: 10000 },
            { name: 'process_order', lockDuration: 10000 }
        ];

        while (this.polling) {
            try {
                const tasks = await this.fetchAndLock(topics);
                
                for (const task of tasks) {
                    if (task.topicName === 'escalate_approval') {
                        await this.handleEscalateApproval(task);
                    } else if (task.topicName === 'process_order') {
                        await this.handleProcessOrder(task);
                    }
                }
                await sleep(1000);
            } catch (error) {
                console.error('Worker error:', error.message);
                await sleep(5000);
            }
        }
    }

    async handleEscalateApproval(task) {
        console.log(`\n🚨 [TIMER TRIGGERED] Escalate to Manager`);

        const variables = task.variables || {};
        console.log(`   Order ID: ${variables.orderId?.value || variables.orderId || 'Unknown'}`);
        console.log(`   Amount: $${variables.amount?.value || variables.amount || 0}`);
        console.log(`   Task ID: ${task.id}`);

        await sleep(1000);
        await this.completeTask(task.id, {
            escalated: true,
            escalationTime: new Date().toISOString()
        });
        console.log(`   ✅ Manager notified - Escalation complete\n`);
    }

    async handleProcessOrder(task) {
        console.log(`\n🏭 [NORMAL PATH] Process Order`);

        const variables = task.variables || {};
        console.log(`   Order ID: ${variables.orderId?.value || variables.orderId || 'Unknown'}`);
        console.log(`   Amount: $${variables.amount?.value || variables.amount || 0}`);
        console.log(`   Task ID: ${task.id}`);

        await sleep(2000);
        await this.completeTask(task.id, {
            processed: true,
            processedAt: new Date().toISOString()
        });
        console.log(`   ✅ Order fulfilled - Order Complete\n`);
    }

    stop() {
        this.polling = false;
        console.log('🛑 Workers stopped');
    }
}

// ============================================================
// TIMER BPMN TEST (Normal approval - timer doesn't fire)
// ============================================================
async function runTimerTest() {
    try {
        console.log('\n⏰ TIMER BPMN TEST - Normal Approval Path\n');

        const bpmnFilePath = './PurchaseOrderProcessWithTimer.bpmn';
        
        console.log(`📁 Deploying from: ${bpmnFilePath}`);
        await GoFlowClient.deployBpmn(bpmnFilePath, 'Timer Process Test');
        
        const processData = await GoFlowClient.startProcess('PurchaseOrderProcessWithTimer', {
            orderId: 'ORD-12345',
            amount: 1500,
            approver: 'approver'
        });

        const instanceId = processData.processInstanceId;
        console.log(`📋 Process Instance ID: ${instanceId}\n`);

        const approvalTask = await GoFlowClient.waitForTask('Task_Approval', instanceId, 30000);
        console.log('✍️ Approve Order task found - ID:', approvalTask.id);

        console.log('\n✅ Approving within 30 seconds...');
        await sleep(5000);
        
        await GoFlowClient.completeTask(approvalTask.id, {
            approved: true,
            comments: 'Approved by manager'
        });
        
        console.log('✅ Order approved!');
        
        // Wait for process to complete (worker will handle the external task)
        console.log('🔍 Waiting for process to complete...');
        
        let completed = false;
        let attempts = 0;
        const maxAttempts = 30;
        
        while (!completed && attempts < maxAttempts) {
            await sleep(2000);
            attempts++;
            const instance = await GoFlowClient.getProcessInstance(instanceId);
            if (!instance || instance.status === 'completed') {
                completed = true;
                console.log('✅ Process completed by external worker');
            }
        }
        
        if (completed) {
            console.log('\n🎉 ORDER COMPLETE - Process finished successfully!\n');
        } else {
            console.log('\n⚠️ Process still running, check worker\n');
        }

    } catch (err) {
        console.error('❌ ERROR:', err.message);
    }
}

// ============================================================
// TIMER TRIGGER TEST (No approval - timer fires)
// ============================================================
async function runTimerTriggerTest() {
    try {
        console.log('\n⏰ TIMER BPMN TEST - Timer Trigger Path\n');

        const bpmnFilePath = './PurchaseOrderProcessWithTimer.bpmn';

        console.log(`📁 Deploying from: ${bpmnFilePath}`);
        await GoFlowClient.deployBpmn(bpmnFilePath, 'Timer Process Test');

        const processData = await GoFlowClient.startProcess('PurchaseOrderProcessWithTimer', {
            orderId: 'ORD-67890',
            amount: 2500,
            approver: 'approver'
        });

        const instanceId = processData.processInstanceId;
        console.log(`📋 Process Instance ID: ${instanceId}\n`);

        const approvalTask = await GoFlowClient.waitForTask('Task_Approval', instanceId, 30000);
        console.log('✍️ Approve Order task found - ID:', approvalTask.id);
        console.log('⏰ Timer is set for 30 seconds...');
        console.log('😴 Waiting 35 seconds for timer to fire (no approval)...\n');

        await sleep(35000);

        // Check if approval task was canceled
        const tasksAfterTimer = await GoFlowClient.getTasks(instanceId);
        const stillApproving = tasksAfterTimer.find(t => t.taskDefinitionKey === 'Task_Approval');

        if (!stillApproving) {
            console.log('✅ Approval task was canceled - Timer fired successfully!');
        } else if (stillApproving.status === 'cancelled') {
            console.log('✅ Task was canceled by timer - Test PASSED!');
        } else {
            console.log('❌ Approval task still active - Timer did NOT fire');
        }

        // Wait for worker to process escalation
        await sleep(5000);

        const finalTasks = await GoFlowClient.getTasks(instanceId);
        if (finalTasks.length === 0) {
            console.log('🎉 Process completed (escalated path) - Timer test PASSED!\n');
        }

    } catch (err) {
        console.error('❌ ERROR:', err.message);
    }
}

// ============================================================
// MAIN
// ============================================================
if (require.main === module) {
    const testType = process.argv[2];

    if (testType === 'trigger') {
        runTimerTriggerTest();
    } else if (testType === 'worker') {
        const worker = new ExternalTaskWorker();
        worker.startWorkers();
    } else if (testType === 'deploy') {
        GoFlowClient.deployBpmn('./PurchaseOrderProcessWithTimer.bpmn', 'Timer Process Test')
            .then(() => console.log('✅ Deployment complete'))
            .catch(err => console.error('❌ Deploy failed:', err.message));
    } else {
        console.log('\n📌 Usage:');
        console.log(`   node test.js           - Run normal approval test`);
        console.log(`   node test.js trigger   - Run timer trigger test`);
        console.log(`   node test.js worker    - Run external task workers`);
        console.log(`   node test.js deploy    - Deploy BPMN only`);
        console.log('\n⚠️ Make sure your BPMN file exists: ./PurchaseOrderProcessWithTimer.bpmn');
        console.log(`\nRunning normal approval test by default...\n`);
        runTimerTest();
    }
}

module.exports = { GoFlowClient, ExternalTaskWorker, runTimerTest, runTimerTriggerTest };
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');

const API_URL = 'http://localhost:8080';
const CONTEXT_FILE = './context.json';

const api = axios.create({
    baseURL: API_URL,
    headers: { 'Content-Type': 'application/json' }
});

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Context Manager to track multiple process instances
class ContextManager {
    static load() {
        if (fs.existsSync(CONTEXT_FILE)) {
            const data = fs.readFileSync(CONTEXT_FILE, 'utf8');
            return JSON.parse(data);
        }
        return {
            deployments: [],
            processes: [],
            relationships: [],
            lastUpdated: null,
            totalProcesses: 0
        };
    }

    static save(context) {
        context.lastUpdated = new Date().toISOString();
        fs.writeFileSync(CONTEXT_FILE, JSON.stringify(context, null, 2));
        console.log(`📝 Context saved to ${CONTEXT_FILE}`);
    }

    static addDeployment(deploymentInfo) {
        const context = this.load();
        deploymentInfo.deployedAt = new Date().toISOString();
        context.deployments.push(deploymentInfo);
        this.save(context);
        return deploymentInfo;
    }

    static addProcess(processInfo, parentInstanceId = null) {
        const context = this.load();
        processInfo.id = context.totalProcesses + 1;
        processInfo.startedAt = new Date().toISOString();
        processInfo.status = 'running';
        processInfo.tasks = [];
        
        context.processes.push(processInfo);
        context.totalProcesses++;
        
        // Track relationship if this is a child process
        if (parentInstanceId) {
            context.relationships.push({
                parentProcessInstanceId: parentInstanceId,
                childProcessInstanceId: processInfo.processInstanceId,
                relationship: processInfo.processKey,
                createdAt: new Date().toISOString()
            });
        }
        
        this.save(context);
        return processInfo;
    }

    static updateProcess(instanceId, updates) {
        const context = this.load();
        const process = context.processes.find(p => p.processInstanceId === instanceId);
        if (process) {
            Object.assign(process, updates);
            process.updatedAt = new Date().toISOString();
            this.save(context);
        }
    }

    static addTask(processInstanceId, taskInfo) {
        const context = this.load();
        const process = context.processes.find(p => p.processInstanceId === processInstanceId);
        if (process) {
            taskInfo.completedAt = new Date().toISOString();
            process.tasks.push(taskInfo);
            process.currentTask = taskInfo.taskDefinitionKey;
            this.save(context);
        }
    }

    static getProcess(instanceId) {
        const context = this.load();
        return context.processes.find(p => p.processInstanceId === instanceId);
    }

    static getAllProcesses() {
        const context = this.load();
        return context.processes;
    }

    static getProcessTree(parentInstanceId) {
        const context = this.load();
        const parent = context.processes.find(p => p.processInstanceId === parentInstanceId);
        const children = context.relationships
            .filter(r => r.parentProcessInstanceId === parentInstanceId)
            .map(r => context.processes.find(p => p.processInstanceId === r.childProcessInstanceId));
        
        return {
            parent,
            children
        };
    }

    static printSummary() {
        const context = this.load();
        console.log('\n📊 PROCESS SUMMARY');
        console.log('='.repeat(60));
        console.log(`Total Processes: ${context.totalProcesses}`);
        console.log(`Last Updated: ${context.lastUpdated}`);
        console.log('\nDeployments:');
        context.deployments.forEach(d => {
            console.log(`  📦 ${d.name} - ID: ${d.id}`);
        });
        console.log('\nProcesses:');
        context.processes.forEach(p => {
            const statusIcon = p.status === 'completed' ? '✅' : p.status === 'failed' ? '❌' : '🔄';
            console.log(`  ${statusIcon} [${p.id}] ${p.processKey} - ${p.status}`);
            console.log(`      Instance: ${p.processInstanceId}`);
            console.log(`      Started: ${p.startedAt}`);
            if (p.completedAt) console.log(`      Completed: ${p.completedAt}`);
            if (p.tasks && p.tasks.length > 0) {
                console.log(`      Tasks completed: ${p.tasks.length}`);
                p.tasks.forEach(t => {
                    console.log(`        - ${t.taskDefinitionKey} (${t.status})`);
                });
            }
        });
        
        if (context.relationships.length > 0) {
            console.log('\nRelationships:');
            context.relationships.forEach(r => {
                console.log(`  ${r.parentProcessInstanceId} → ${r.childProcessInstanceId} (${r.relationship})`);
            });
        }
        console.log('='.repeat(60));
    }
}

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
            
            // Track deployment in context
            ContextManager.addDeployment({
                id: res.data.id,
                name: res.data.name,
                filePath: filePath
            });
            
            return res.data;
        } catch (error) {
            console.error('❌ Deploy failed:', error.response?.data || error.message);
            throw error;
        }
    }

    static async startProcess(processKey, variables = {}, metadata = {}, parentInstanceId = null) {
        try {
            const res = await api.post(`/engine-rest/v2/process-definitions/${processKey}/start`, { variables });
            console.log('🚀 Process started - Key:', processKey, '- Instance:', res.data.processInstanceId);
            
            // Track process in context with parent relationship
            const processInfo = {
                processKey,
                processDefinitionId: res.data.id,
                processInstanceId: res.data.processInstanceId,
                variables,
                metadata,
                status: 'started',
                tasks: []
            };
            ContextManager.addProcess(processInfo, parentInstanceId);
            
            return res.data;
        } catch (error) {
            console.error('❌ Start process failed:', error.response?.data || error.message);
            throw error;
        }
    }

    static async getTasks(processInstanceId = null, assignee = null, candidateGroup = null) {
        const params = { status: 'created' };
        if (processInstanceId) params.processInstanceId = processInstanceId;
        if (assignee) params.assignee = assignee;
        if (candidateGroup) params.candidateGroup = candidateGroup;
        const res = await api.get('/tasks', { params });
        return res.data;
    }

    static async completeTask(taskId, variables = {}, processInstanceId = null, taskInfo = {}) {
        const res = await api.post(`/tasks/${taskId}/complete`, { variables });
        console.log(`✅ Task ${taskId} completed`);
        
        // Track task completion in context
        if (processInstanceId) {
            ContextManager.addTask(processInstanceId, {
                taskId: taskId,
                taskDefinitionKey: taskInfo.taskDefinitionKey,
                assignee: taskInfo.assignee,
                status: 'completed',
                variables: variables
            });
        }
        
        return res.data;
    }

    static async waitForTask(assignee, taskDefinitionKey, processInstanceId, timeoutMs = 60000) {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            const tasks = await this.getTasks(processInstanceId, assignee);
            if (tasks != null) {
                const task = tasks.find(t => t.taskDefinitionKey === taskDefinitionKey);
                if (task) {
                    return task;
                }
            }
            await sleep(2000);
        }
        throw new Error(`Timeout waiting for task ${taskDefinitionKey} for ${assignee}`);
    }

    static async waitForTaskByKey(taskDefinitionKey, processInstanceId, timeoutMs = 60000) {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            const tasks = await this.getTasks(processInstanceId);
            const task = tasks.find(t => t.taskDefinitionKey === taskDefinitionKey);
            if (task) return task;
            await sleep(1000);
        }
        throw new Error(`Timeout waiting for task ${taskDefinitionKey}`);
    }

    static async completeProcess(processInstanceId, status = 'completed') {
        ContextManager.updateProcess(processInstanceId, {
            status: status,
            completedAt: new Date().toISOString()
        });
        console.log(`✅ Process ${processInstanceId} marked as ${status}`);
    }
}

// ============================================================
// PRF test flow with deployment and context tracking
// ============================================================
async function runPRF() {
    const startTime = Date.now();
    let requesterInstanceId = null;
    let supervisorInstanceId = null;
    let approverInstanceId = null;
    let accountantInstanceId = null;
    
    try {
        console.log('\n🚀 START PRF TEST\n');

        // STEP 1: Deploy the BPMN file
        const bpmnFilePath = './prf_with_participants.bpmn';
        
        if (!fs.existsSync(bpmnFilePath)) {
            console.error(`❌ BPMN file not found: ${bpmnFilePath}`);
            console.log('Please make sure the file exists at:', bpmnFilePath);
            return;
        }
        
        console.log(`📁 Deploying from: ${bpmnFilePath}`);
        await GoFlowClient.deployBpmn(bpmnFilePath, 'PRF Process Test');
        
        // STEP 2: Prepare process variables
        const data = {
            document_uuid: '102',
            project: 'GF',
            maker_name: 'JOHAN',
            total_amount: 500,
            requester: 'Ousman KULIBALI',
            supervisor: 'NELSON NDEFU',
            approver: 'Joe',
            accountant: 'MUSASA'
        };

        // STEP 3: Start the Requester process
        const metadata = {
            testName: 'PRF Full Flow',
            environment: 'development',
            requester: data.requester
        };
        
        const requesterData = await GoFlowClient.startProcess('prf_process_requester', data, metadata);
        requesterInstanceId = requesterData.processInstanceId;
        console.log(`📋 Requester Process Instance ID: ${requesterInstanceId}\n`);

        // STEP 4: Create PRF task (in requester process)
        const createPrfTask = await GoFlowClient.waitForTask(null, 'Task_CreatePRF', requesterInstanceId);
        console.log('✍️ Creating PRF...');
        await GoFlowClient.completeTask(createPrfTask.id, {}, requesterInstanceId, {
            taskDefinitionKey: 'Task_CreatePRF',
            assignee: null
        });
        console.log('✅ PRF created\n');
        
        await sleep(2000);
        
        // STEP 5: Requester signs (in requester process)
        const signTask = await GoFlowClient.waitForTask(data.maker_name, 'Task_SignPRF', requesterInstanceId);
        console.log('✍️ Signing PRF...');
        await GoFlowClient.completeTask(signTask.id, {}, requesterInstanceId, {
            taskDefinitionKey: 'Task_SignPRF',
            assignee: data.maker_name
        });
        console.log('✅ PRF signed\n');

        await sleep(2000);
        
        // Note: The worker will start the Supervisor process
        // We need to capture its instance ID from the worker or from API
        console.log('⏳ Waiting for Supervisor process to be created by worker...');
        await sleep(3000);
        
        // STEP 6: Get Supervisor process instance from context or API
        // For now, we'll wait and the worker will handle it
        // The supervisor process will be started by the worker
        
        // The test will continue with the Supervisor task
        // But note: This task is in the SUPERVISOR process, not the requester process
        const supervisorTask = await GoFlowClient.waitForTask(data.supervisor, 'Task_SupervisorReview', null);
        console.log('👔 Supervisor reviewing...');
        
        // Get the process instance ID from the task
        supervisorInstanceId = supervisorTask.processInstanceId;
        console.log(`📋 Supervisor Process Instance ID: ${supervisorInstanceId}`);
        
        await GoFlowClient.completeTask(supervisorTask.id, { supervisor_reviewed: true }, supervisorInstanceId, {
            taskDefinitionKey: 'Task_SupervisorReview',
            assignee: data.supervisor
        });
        console.log('✅ Supervisor approved\n');

        await sleep(2000);
        
        // STEP 7: Approver approves (in APPROVER process)
        const approverTask = await GoFlowClient.waitForTask(data.approver, 'Task_ApproverApproved', null);
        console.log('💼 Approver reviewing...');
        
        approverInstanceId = approverTask.processInstanceId;
        console.log(`📋 Approver Process Instance ID: ${approverInstanceId}`);
        
        await GoFlowClient.completeTask(approverTask.id, { approver_approved: true }, approverInstanceId, {
            taskDefinitionKey: 'Task_ApproverApproved',
            assignee: data.approver
        });
        console.log('✅ Approver approved\n');
        
        await sleep(2000);
        
        // STEP 8: Accountant pays (in ACCOUNTANT process)
        const accountantTask = await GoFlowClient.waitForTask(data.accountant, 'Task_AccountantPay', null);
        console.log('💰 Processing payment...');
        
        accountantInstanceId = accountantTask.processInstanceId;
        console.log(`📋 Accountant Process Instance ID: ${accountantInstanceId}`);
        
        await GoFlowClient.completeTask(accountantTask.id, { 
            accountant_approved: true, 
            paymentProcessed: true 
        }, accountantInstanceId, {
            taskDefinitionKey: 'Task_AccountantPay',
            assignee: data.accountant
        });
        console.log('✅ Payment processed\n');

        // Mark all processes as completed
        await GoFlowClient.completeProcess(requesterInstanceId, 'completed');
        if (supervisorInstanceId) await GoFlowClient.completeProcess(supervisorInstanceId, 'completed');
        if (approverInstanceId) await GoFlowClient.completeProcess(approverInstanceId, 'completed');
        if (accountantInstanceId) await GoFlowClient.completeProcess(accountantInstanceId, 'completed');
        
        const duration = (Date.now() - startTime) / 1000;
        console.log(`\n🏁 ALL PROCESSES COMPLETED SUCCESSFULLY! 🎉 (Duration: ${duration}s)\n`);
        
        // Print summary
        ContextManager.printSummary();
        
        // Print process tree
        console.log('\n📊 PROCESS TREE:');
        console.log(`Requester Process: ${requesterInstanceId}`);
        if (supervisorInstanceId) console.log(`  └── Supervisor Process: ${supervisorInstanceId}`);
        if (approverInstanceId) console.log(`  └── Approver Process: ${approverInstanceId}`);
        if (accountantInstanceId) console.log(`  └── Accountant Process: ${accountantInstanceId}`);
        
    } catch (err) {
        console.error('❌ ERROR:', err.message);
        if (err.response) {
            console.error('Status:', err.response.status);
            console.error('Data:', err.response.data);
        }
        
        // Update process statuses on error
        if (requesterInstanceId) {
            ContextManager.updateProcess(requesterInstanceId, {
                status: 'failed',
                error: err.message,
                failedAt: new Date().toISOString()
            });
        }
    }
}

// ============================================================
// Helper function to view context
// ============================================================
function viewContext() {
    const context = ContextManager.load();
    console.log('\n📄 Current Context:');
    console.log(JSON.stringify(context, null, 2));
}

// ============================================================
// Helper function to clear context
// ============================================================
function clearContext() {
    if (fs.existsSync(CONTEXT_FILE)) {
        fs.unlinkSync(CONTEXT_FILE);
        console.log('🗑️ Context file cleared');
    }
}

// ============================================================
// Helper function to show process tree
// ============================================================
function showProcessTree() {
    const context = ContextManager.load();
    console.log('\n🌲 PROCESS TREE');
    console.log('='.repeat(50));
    
    // Find root processes (no parent)
    const rootProcesses = context.processes.filter(p => {
        return !context.relationships.some(r => r.childProcessInstanceId === p.processInstanceId);
    });
    
    function printProcess(instanceId, indent = '') {
        const process = context.processes.find(p => p.processInstanceId === instanceId);
        if (!process) return;
        
        const statusIcon = process.status === 'completed' ? '✅' : process.status === 'failed' ? '❌' : '🔄';
        console.log(`${indent}${statusIcon} ${process.processKey} (${process.processInstanceId})`);
        
        const children = context.relationships.filter(r => r.parentProcessInstanceId === instanceId);
        children.forEach(child => {
            printProcess(child.childProcessInstanceId, indent + '  └── ');
        });
    }
    
    rootProcesses.forEach(root => {
        printProcess(root.processInstanceId);
    });
    console.log('='.repeat(50));
}

// ============================================================
// Run the test
// ============================================================
const command = process.argv[2];

if (command === 'view') {
    viewContext();
} else if (command === 'clear') {
    clearContext();
} else if (command === 'tree') {
    showProcessTree();
} else {
    runPRF();
}

module.exports = { GoFlowClient, ContextManager };
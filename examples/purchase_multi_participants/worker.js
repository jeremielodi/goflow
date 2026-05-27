const { Client, logger } = require('camunda-external-task-client-js');
const axios = require('axios');
const fs = require('fs');

const CONTEXT_FILE = './context.json';

// Configuration for your goflow engine
const config = {
  baseUrl: 'http://localhost:8080/engine-rest',
  use: logger,
  asyncResponseTimeout: 30000,
};

const client = new Client(config);

// Context Manager to track process instances
class ContextManager {
  static load() {
    if (fs.existsSync(CONTEXT_FILE)) {
      try {
        const data = fs.readFileSync(CONTEXT_FILE, 'utf8');
        return JSON.parse(data);
      } catch (e) {
        console.error('Error parsing context file:', e.message);
      }
    }
    // Initialize with default structure
    return {
      deployments: [],
      processes: [],
      relationships: [],
      workerTasks: [],
      lastUpdated: null,
      totalProcesses: 0
    };
  }

  static save(context) {
    try {
      context.lastUpdated = new Date().toISOString();
      fs.writeFileSync(CONTEXT_FILE, JSON.stringify(context, null, 2));
    } catch (e) {
      console.error('Error saving context file:', e.message);
    }
  }

  static addProcess(processInfo, parentInstanceId = null) {
    const context = this.load();
    
    // Ensure arrays exist
    if (!context.processes) context.processes = [];
    if (!context.relationships) context.relationships = [];
    if (context.totalProcesses === undefined) context.totalProcesses = 0;
    
    processInfo.id = context.totalProcesses + 1;
    processInfo.startedAt = new Date().toISOString();
    processInfo.status = 'started';
    processInfo.tasks = [];
    
    context.processes.push(processInfo);
    context.totalProcesses++;
    
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
    if (!context.processes) context.processes = [];
    
    const process = context.processes.find(p => p.processInstanceId === instanceId);
    if (process) {
      Object.assign(process, updates);
      process.updatedAt = new Date().toISOString();
      this.save(context);
    }
  }

  static addWorkerTask(taskInfo) {
    const context = this.load();
    if (!context.workerTasks) context.workerTasks = [];
    
    taskInfo.processedAt = new Date().toISOString();
    context.workerTasks.push(taskInfo);
    this.save(context);
  }

  static getProcess(instanceId) {
    const context = this.load();
    if (!context.processes) return null;
    return context.processes.find(p => p.processInstanceId === instanceId);
  }
}

console.log('🚀 External task workers starting...');
console.log('📡 Connecting to: http://localhost:8080/engine-rest');
console.log('');

// ------------------------------------------------------------
// Worker 1: Notify Supervisor (topic: prf_notify_supervisor)
// ------------------------------------------------------------
client.subscribe('prf_notify_supervisor', async ({ task, taskService }) => {
  console.log(`\n📧 [WORKER] Notifying supervisor...`);
  console.log(`   Task ID: ${task.id}`);
  
  try {
    const vars = task.variables.getAll();
    console.log(`   Requester: ${vars.requester || vars.maker_name || 'Unknown'}`);
    console.log(`   Project: ${vars.project || 'Unknown'}`);
    console.log(`   Total Amount: $${vars.total_amount || 0}`);
    console.log(`   Supervisor: ${vars.supervisor || 'Unknown'}`);

    // Track the requester process instance (parent)
    const requesterInstanceId = task.processInstanceId;
    console.log(`   📋 Requester Process Instance: ${requesterInstanceId}`);
    
    // Update requester process status if it exists
    const existingProcess = ContextManager.getProcess(requesterInstanceId);
    if (existingProcess) {
      ContextManager.updateProcess(requesterInstanceId, {
        status: 'notifying_supervisor',
        lastWorkerTask: 'prf_notify_supervisor'
      });
    } else {
      // Add requester process if not tracked yet
      ContextManager.addProcess({
        processKey: 'prf_process_requester',
        processInstanceId: requesterInstanceId,
        variables: vars,
        source: 'worker'
      });
    }

    // Simulate notification work
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Complete the current job
    await taskService.complete(task, {
      variables: { supervisor_notified: true }
    });
    console.log('   ✅ Supervisor notification sent, starting supervisor process...');

    // Track worker task completion
    ContextManager.addWorkerTask({
      topic: 'prf_notify_supervisor',
      taskId: task.id,
      processInstanceId: requesterInstanceId,
      status: 'completed',
      variables: vars
    });

    // Manually start the supervisor process
    const startResponse = await axios.post(
      config.baseUrl + '/v2/process-definitions/prf_process_supervisor/start',
      { variables: vars }
    );
    
    const supervisorInstanceId = startResponse.data.processInstanceId;
    console.log(`   ✅ Supervisor process started with instance ID: ${supervisorInstanceId}`);
    
    // Track the supervisor process as a child
    ContextManager.addProcess({
      processKey: 'prf_process_supervisor',
      processDefinitionId: startResponse.data.id,
      processInstanceId: supervisorInstanceId,
      variables: vars,
      startedBy: 'worker',
      parentTaskId: task.id
    }, requesterInstanceId);
    
    ContextManager.updateProcess(supervisorInstanceId, {
      status: 'running',
      supervisor: vars.supervisor
    });

  } catch (err) {
    console.error('   ❌ Failed to complete task or start supervisor process:', err.message);
    if (err.response) {
      console.error('   Status:', err.response.status);
      console.error('   Data:', err.response.data);
    }
    
    ContextManager.addWorkerTask({
      topic: 'prf_notify_supervisor',
      taskId: task.id,
      processInstanceId: task.processInstanceId,
      status: 'failed',
      error: err.message
    });
  }
});

// ------------------------------------------------------------
// Worker 2: Notify Approver (topic: prf_notify_approver)
// ------------------------------------------------------------
client.subscribe('prf_notify_approver', async ({ task, taskService }) => {
  console.log(`\n👨‍💼 [WORKER] Notifying approver...`);
  
  const vars = task.variables.getAll();
  
  try {
    console.log(`   Requester: ${vars.requester || 'Unknown'}`);
    console.log(`   Amount: $${vars.total_amount || 0}`);
    console.log(`   Approver: ${vars.approver || 'Unknown'}`);
    
    // Get parent process instance
    const parentInstanceId = task.processInstanceId;
    console.log(`   📋 Parent Process Instance: ${parentInstanceId}`);
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    await taskService.complete(task, { variables: { approver_notified: true } });
    console.log('   ✅ Approver notified, starting approver process...');

    ContextManager.addWorkerTask({
      topic: 'prf_notify_approver',
      taskId: task.id,
      processInstanceId: parentInstanceId,
      status: 'completed',
      variables: vars
    });

    // Start the approver process
    const startResponse = await axios.post(
      config.baseUrl + '/v2/process-definitions/prf_process_approver/start',
      { variables: vars }
    );
    
    const approverInstanceId = startResponse.data.processInstanceId;
    console.log(`   ✅ Approver process started with instance ID: ${approverInstanceId}`);
    
    // Track the approver process as a child
    ContextManager.addProcess({
      processKey: 'prf_process_approver',
      processDefinitionId: startResponse.data.id,
      processInstanceId: approverInstanceId,
      variables: vars,
      startedBy: 'worker',
      parentTaskId: task.id
    }, parentInstanceId);
    
    ContextManager.updateProcess(approverInstanceId, {
      status: 'running',
      approver: vars.approver
    });

  } catch (err) {
    console.error('   ❌ Failed:', err.message);
    ContextManager.addWorkerTask({
      topic: 'prf_notify_approver',
      taskId: task.id,
      processInstanceId: task.processInstanceId,
      status: 'failed',
      error: err.message
    });
  }
});

// ------------------------------------------------------------
// Worker 3: Notify Accountant (topic: prf_notify_accountant)
// ------------------------------------------------------------
client.subscribe('prf_notify_accountant', async ({ task, taskService }) => {
  console.log(`\n🧾 [WORKER] Notifying accountant...`);
  
  const vars = task.variables.getAll();
  
  try {
    console.log(`   Requester: ${vars.requester || 'Unknown'}`);
    console.log(`   Amount: $${vars.total_amount || 0}`);
    console.log(`   Accountant: ${vars.accountant || 'Unknown'}`);
    
    const parentInstanceId = task.processInstanceId;
    console.log(`   📋 Parent Process Instance: ${parentInstanceId}`);
    
    await new Promise(resolve => setTimeout(resolve, 1000));
    await taskService.complete(task, { variables: { accountant_notified: true } });
    console.log('   ✅ Accountant notified, starting accountant process...');

    ContextManager.addWorkerTask({
      topic: 'prf_notify_accountant',
      taskId: task.id,
      processInstanceId: parentInstanceId,
      status: 'completed',
      variables: vars
    });

    const startResponse = await axios.post(
      config.baseUrl + '/v2/process-definitions/prf_process_accountant/start',
      { variables: vars }
    );
    
    const accountantInstanceId = startResponse.data.processInstanceId;
    console.log(`   ✅ Accountant process started with instance ID: ${accountantInstanceId}`);
    
    // Track the accountant process as a child
    ContextManager.addProcess({
      processKey: 'prf_process_accountant',
      processDefinitionId: startResponse.data.id,
      processInstanceId: accountantInstanceId,
      variables: vars,
      startedBy: 'worker',
      parentTaskId: task.id
    }, parentInstanceId);
    
    ContextManager.updateProcess(accountantInstanceId, {
      status: 'running',
      accountant: vars.accountant
    });

  } catch (err) {
    console.error('   ❌ Failed:', err.message);
    ContextManager.addWorkerTask({
      topic: 'prf_notify_accountant',
      taskId: task.id,
      processInstanceId: task.processInstanceId,
      status: 'failed',
      error: err.message
    });
  }
});

// ------------------------------------------------------------
// Worker 4: Process Payment (topic: prf_process_payment)
// ------------------------------------------------------------
client.subscribe('prf_process_payment', async ({ task, taskService }) => {
  console.log(`\n💳 [WORKER] Processing payment...`);
  
  const vars = task.variables.getAll();
  
  try {
    console.log(`   Amount: $${vars.total_amount || 0}`);
    console.log(`   Accountant: ${vars.accountant || 'Unknown'}`);
    console.log(`   Task ID: ${task.id}`);
    
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    await taskService.complete(task, {
      variables: { 
        payment_processed: true,
        transaction_id: `TXN-${Date.now()}`
      }
    });
    
    console.log('   ✅ Payment processed successfully');
    
    // Mark the process as completed
    const processInstanceId = task.processInstanceId;
    ContextManager.updateProcess(processInstanceId, {
      status: 'completed',
      completedAt: new Date().toISOString(),
      transactionId: `TXN-${Date.now()}`
    });
    
    ContextManager.addWorkerTask({
      topic: 'prf_process_payment',
      taskId: task.id,
      processInstanceId: processInstanceId,
      status: 'completed',
      variables: vars
    });

  } catch (err) {
    console.error('   ❌ Payment failed:', err.message);
    ContextManager.addWorkerTask({
      topic: 'prf_process_payment',
      taskId: task.id,
      processInstanceId: task.processInstanceId,
      status: 'failed',
      error: err.message
    });
  }
});

// ------------------------------------------------------------
// Utility function to print summary on exit
// ------------------------------------------------------------
function printSummary() {
  const context = ContextManager.load();
  console.log('\n📊 WORKER SUMMARY');
  console.log('='.repeat(50));
  console.log(`Total Processes Tracked: ${context.totalProcesses || 0}`);
  console.log(`Worker Tasks Processed: ${(context.workerTasks || []).length}`);
  console.log('\nProcesses:');
  (context.processes || []).forEach(p => {
    const statusIcon = p.status === 'completed' ? '✅' : p.status === 'failed' ? '❌' : '🔄';
    console.log(`  ${statusIcon} ${p.processKey}: ${p.processInstanceId} (${p.status})`);
  });
  console.log('='.repeat(50));
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  printSummary();
  console.log('\n🛑 Worker shutting down...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  printSummary();
  console.log('\n🛑 Worker shutting down...');
  process.exit(0);
});

console.log('✅ External task workers started and waiting for tasks...\n');
console.log('Subscribed topics:');
console.log('   - prf_notify_supervisor');
console.log('   - prf_notify_approver');
console.log('   - prf_notify_accountant');
console.log('   - prf_process_payment');
console.log('\n⏳ Waiting for external tasks from goflow engine...\n');
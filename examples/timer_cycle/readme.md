How to run:
Terminal 1 - Start the external worker:
bash
node test-timer-cycle.js worker
Terminal 2 - Run the timer cycle test:
bash
node test-timer-cycle.js cycle
Expected Output for Timer Cycle Test:
text
🧪 TEST: Timer Cycle (R3/PT10S)
============================================================

✅ Deployed: Timer Cycle Test - ID: xxx
🚀 Process started - Instance: xxx
📋 Process Instance ID: xxx

📊 Observation 1:
   Active Timers: 1
   Tasks: Waiting for Timer Cycles
   🔔 Cycle 1 triggered!
   ✅ Cycle 1/3 completed

📊 Observation 2:
   Active Timers: 1
   Tasks: Waiting for Timer Cycles
   🔔 Cycle 2 triggered!
   ✅ Cycle 2/3 completed

📊 Observation 3:
   Active Timers: 1
   Tasks: Waiting for Timer Cycles
   🔔 Cycle 3 triggered!
   ✅ Cycle 3/3 completed

📊 Observation 4:
   Active Timers: 0
   Tasks: none

🎉 Process completed after 3 cycles!

📊 TEST SUMMARY
============================================================
Expected cycles: 3
Actual cycles triggered: 3

✅ TEST PASSED: Timer cycle executed correctly!
============================================================
This test validates:

✅ Timer cycle creation (R3/PT10S)

✅ Multiple cycle repetitions

✅ Automatic rescheduling of next cycles

✅ Process completion after all cycles
Run the test:
bash

node test-sequential.js 











testing server performances:

How to Run:
bash
# Make sure your GoFlow server is running
cd /d/apps/camunda-like
go run main.go

# In another terminal, run the load test
cd examples/multi_instance_sequential
node load-test.js simple      # Simple test
node load-test.js parallel    # Concurrent requests test
node load-test.js stress      # Stress test (increasing load)
node load-test.js duration    # Sustained load test
node load-test.js pool        # Connection pool test
node load-test.js all         # Run all tests
Expected Output:
text
============================================================
🚀 GOFLOW LOAD TEST SUITE
============================================================
Server: http://localhost:8080
CPU Cores: 8
Total Memory: 16.00 GB
============================================================

🏥 Checking server health...
✅ Server is healthy

🔧 Running simple performance test...
   Duration: 30s
   Concurrent requests: 1

============================================================
📊 TEST RESULTS: Simple Performance Test
============================================================
Duration:           30.02s
Total Requests:     285
Successful:         285
Failed:             0
Success Rate:       100.00%
Requests/sec:       9.49

Response Times (ms):
  Min:    45
  Max:    234
  Avg:    102.34
  Median: 98
  P95:    156
  P99:    201
============================================================









Worker pool test:


3. Run the worker pool tests:
bash
# Test single async request
node test-worker-pool.js async

# Test concurrent async requests (50 at once)
node test-worker-pool.js concurrent

# Test queue overflow (1100 requests)
node test-worker-pool.js overflow

# Compare sync vs async response times
node test-worker-pool.js compare

# Monitor pool metrics over time
node test-worker-pool.js monitor

# Run all tests
node test-worker-pool.js all
Expected output for async test:
text
============================================================
🧪 TESTING ASYNC WORKER POOL ENDPOINT
============================================================

📤 Sending async process start request...
✅ Response received in 15ms
   Status: 202
   Request ID: abc-123-def
   Queue size: 1
   Status: queued

📊 Worker Pool Metrics:
   Jobs Submitted: 1
   Jobs Processed: 0
   Queue Length: 1
   Active Workers: 10
   Healthy: true
Expected output for compare test:
text
⚡ COMPARING SYNC VS ASYNC
============================================================

📤 Testing SYNC endpoint...
📤 Testing ASYNC endpoint...

📊 Results:
   SYNC endpoint avg response:  342.50ms
   ASYNC endpoint avg response: 18.30ms
   Improvement: 94.66% faster

✅ ASYNC endpoint is significantly faster for client!
The async endpoint should return in ~10-20ms vs ~300-400ms for sync, because it just queues the request and returns immediately!
# Zorp Stress Tester (Chaos Edition)
# "Chaos is a friend of mine." - Bob Dylan (and probably Zorp)

import requests
import json
import threading
import time
import os
import random
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()

# --- CONFIGURATION ---
ZORP_SECRET_KEY = os.getenv("ZORP_SECRET_KEY")
if not ZORP_SECRET_KEY:
    print("❌ Lỗi: Không tìm thấy ZORP_SECRET_KEY. Hãy kiểm tra file .env!")
    exit(1)

BEARER_TOKEN = f"Bearer {ZORP_SECRET_KEY}"
ZORP_URL = "http://127.0.0.1:3000"
WEBHOOK_PORT = 9090
HOST_IP = "127.0.0.1" 
WEBHOOK_URL = f"http://{HOST_IP}:{WEBHOOK_PORT}"

# Tăng áp lực lên một chút
NUM_CONCURRENT_JOBS = 50
# Giới hạn thời gian chờ tối đa cho toàn bộ test
MAX_WAIT_TIME = 30 

# Thiết lập proxy
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

# Lưu trữ kết quả
results = {
    "dispatched": 0,
    "callbacks_received": 0,
    "success_ok": 0,    # Expected success, Actual success
    "fail_ok": 0,       # Expected fail, Actual fail
    "mismatch": 0,      # Unexpected status
    "details": []
}
lock = threading.Lock()

# --- JOB SCENARIOS ---
def generate_scenario(index):
    """Tạo ra các kịch bản test ngẫu nhiên"""
    scenarios = [
        {
            "type": "NORMAL",
            "image": "busybox:latest",
            "commands": ["sh", "-c", f"echo 'Job {index} normal' && sleep 1"],
            "expect_status": "FINISHED",
            "expect_exit": 0,
            "weight": 40
        },
        {
            "type": "FAST",
            "image": "busybox:latest",
            "commands": ["sh", "-c", f"echo 'Job {index} flash!'"],
            "expect_status": "FINISHED",
            "expect_exit": 0,
            "weight": 20
        },
        {
            "type": "FAIL",
            "image": "busybox:latest",
            "commands": ["sh", "-c", f"echo 'Job {index} dying...' && exit 1"],
            "expect_status": "FINISHED", # Zorp vẫn finish job, nhưng exit code khác
            "expect_exit": 1,
            "weight": 20
        },
        {
            "type": "HEAVY", # Test limits params
            "image": "busybox:latest",
            "commands": ["sh", "-c", f"echo 'Checking limits...' && sleep 2"],
            "limits": {"memory_mb": 128, "cpu_cores": 0.5},
            "expect_status": "FINISHED",
            "expect_exit": 0,
            "weight": 10
        },
        {
            "type": "OOM", # Test Out-of-Memory
            "image": "python:3.9-alpine",
            "commands": ["python", "-c", "print('Allocating 20MB...'); a = bytearray(20 * 1024 * 1024); import time; time.sleep(10)"],
            "limits": {"memory_mb": 10}, # Allow only 10MB
            "expect_status": "FINISHED", # OOM kill should be a finished job
            "expect_exit": 137, # Standard exit code for OOM kill
            "weight": 10
        }
    ]
    
    # Chọn ngẫu nhiên dựa trên trọng số
    choice = random.choices(scenarios, weights=[s['weight'] for s in scenarios], k=1)[0]
    return choice

class ChaosWebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return # Im lặng là vàng

    def do_POST(self):
        try:
            # Simulate network latency
            time.sleep(random.uniform(0.5, 5.0))
            
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            job_id = data.get("job_id")
            status = data.get("status")
            exit_code = data.get("exit_code")
            duration = data.get("duration_seconds")
            
            # Phân tích kết quả
            with lock:
                results["callbacks_received"] += 1
                
                # Tìm job gốc trong danh sách dispatched
                job_info = next((item for item in results["details"] if item["id"] == job_id), None)
                
                if job_info:
                    expected_exit = job_info["expect_exit"]
                    # Logic verify
                    is_match = (status == "FINISHED" and exit_code == expected_exit) or \
                               (status == "FAILED" and expected_exit != 0) # Zorp logic tùy chỉnh

                    if is_match:
                        if expected_exit == 0: results["success_ok"] += 1
                        else: results["fail_ok"] += 1
                        icon = "✅"
                    else:
                        results["mismatch"] += 1
                        icon = "⚠️"

                    print(f"   ↪️  Callback [{job_id[:6]}] | Type: {job_info['type']:<6} | Exit: {exit_code} (Exp: {expected_exit}) | {duration:.2f}s | {icon}")
                else:
                    print(f"   ❓ Ghost Callback [{job_id[:6]}] ???")

            self.send_response(200)
            self.end_headers()
        except Exception as e:
            print(f"❌ Webhook Error: {e}")

def start_webhook_server():
    server = ThreadingHTTPServer(('0.0.0.0', WEBHOOK_PORT), ChaosWebhookHandler)
    server.serve_forever()

def dispatch_worker(i):
    # Random delay start (Jitter) để giả lập request đến không đồng đều
    time.sleep(random.uniform(0.1, 2.0))
    
    scenario = generate_scenario(i)
    
    payload = {
        "image": scenario["image"],
        "commands": scenario["commands"],
        "callback_url": WEBHOOK_URL
    }
    
    if "limits" in scenario:
        payload["limits"] = scenario["limits"]

    headers = {"Authorization": BEARER_TOKEN}
    
    try:
        resp = requests.post(f"{ZORP_URL}/dispatch", json=payload, headers=headers, timeout=5)
        if resp.status_code == 202:
            data = resp.json()
            job_id = data["job_id"]
            with lock:
                results["dispatched"] += 1
                results["details"].append({
                    "id": job_id,
                    "type": scenario["type"],
                    "expect_exit": scenario["expect_exit"]
                })
            print(f"🚀 [{i:02d}] Dispatched ({scenario['type']}) -> {job_id[:6]}")
        else:
            print(f"💥 [{i:02d}] HTTP Error: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"🔥 [{i:02d}] Connection Error: {e}")

def main():
    print(f"\n⚡ ZORP CHAOS TESTER ⚡")
    print(f"Target: {ZORP_URL} | Jobs: {NUM_CONCURRENT_JOBS}")
    print("-" * 50)

    # Start Webhook
    server_thread = threading.Thread(target=start_webhook_server, daemon=True)
    server_thread.start()
    print(f"👂 Webhook ready at {WEBHOOK_URL}")
    time.sleep(1)

    print("🚀 Giving Zorp a moment to warm up...")
    time.sleep(5)
    
    # Dispatch Threads
    threads = []
    for i in range(1, NUM_CONCURRENT_JOBS + 1):
        t = threading.Thread(target=dispatch_worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Wait for callbacks
    print(f"\n⏳ Waiting for callbacks (Max {MAX_WAIT_TIME}s)...")
    start_wait = time.time()
    while True:
        with lock:
            if results["callbacks_received"] >= results["dispatched"] and results["dispatched"] > 0:
                break
        if time.time() - start_wait > MAX_WAIT_TIME:
            print("\n⏰ Timeout waiting for callbacks!")
            break
        time.sleep(0.5)

    # Report
    print("\n" + "="*50)
    print("📊  TEST REPORT")
    print("="*50)
    print(f"Total Dispatched    : {results['dispatched']}")
    print(f"Callbacks Received  : {results['callbacks_received']}")
    print("-" * 20)
    print(f"✅ Healthy Success   : {results['success_ok']}")
    print(f"🛡️  Expected Failure  : {results['fail_ok']} (Exit code != 0 handled correctly)")
    print(f"⚠️  Mismatches        : {results['mismatch']} (Status/Exit code wrong)")
    
    success_rate = ((results['success_ok'] + results['fail_ok']) / results['dispatched'] * 100) if results['dispatched'] > 0 else 0
    print(f"\n🏆 Integrity Score: {success_rate:.1f}%")
    
    if results['mismatch'] == 0 and results['dispatched'] == results['callbacks_received']:
        print("✨ ZORP IS SOLID! ✨")
    else:
        print("💀 ZORP MIGHT BE LEAKING...")

if __name__ == "__main__":
    main()

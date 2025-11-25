# Zorp Stress Tester (Verifier Edition)
# Checks if Zorp actually ran the code by inspecting returned logs.

import requests
import json
import threading
import time
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dotenv import load_dotenv # <--- ĐÃ THÊM

# Tải biến môi trường từ file .env
load_dotenv()

# --- CONFIGURATION ---
# Lấy biến môi trường ZORP_SECRET_KEY
ZORP_SECRET_KEY = os.getenv("ZORP_SECRET_KEY")
if not ZORP_SECRET_KEY:
    print("❌ Lỗi: Không tìm thấy ZORP_SECRET_KEY. Hãy kiểm tra file .env!")
    exit(1)

BEARER_TOKEN = f"Bearer {ZORP_SECRET_KEY}" 

ZORP_URL = "http://127.0.0.1:3000"
WEBHOOK_PORT = 9090
HOST_IP = "127.0.0.1" # Hoặc IP LAN nếu chạy Windows <> WSL
WEBHOOK_URL = f"http://{HOST_IP}:{WEBHOOK_PORT}"
NUM_CONCURRENT_JOBS = 5  
JOB_SLEEP_SECONDS = 2

# Thiết lập để tránh lỗi proxy khi gọi localhost
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

received_webhooks = []
lock = threading.Lock()

class SimpleWebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Tắt log mặc định của HTTP server
        return 

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            job_id = data.get("job_id")
            status = data.get("status")
            logs = data.get("logs", "[No Logs]").strip()
            
            with lock:
                received_webhooks.append(job_id)
            
            print(f"\n🔔 [WEBHOOK] Recv {job_id[:8]} | {status}")
            print(f"   > PROOF OF EXECUTION:")
            # In logs thụt dòng cho đẹp
            for line in logs.split('\n'):
                print(f"   | {line}")
            
            self.send_response(200)
            self.end_headers()
        except Exception as e:
            print(f"❌ Handler Error: {e}")

def start_webhook_server():
    try:
        server = ThreadingHTTPServer(('0.0.0.0', WEBHOOK_PORT), SimpleWebhookHandler)
        print(f"👂 Webhook Server listening on port {WEBHOOK_PORT}...")
        server.serve_forever()
    except OSError:
        print(f"🔥 Port {WEBHOOK_PORT} busy.")
        os._exit(1)

JOB_PAYLOAD = {
    "image": "busybox:latest",
    "commands": ["sh", "-c", f"echo 'Checking Secret...' && echo 'CONFIRMED: $MY_SECRET_KEY' && sleep {JOB_SLEEP_SECONDS}"],
    "env": { "MY_SECRET_KEY": "TEASERVERSE_123" },
    "callback_url": WEBHOOK_URL
}

def send_dispatch_request(i):
    # Thiết lập header Authorization với Bearer token <--- ĐÃ THÊM
    headers = {
        "Authorization": BEARER_TOKEN
    }
    
    try:
        # Gửi request kèm headers <--- ĐÃ CẬP NHẬT
        requests.post(f"{ZORP_URL}/dispatch", json=JOB_PAYLOAD, headers=headers, timeout=5)
        print(f"[{i:02d}] Dispatched.")
    except Exception as e:
        print(f"[{i:02d}] ❌ Dispatch Error: {e}")

def main():
    print("="*50 + "\n ZORP VERIFIER \n" + "="*50)
    threading.Thread(target=start_webhook_server, daemon=True).start()
    time.sleep(1)

    threads = [threading.Thread(target=send_dispatch_request, args=(i,)) for i in range(1, NUM_CONCURRENT_JOBS + 1)]
    for t in threads: t.start()
    for t in threads: t.join()

    # Wait loop
    start_wait = time.time()
    while len(received_webhooks) < NUM_CONCURRENT_JOBS:
        if time.time() - start_wait > (JOB_SLEEP_SECONDS + 5): break
        time.sleep(0.5)

    print("\nDONE.")

if __name__ == "__main__":
    main()
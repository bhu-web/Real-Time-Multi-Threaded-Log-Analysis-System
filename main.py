import threading
import queue
import time
import re
import os
import sqlite3

# SHARED INFRASTRUCTURE
log_queue = queue.Queue(maxsize=50)
# queue dedicated for database tasks
db_queue = queue.Queue() 
# Use absolute paths to avoid confusion
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "logs", "server.log")
DB_FILE = os.path.join(BASE_DIR, "output", "analysis_results.db")

def init_db():
    print(f"[System] Initializing database at: {DB_FILE}")
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS alerts 
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                       timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, 
                       level TEXT, message TEXT)''')
    conn.commit()
    conn.close()

def log_producer():
    print(f"[Producer] Monitoring: {LOG_FILE}")
    last_size = os.path.getsize(LOG_FILE) if os.path.exists(LOG_FILE) else 0

    while True:
        try:
            current_size = os.path.getsize(LOG_FILE)
            if current_size > last_size:
                time.sleep(0.1)  # Crucial: Give Windows time to finish the write
                with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(last_size)
                    content = f.read()
                    
                for line in content.splitlines():
                    if line.strip():
                        print(f"[Producer] Queued: {line.strip()}")
                        log_queue.put(line.strip())
                last_size = current_size
            elif current_size < last_size:
                # Handle file being cleared/truncated
                last_size = current_size
        except Exception as e:
            print(f"[Producer] Error: {e}")
            
        time.sleep(0.5)
        
def log_consumer(worker_id):
    while True:
        line = log_queue.get()
        clean_line = line.encode('ascii', 'ignore').decode('ascii').strip()
        
        if re.search(r"ERROR|CRITICAL", clean_line, re.IGNORECASE):
            level = "CRITICAL" if "CRITICAL" in clean_line.upper() else "ERROR"
            
            # INSTEAD OF CONNECTING TO DB, WE OFFLOAD TO THE DB QUEUE
            db_queue.put((level, clean_line))
            print(f"!!! [Consumer {worker_id}] Found {level}, offloading to DB queue.")
        else:
            print(f"[Consumer {worker_id}] Ignored: '{clean_line}'")
        
        log_queue.task_done()

def log_db_writer():
    """
    Dedicated single-thread worker that manages the SQLite connection lifecycle.
    Bypasses file-locking conflicts entirely.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    while True:
        # Pulls the structured alert data from the db_queue
        alert_data = db_queue.get()
        if alert_data is None:  # Poison pill to gracefully shut down if needed
            break
            
        level, message = alert_data
        try:
            cursor.execute("INSERT INTO alerts (level, message) VALUES (?, ?)", (level, message))
            conn.commit()
            print("--- [DB Writer] Successfully persisted alert to DB.")
        except sqlite3.OperationalError as e:
            print(f"--- [DB Writer] Error writing to database: {e}")
            
        db_queue.task_done()
    
    conn.close()

if __name__ == "__main__":
    init_db()
    threading.Thread(target=log_producer, daemon=True).start()
    threading.Thread(target=log_db_writer, daemon=True).start()
    for i in range(2): # 2 consumers is plenty for testing
        threading.Thread(target=log_consumer, args=(i,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[System] Shutdown.")
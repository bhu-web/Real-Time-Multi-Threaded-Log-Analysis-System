import threading
import queue
import time
import re
import os
import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

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

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, file_path):
        self.file_path = os.path.abspath(file_path)
        self.last_size = os.path.getsize(self.file_path)

    def on_modified(self, event):
        # We only care about modifications to our specific target log file
        if os.path.abspath(event.src_path) == self.file_path:
            current_size = os.path.getsize(self.file_path)
            
            if current_size > self.last_size:
                # Settle-time to ensure the OS stream is flushed
                time.sleep(0.05) 
                
                with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    f.seek(self.last_size)
                    new_lines = f.read().splitlines()
                    
                    for line in new_lines:
                        if line.strip():
                            log_queue.put(line)
                            
                self.last_size = current_size

def start_event_producer():
    log_dir = os.path.dirname(os.path.abspath(LOG_FILE))
    
    # Set up the OS directory observer
    event_handler = LogFileHandler(LOG_FILE)
    observer = Observer()
    observer.schedule(event_handler, path=log_dir, recursive=False)
    observer.start()
    
    print("--- [Producer] Event-Driven Watcher started via OS Kernel hooks.")
    # Keep the thread alive quietly
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
        
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
    threading.Thread(target=start_event_producer, daemon=True).start()
    threading.Thread(target=log_db_writer, daemon=True).start()
    for i in range(2): # 2 consumers is plenty for testing
        threading.Thread(target=log_consumer, args=(i,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[System] Shutdown.")
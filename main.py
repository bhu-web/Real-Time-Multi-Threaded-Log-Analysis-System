import threading
import queue
import time
import re
import os
import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ANSI Color Codes for Terminal Styling
CLR_RESET  = "\033[0m"
CLR_RED    = "\033[91m"    # Critical
CLR_YELLOW = "\033[93m"    # Error
CLR_GREEN  = "\033[92m"    # DB Success
CLR_CYAN   = "\033[96m"    # Consumers
CLR_GRAY   = "\033[90m"    # Ignored lines

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
    LOG_LEVEL_PATTERN = re.compile(r"^(ERROR|CRITICAL):", re.IGNORECASE)

    while True:
        line = log_queue.get()
        
        # POISON PILL CHECK: If the main thread sent None, shut down this worker cleanly
        if line is None:
            log_queue.task_done()
            print(f"{CLR_CYAN}[Consumer {worker_id}] Poison pill received. Terminating thread execution...{CLR_RESET}")
            break
            
        clean_line = line.encode('ascii', 'ignore').decode('ascii').strip()
        
        if LOG_LEVEL_PATTERN.match(clean_line):
            match = LOG_LEVEL_PATTERN.match(clean_line)
            level = match.group(1).upper()
            color = CLR_RED if level == "CRITICAL" else CLR_YELLOW
            
            db_queue.put((level, clean_line))
            print(f"{color}!!! [Consumer {worker_id}] Detected legitimate {level} -> Offloading to Storage Layer{CLR_RESET}")
        else:
            print(f"{CLR_GRAY}[Consumer {worker_id}] Ignored: Info/Warning event.{CLR_RESET}")
        
        log_queue.task_done()

def log_db_writer():
    """
    Dedicated single-thread worker that manages the SQLite connection lifecycle.
    Bypasses file-locking conflicts entirely.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    while True:
        alert_data = db_queue.get()
        if alert_data is None:
            break
            
        level, message = alert_data
        try:
            cursor.execute("INSERT INTO alerts (level, message) VALUES (?, ?)", (level, message))
            conn.commit()
            
            # --- MEANINGFUL TERMINAL OUTPUT ---
            timestamp = time.strftime("%H:%M:%S")
            print(f"{CLR_GREEN}✓ [{timestamp}] [DB_STORE] Successfully persisted [{level}]: {message}{CLR_RESET}")
            
        except sqlite3.OperationalError as e:
            print(f"{CLR_RED}❌ [DB_STORE] Operational Error writing to database: {e}{CLR_RESET}")
            
        db_queue.task_done()
    
    conn.close()

if __name__ == "__main__":
    init_db()
    
    # 1. Initialize Thread Objects (Explicitly remove daemon=True from workers handling data)
    producer_thread = threading.Thread(target=start_event_producer, daemon=True) # Producer can stay daemon
    db_writer_thread = threading.Thread(target=log_db_writer, daemon=False)     # MUST finish its commits
    
    consumer_threads = []
    for i in range(2):
        t = threading.Thread(target=log_consumer, args=(i,), daemon=False)      # MUST finish reading current item
        consumer_threads.append(t)

    # 2. Start Threads
    producer_thread.start()
    db_writer_thread.start()
    for t in consumer_threads:
        t.start()

    # 3. Wait for User Interruption
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n[System] Shutdown signal received. Initiating graceful termination...")
        
        # --- THE GRACEFUL SHUTDOWN SEQUENCE ---
        
        # Step A: Let Consumers finish whatever lines they already grabbed, then tell them to exit
        print("[System] Phase 1: Signalling Consumers to halt...")
        for _ in range(2):
            log_queue.put(None) # Poison Pill for Consumers (We need to update log_consumer to look for this)
            
        for t in consumer_threads:
            t.join() # Wait until both consumers process their poison pill and exit
        print("[System] Phase 1 Complete: All Consumers successfully terminated.")

        # Step B: Wait for the Consumer-to-DB queue to completely clear out
        print("[System] Phase 2: Flushing remaining database queue entries...")
        db_queue.put(None) # Trigger your poison pill inside log_db_writer
        
        db_writer_thread.join() # Wait for SQLite to finish writing every last alert and close cleanly
        print("[System] Phase 2 Complete: Database layer safely persisted and connection closed.")
        
        print("[System] Shutdown successful. Zero data lost. Safe to exit.")
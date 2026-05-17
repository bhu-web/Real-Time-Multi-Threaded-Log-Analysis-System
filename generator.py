import random

levels = ["INFO", "INFO", "INFO", "WARNING", "ERROR", "CRITICAL"]
messages = [
    "User login successful",
    "Database connection timeout",
    "System heartbeat stable",
    "High CPU usage",
    "Unauthorized access attempt",
    "Disk space reaching capacity",
    "API request processed in 50ms"
]

with open("logs/server.log", "a") as f:
    for i in range(10):
        level = random.choice(levels)
        msg = random.choice(messages)
        f.write(f"{level}: Simulated log entry #{i} - {msg}\n")
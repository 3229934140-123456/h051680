BLOCK_SIZE = 64 * 1024 * 1024
REPLICA_COUNT = 3
DEFAULT_CHUNK_SIZE = 1024 * 1024

HEARTBEAT_INTERVAL = 5
NODE_FAILURE_THRESHOLD = 3

METADATA_CACHE_TTL = 30
MAX_CACHE_ENTRIES = 1000

STORAGE_NODES = [
    {"id": "node-1", "host": "localhost", "port": 5001, "capacity": 10 * 1024 * 1024 * 1024},
    {"id": "node-2", "host": "localhost", "port": 5002, "capacity": 10 * 1024 * 1024 * 1024},
    {"id": "node-3", "host": "localhost", "port": 5003, "capacity": 10 * 1024 * 1024 * 1024},
    {"id": "node-4", "host": "localhost", "port": 5004, "capacity": 10 * 1024 * 1024 * 1024},
    {"id": "node-5", "host": "localhost", "port": 5005, "capacity": 10 * 1024 * 1024 * 1024},
]

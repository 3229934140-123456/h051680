import threading
import time
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class BlockData:
    block_id: str
    data: bytes
    size: int
    version: int = 0
    checksum: str = ""
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)


class StorageNode:
    def __init__(self, node_id: str, host: str, port: int, capacity: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.capacity = capacity
        self._blocks: Dict[str, BlockData] = {}
        self._lock = threading.RLock()
        self._is_alive = True
        self._last_heartbeat = time.time()

    @property
    def used_space(self) -> int:
        with self._lock:
            return sum(b.size for b in self._blocks.values())

    @property
    def available_space(self) -> int:
        return self.capacity - self.used_space

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    def set_alive(self, alive: bool):
        self._is_alive = alive

    def heartbeat(self) -> dict:
        self._last_heartbeat = time.time()
        return {
            "node_id": self.node_id,
            "alive": self._is_alive,
            "used_space": self.used_space,
            "available_space": self.available_space,
            "block_count": len(self._blocks),
            "timestamp": time.time()
        }

    def read_block(self, block_id: str) -> Tuple[bool, str, Optional[bytes]]:
        if not self._is_alive:
            return False, "Node is not available", None

        with self._lock:
            block = self._blocks.get(block_id)
            if not block:
                return False, f"Block {block_id} not found", None
            return True, "Success", block.data

    def write_block(self, block_id: str, data: bytes,
                    version: int = 0) -> Tuple[bool, str, int]:
        if not self._is_alive:
            return False, "Node is not available", -1

        with self._lock:
            data_size = len(data)
            current_used = self.used_space
            existing = self._blocks.get(block_id)

            if existing:
                new_used = current_used - existing.size + data_size
            else:
                new_used = current_used + data_size

            if new_used > self.capacity:
                return False, "Insufficient space", -1

            checksum = self._compute_checksum(data)

            if existing:
                existing.data = data
                existing.size = data_size
                existing.version = version if version > 0 else existing.version + 1
                existing.checksum = checksum
                existing.modified_at = time.time()
                return True, "Success", existing.version
            else:
                block = BlockData(
                    block_id=block_id,
                    data=data,
                    size=data_size,
                    version=max(version, 1),
                    checksum=checksum
                )
                self._blocks[block_id] = block
                return True, "Success", block.version

    def delete_block(self, block_id: str) -> Tuple[bool, str]:
        if not self._is_alive:
            return False, "Node is not available"

        with self._lock:
            if block_id not in self._blocks:
                return False, f"Block {block_id} not found"
            del self._blocks[block_id]
            return True, "Success"

    def has_block(self, block_id: str) -> bool:
        with self._lock:
            return block_id in self._blocks

    def get_block_info(self, block_id: str) -> Tuple[bool, str, Optional[dict]]:
        if not self._is_alive:
            return False, "Node is not available", None

        with self._lock:
            block = self._blocks.get(block_id)
            if not block:
                return False, f"Block {block_id} not found", None
            return True, "Success", {
                "block_id": block.block_id,
                "size": block.size,
                "version": block.version,
                "checksum": block.checksum,
                "created_at": block.created_at,
                "modified_at": block.modified_at
            }

    def list_blocks(self) -> list:
        with self._lock:
            return [
                {
                    "block_id": b.block_id,
                    "size": b.size,
                    "version": b.version
                }
                for b in self._blocks.values()
            ]

    def _compute_checksum(self, data: bytes) -> str:
        import hashlib
        return hashlib.md5(data).hexdigest()

    def verify_block(self, block_id: str) -> Tuple[bool, str]:
        with self._lock:
            block = self._blocks.get(block_id)
            if not block:
                return False, f"Block {block_id} not found"
            expected = self._compute_checksum(block.data)
            if expected == block.checksum:
                return True, "Checksum valid"
            return False, "Checksum mismatch"

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "node_id": self.node_id,
                "capacity": self.capacity,
                "used_space": self.used_space,
                "available_space": self.available_space,
                "block_count": len(self._blocks),
                "is_alive": self._is_alive
            }

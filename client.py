import threading
import time
import hashlib
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from config import BLOCK_SIZE, REPLICA_COUNT, METADATA_CACHE_TTL, MAX_CACHE_ENTRIES
from metadata_service import MetadataService, INode, BlockInfo, FileType
from block_placement import BlockPlacementStrategy
from replication_manager import ReplicationManager
from storage_node import StorageNode


@dataclass
class CacheEntry:
    key: str
    data: object
    version: int
    timestamp: float
    ttl: float = METADATA_CACHE_TTL


class MetadataCache:
    def __init__(self, max_entries: int = MAX_CACHE_ENTRIES):
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self.max_entries = max_entries

    def get(self, key: str) -> Optional[CacheEntry]:
        with self._lock:
            entry = self._cache.get(key)
            if not entry:
                return None
            if time.time() - entry.timestamp > entry.ttl:
                del self._cache[key]
                return None
            return entry

    def put(self, key: str, data: object, version: int, ttl: Optional[float] = None):
        with self._lock:
            if len(self._cache) >= self.max_entries:
                self._evict_oldest()
            self._cache[key] = CacheEntry(
                key=key,
                data=data,
                version=version,
                timestamp=time.time(),
                ttl=ttl or METADATA_CACHE_TTL
            )

    def invalidate(self, key: str):
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def invalidate_prefix(self, prefix: str):
        with self._lock:
            keys_to_remove = [k for k in self._cache if k.startswith(prefix)]
            for k in keys_to_remove:
                del self._cache[k]

    def _evict_oldest(self):
        if not self._cache:
            return
        oldest_key = min(self._cache.keys(),
                         key=lambda k: self._cache[k].timestamp)
        del self._cache[oldest_key]

    def clear(self):
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._cache)


class DFSClient:
    def __init__(self, metadata_service: MetadataService,
                 placement_strategy: BlockPlacementStrategy,
                 replication_manager: ReplicationManager,
                 storage_nodes: Dict[str, StorageNode]):
        self.metadata_service = metadata_service
        self.placement_strategy = placement_strategy
        self.replication_manager = replication_manager
        self.storage_nodes = storage_nodes

        self._metadata_cache = MetadataCache()
        self._block_cache: Dict[str, bytes] = {}
        self._lock = threading.RLock()

    def _cache_key(self, path: str, data_type: str = "inode") -> str:
        return f"{data_type}:{path}"

    def create_file(self, path: str, owner: str = "root",
                    group: str = "root", permissions: int = 0o644) -> Tuple[bool, str]:
        success, msg, inode = self.metadata_service.create_file(
            path, owner, group, permissions
        )
        if success:
            self._metadata_cache.invalidate_prefix(self._cache_key(path.rsplit("/", 1)[0] or "/", "list"))
            self._metadata_cache.invalidate(self._cache_key(path))
        return success, msg

    def create_directory(self, path: str, owner: str = "root",
                         group: str = "root", permissions: int = 0o755) -> Tuple[bool, str]:
        success, msg, inode = self.metadata_service.create_directory(
            path, owner, group, permissions
        )
        if success:
            parent_path = path.rsplit("/", 1)[0] or "/"
            self._metadata_cache.invalidate_prefix(self._cache_key(parent_path, "list"))
            self._metadata_cache.invalidate(self._cache_key(path))
        return success, msg

    def delete(self, path: str) -> Tuple[bool, str]:
        success, msg, inode = self.metadata_service.get_metadata(path)
        block_ids = []
        if success and inode and inode.type == FileType.FILE:
            block_ids = list(inode.blocks)

        success, msg = self.metadata_service.delete(path)
        if success:
            for block_id in block_ids:
                for node in self.storage_nodes.values():
                    if node.has_block(block_id):
                        node.delete_block(block_id)

            parent_path = path.rsplit("/", 1)[0] or "/"
            self._metadata_cache.invalidate_prefix(self._cache_key(parent_path, "list"))
            self._metadata_cache.invalidate(self._cache_key(path))
            self._metadata_cache.invalidate_prefix(self._cache_key(path + "/"))
        return success, msg

    def rename(self, old_path: str, new_path: str) -> Tuple[bool, str]:
        success, msg = self.metadata_service.rename(old_path, new_path)
        if success:
            old_parent = old_path.rsplit("/", 1)[0] or "/"
            new_parent = new_path.rsplit("/", 1)[0] or "/"
            self._metadata_cache.invalidate_prefix(self._cache_key(old_parent, "list"))
            self._metadata_cache.invalidate_prefix(self._cache_key(new_parent, "list"))
            self._metadata_cache.invalidate(self._cache_key(old_path))
            self._metadata_cache.invalidate(self._cache_key(new_path))
        return success, msg

    def get_metadata(self, path: str, use_cache: bool = True) -> Tuple[bool, str, Optional[INode]]:
        cache_key = self._cache_key(path)

        if use_cache:
            entry = self._metadata_cache.get(cache_key)
            if entry:
                return True, "Success (cached)", entry.data

        success, msg, inode = self.metadata_service.get_metadata(path)
        if success and inode:
            self._metadata_cache.put(cache_key, inode, inode.version)

        return success, msg, inode

    def list_directory(self, path: str, use_cache: bool = True) -> Tuple[bool, str, List[INode]]:
        cache_key = self._cache_key(path, "list")

        if use_cache:
            entry = self._metadata_cache.get(cache_key)
            if entry:
                return True, "Success (cached)", entry.data

        success, msg, children = self.metadata_service.list_directory(path)
        if success:
            success2, msg2, dir_inode = self.metadata_service.get_metadata(path)
            version = dir_inode.version if success2 else 0
            self._metadata_cache.put(cache_key, children, version)

        return success, msg, children

    def write_file(self, path: str, data: bytes) -> Tuple[bool, str]:
        success, msg, inode = self.metadata_service.get_metadata(path)
        file_existed = success and inode is not None

        if not file_existed:
            success, msg = self.create_file(path)
            if not success:
                return False, msg

        if file_existed and inode and inode.type == FileType.FILE and inode.blocks:
            success, msg, old_block_ids = self.metadata_service.clear_file_blocks(path)
            if success:
                for block_id in old_block_ids:
                    for node in self.storage_nodes.values():
                        if node.has_block(block_id):
                            node.delete_block(block_id)

        total_size = len(data)
        num_blocks = (total_size + BLOCK_SIZE - 1) // BLOCK_SIZE

        success, msg, new_block_ids = self.metadata_service.allocate_blocks(path, num_blocks)
        if not success:
            return False, msg

        for i, block_id in enumerate(new_block_ids):
            start = i * BLOCK_SIZE
            end = min(start + BLOCK_SIZE, total_size)
            block_data = data[start:end]

            try:
                replica_nodes = self.placement_strategy.place_block(
                    block_id, len(block_data)
                )
            except ValueError as e:
                return False, str(e)

            success, msg = self.replication_manager.create_initial_replicas(
                block_id, block_data, replica_nodes
            )
            if not success:
                return False, f"Failed to write block {block_id}: {msg}"

        self.metadata_service.update_file_size(path, total_size)

        self._metadata_cache.invalidate(self._cache_key(path))
        self._metadata_cache.invalidate(self._cache_key(path, "blocks"))

        return True, "Success"

    def read_file(self, path: str, offset: int = 0,
                  length: Optional[int] = None) -> Tuple[bool, str, bytes]:
        success, msg, inode = self.get_metadata(path)
        if not success or not inode:
            return False, msg, b""

        if inode.type != FileType.FILE:
            return False, "Not a file", b""

        file_size = inode.size

        if offset >= file_size:
            return True, "Success", b""

        if length is None:
            length = file_size - offset
        else:
            length = min(length, file_size - offset)

        start_block = offset // BLOCK_SIZE
        end_block = (offset + length - 1) // BLOCK_SIZE

        result_data = bytearray()
        bytes_read = 0

        success, msg, blocks = self.metadata_service.get_file_blocks(path)
        if not success:
            return False, msg, b""

        for block_idx in range(start_block, end_block + 1):
            if block_idx >= len(blocks):
                break

            block_info = blocks[block_idx]
            block_data = self._read_block_with_retry(block_info)

            if block_data is None:
                return False, f"Failed to read block {block_info.block_id}", b""

            block_start_offset = block_idx * BLOCK_SIZE
            read_start = max(0, offset - block_start_offset)
            read_end = min(len(block_data), offset + length - block_start_offset)

            if bytes_read == 0 and block_idx == start_block:
                result_data.extend(block_data[read_start:read_end])
            else:
                result_data.extend(block_data[read_start:read_end])

            bytes_read += (read_end - read_start)

            if bytes_read >= length:
                break

        return True, "Success", bytes(result_data)

    def _read_block_with_retry(self, block_info: BlockInfo) -> Optional[bytes]:
        if not block_info.replicas:
            return None

        failed_nodes = set()

        for attempt in range(len(block_info.replicas)):
            node_id = self.placement_strategy.choose_replica_for_read(
                block_info.block_id,
                preferred_nodes=set(block_info.replicas) - failed_nodes
            )

            if not node_id or node_id not in self.storage_nodes:
                if node_id:
                    failed_nodes.add(node_id)
                continue

            node = self.storage_nodes[node_id]
            if not node.is_alive:
                failed_nodes.add(node_id)
                continue

            try:
                success, msg, data = node.read_block(block_info.block_id)
                if success and data is not None:
                    return data
                else:
                    failed_nodes.add(node_id)
            except Exception:
                failed_nodes.add(node_id)

        return None

    def get_file_blocks(self, path: str,
                        use_cache: bool = True) -> Tuple[bool, str, List[BlockInfo]]:
        cache_key = self._cache_key(path, "blocks")

        if use_cache:
            entry = self._metadata_cache.get(cache_key)
            if entry:
                return True, "Success (cached)", entry.data

        success, msg, blocks = self.metadata_service.get_file_blocks(path)
        if success:
            self._metadata_cache.put(cache_key, blocks, 0)

        return success, msg, blocks

    def invalidate_cache(self, path: Optional[str] = None):
        if path:
            self._metadata_cache.invalidate(self._cache_key(path))
            self._metadata_cache.invalidate(self._cache_key(path, "blocks"))
            self._metadata_cache.invalidate(self._cache_key(path, "list"))
        else:
            self._metadata_cache.clear()

    def get_cache_stats(self) -> dict:
        return {
            "metadata_cache_size": self._metadata_cache.size(),
            "block_cache_size": len(self._block_cache)
        }

    def stat(self, path: str) -> Tuple[bool, str, Optional[dict]]:
        success, msg, inode = self.get_metadata(path)
        if not success or not inode:
            return False, msg, None

        return True, "Success", {
            "path": path,
            "name": inode.name,
            "type": inode.type.value,
            "size": inode.size,
            "owner": inode.owner,
            "group": inode.group,
            "permissions": oct(inode.permissions),
            "created_at": inode.created_at,
            "modified_at": inode.modified_at,
            "version": inode.version,
            "block_count": len(inode.blocks) if inode.type == FileType.FILE else 0
        }

    def append_file(self, path: str, data: bytes) -> Tuple[bool, str]:
        success, msg, existing_data = self.read_file(path)
        if not success:
            if "not found" in msg.lower():
                return self.write_file(path, data)
            return False, msg

        new_data = existing_data + data
        return self.write_file(path, new_data)

import threading
import time
import hashlib
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class FileType(Enum):
    FILE = "file"
    DIRECTORY = "directory"


@dataclass
class INode:
    id: str
    name: str
    type: FileType
    size: int = 0
    blocks: List[str] = field(default_factory=list)
    owner: str = "root"
    group: str = "root"
    permissions: int = 0o755
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    version: int = 0
    parent_id: Optional[str] = None
    children: Dict[str, str] = field(default_factory=dict)


@dataclass
class BlockInfo:
    block_id: str
    replicas: List[str] = field(default_factory=list)
    size: int = 0
    version: int = 0


class MetadataService:
    def __init__(self):
        self._lock = threading.RLock()
        self._inodes: Dict[str, INode] = {}
        self._blocks: Dict[str, BlockInfo] = {}
        self._next_inode_id = 1
        self._next_block_id = 1
        self._init_root()

    def _init_root(self):
        root = INode(
            id="/",
            name="/",
            type=FileType.DIRECTORY,
            parent_id=None
        )
        self._inodes["/"] = root

    def _split_path(self, path: str) -> List[str]:
        path = path.strip("/")
        if not path:
            return []
        return path.split("/")

    def _resolve_path(self, path: str) -> Optional[INode]:
        components = self._split_path(path)
        current = self._inodes.get("/")
        if not current:
            return None
        for comp in components:
            if current.type != FileType.DIRECTORY:
                return None
            child_id = current.children.get(comp)
            if not child_id:
                return None
            current = self._inodes.get(child_id)
            if not current:
                return None
        return current

    def _get_parent_and_name(self, path: str) -> Tuple[Optional[INode], Optional[str]]:
        components = self._split_path(path)
        if not components:
            return self._inodes.get("/"), None
        parent_path = "/" + "/".join(components[:-1]) if len(components) > 1 else "/"
        parent = self._resolve_path(parent_path)
        name = components[-1]
        return parent, name

    def create_file(self, path: str, owner: str = "root", group: str = "root",
                    permissions: int = 0o644) -> Tuple[bool, str, Optional[INode]]:
        with self._lock:
            parent, name = self._get_parent_and_name(path)
            if not parent or parent.type != FileType.DIRECTORY:
                return False, "Parent directory not found", None
            if name in parent.children:
                return False, "File already exists", None

            inode_id = f"inode_{self._next_inode_id}"
            self._next_inode_id += 1

            inode = INode(
                id=inode_id,
                name=name,
                type=FileType.FILE,
                size=0,
                blocks=[],
                owner=owner,
                group=group,
                permissions=permissions,
                parent_id=parent.id
            )
            self._inodes[inode_id] = inode
            parent.children[name] = inode_id
            parent.modified_at = time.time()
            parent.version += 1

            return True, "Success", inode

    def create_directory(self, path: str, owner: str = "root", group: str = "root",
                         permissions: int = 0o755) -> Tuple[bool, str, Optional[INode]]:
        with self._lock:
            parent, name = self._get_parent_and_name(path)
            if not parent or parent.type != FileType.DIRECTORY:
                return False, "Parent directory not found", None
            if name in parent.children:
                return False, "Directory already exists", None

            inode_id = f"inode_{self._next_inode_id}"
            self._next_inode_id += 1

            inode = INode(
                id=inode_id,
                name=name,
                type=FileType.DIRECTORY,
                owner=owner,
                group=group,
                permissions=permissions,
                parent_id=parent.id
            )
            self._inodes[inode_id] = inode
            parent.children[name] = inode_id
            parent.modified_at = time.time()
            parent.version += 1

            return True, "Success", inode

    def delete(self, path: str) -> Tuple[bool, str]:
        with self._lock:
            inode = self._resolve_path(path)
            if not inode:
                return False, "Path not found"

            if inode.type == FileType.DIRECTORY and inode.children:
                return False, "Directory not empty"

            parent = self._inodes.get(inode.parent_id) if inode.parent_id else None
            if parent:
                del parent.children[inode.name]
                parent.modified_at = time.time()
                parent.version += 1

            block_ids = list(inode.blocks) if inode.type == FileType.FILE else []

            del self._inodes[inode.id]

            return True, "Success"

    def rename(self, old_path: str, new_path: str) -> Tuple[bool, str]:
        with self._lock:
            inode = self._resolve_path(old_path)
            if not inode:
                return False, "Source path not found"

            old_parent = self._inodes.get(inode.parent_id) if inode.parent_id else None
            new_parent, new_name = self._get_parent_and_name(new_path)

            if not new_parent or new_parent.type != FileType.DIRECTORY:
                return False, "Destination parent not found"

            if new_name in new_parent.children:
                return False, "Destination already exists"

            if old_parent:
                del old_parent.children[inode.name]
                old_parent.modified_at = time.time()
                old_parent.version += 1

            inode.name = new_name
            inode.parent_id = new_parent.id
            new_parent.children[new_name] = inode.id
            new_parent.modified_at = time.time()
            new_parent.version += 1
            inode.modified_at = time.time()
            inode.version += 1

            return True, "Success"

    def get_metadata(self, path: str) -> Tuple[bool, str, Optional[INode]]:
        with self._lock:
            inode = self._resolve_path(path)
            if not inode:
                return False, "Path not found", None
            return True, "Success", inode

    def list_directory(self, path: str) -> Tuple[bool, str, List[INode]]:
        with self._lock:
            inode = self._resolve_path(path)
            if not inode or inode.type != FileType.DIRECTORY:
                return False, "Not a directory", []

            children = []
            for child_id in inode.children.values():
                child = self._inodes.get(child_id)
                if child:
                    children.append(child)

            return True, "Success", children

    def allocate_blocks(self, path: str, num_blocks: int) -> Tuple[bool, str, List[str]]:
        with self._lock:
            inode = self._resolve_path(path)
            if not inode or inode.type != FileType.FILE:
                return False, "Not a file", []

            new_block_ids = []
            for _ in range(num_blocks):
                block_id = f"blk_{self._next_block_id}"
                self._next_block_id += 1
                block_info = BlockInfo(block_id=block_id)
                self._blocks[block_id] = block_info
                inode.blocks.append(block_id)
                new_block_ids.append(block_id)

            inode.modified_at = time.time()
            inode.version += 1

            return True, "Success", new_block_ids

    def update_block_replicas(self, block_id: str, replicas: List[str]) -> Tuple[bool, str]:
        with self._lock:
            if block_id not in self._blocks:
                return False, "Block not found"
            self._blocks[block_id].replicas = replicas
            self._blocks[block_id].version += 1
            return True, "Success"

    def get_block_info(self, block_id: str) -> Tuple[bool, str, Optional[BlockInfo]]:
        with self._lock:
            if block_id not in self._blocks:
                return False, "Block not found", None
            return True, "Success", self._blocks[block_id]

    def get_file_blocks(self, path: str) -> Tuple[bool, str, List[BlockInfo]]:
        with self._lock:
            inode = self._resolve_path(path)
            if not inode or inode.type != FileType.FILE:
                return False, "Not a file", []

            blocks = []
            for block_id in inode.blocks:
                block = self._blocks.get(block_id)
                if block:
                    blocks.append(block)

            return True, "Success", blocks

    def update_file_size(self, path: str, size: int) -> Tuple[bool, str]:
        with self._lock:
            inode = self._resolve_path(path)
            if not inode or inode.type != FileType.FILE:
                return False, "Not a file"
            inode.size = size
            inode.modified_at = time.time()
            inode.version += 1
            return True, "Success"

    def clear_file_blocks(self, path: str) -> Tuple[bool, str, List[str]]:
        with self._lock:
            inode = self._resolve_path(path)
            if not inode or inode.type != FileType.FILE:
                return False, "Not a file", []

            old_block_ids = list(inode.blocks)
            inode.blocks = []
            inode.modified_at = time.time()
            inode.version += 1

            for block_id in old_block_ids:
                if block_id in self._blocks:
                    del self._blocks[block_id]

            return True, "Success", old_block_ids

    def get_all_blocks(self) -> Dict[str, BlockInfo]:
        with self._lock:
            return dict(self._blocks)

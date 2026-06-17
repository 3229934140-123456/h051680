import hashlib
import random
from typing import List, Dict, Optional, Set
from dataclasses import dataclass


@dataclass
class StorageNodeInfo:
    node_id: str
    host: str
    port: int
    capacity: int
    used_space: int = 0
    is_alive: bool = True

    @property
    def available_space(self) -> int:
        return self.capacity - self.used_space


class BlockPlacementStrategy:
    def __init__(self, replica_count: int = 3):
        self.replica_count = replica_count
        self._nodes: Dict[str, StorageNodeInfo] = {}
        self._random = random.Random()

    def register_node(self, node_id: str, host: str, port: int, capacity: int):
        self._nodes[node_id] = StorageNodeInfo(
            node_id=node_id,
            host=host,
            port=port,
            capacity=capacity
        )

    def unregister_node(self, node_id: str):
        if node_id in self._nodes:
            del self._nodes[node_id]

    def set_node_alive(self, node_id: str, is_alive: bool):
        if node_id in self._nodes:
            self._nodes[node_id].is_alive = is_alive

    def update_node_usage(self, node_id: str, used_space: int):
        if node_id in self._nodes:
            self._nodes[node_id].used_space = used_space

    def get_alive_nodes(self) -> List[StorageNodeInfo]:
        return [n for n in self._nodes.values() if n.is_alive]

    def get_node(self, node_id: str) -> Optional[StorageNodeInfo]:
        return self._nodes.get(node_id)

    def _hash_block_id(self, block_id: str) -> int:
        h = hashlib.md5(block_id.encode())
        return int(h.hexdigest(), 16)

    def place_block(self, block_id: str, block_size: int,
                    exclude_nodes: Optional[Set[str]] = None) -> List[str]:
        exclude = exclude_nodes or set()
        alive_nodes = [n for n in self.get_alive_nodes()
                       if n.node_id not in exclude and n.available_space >= block_size]

        if len(alive_nodes) < self.replica_count:
            raise ValueError(
                f"Not enough available nodes. Need {self.replica_count}, "
                f"have {len(alive_nodes)}"
            )

        sorted_nodes = sorted(alive_nodes, key=lambda n: n.available_space, reverse=True)

        selected = []
        primary_idx = self._hash_block_id(block_id) % len(sorted_nodes)
        selected.append(sorted_nodes[primary_idx])

        remaining = [n for i, n in enumerate(sorted_nodes) if i != primary_idx]
        self._random.shuffle(remaining)

        for node in remaining:
            if len(selected) >= self.replica_count:
                break
            selected.append(node)

        return [n.node_id for n in selected]

    def choose_replica_for_read(self, block_id: str,
                                preferred_nodes: Optional[Set[str]] = None) -> Optional[str]:
        preferred = preferred_nodes or set()
        alive_nodes = self.get_alive_nodes()
        alive_ids = {n.node_id for n in alive_nodes}

        for node_id in preferred:
            if node_id in alive_ids:
                return node_id

        if alive_nodes:
            idx = self._hash_block_id(block_id) % len(alive_nodes)
            return alive_nodes[idx].node_id

        return None

    def find_replacement_node(self, failed_node_id: str, block_size: int,
                              existing_replicas: List[str]) -> Optional[str]:
        existing_set = set(existing_replicas)
        candidates = [
            n for n in self.get_alive_nodes()
            if n.node_id not in existing_set
            and n.node_id != failed_node_id
            and n.available_space >= block_size
        ]

        if not candidates:
            return None

        candidates.sort(key=lambda n: n.available_space, reverse=True)
        return candidates[0].node_id

    def get_all_nodes(self) -> Dict[str, StorageNodeInfo]:
        return dict(self._nodes)

    def rebalance_blocks(self, block_sizes: Dict[str, int],
                         block_replicas: Dict[str, List[str]]) -> List[tuple]:
        moves = []
        nodes = self.get_alive_nodes()
        if len(nodes) < 2:
            return moves

        avg_used = sum(n.used_space for n in nodes) / len(nodes)
        threshold = avg_used * 0.1

        overloaded = [n for n in nodes if n.used_space > avg_used + threshold]
        underloaded = [n for n in nodes if n.used_space < avg_used - threshold]

        if not overloaded or not underloaded:
            return moves

        for over_node in overloaded:
            for under_node in underloaded:
                if over_node.used_space - avg_used <= threshold:
                    break
                if under_node.used_space - avg_used >= -threshold:
                    continue

                for block_id, replicas in block_replicas.items():
                    if over_node.node_id in replicas and under_node.node_id not in replicas:
                        size = block_sizes.get(block_id, 0)
                        if under_node.available_space >= size:
                            moves.append((block_id, over_node.node_id, under_node.node_id))
                            over_node.used_space -= size
                            under_node.used_space += size
                            break

        return moves

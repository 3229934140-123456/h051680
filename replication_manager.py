import threading
import time
import logging
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field

from metadata_service import MetadataService, BlockInfo
from block_placement import BlockPlacementStrategy
from storage_node import StorageNode

logger = logging.getLogger(__name__)


@dataclass
class ReplicationTask:
    task_id: str
    block_id: str
    source_node: str
    target_node: str
    status: str = "pending"
    retry_count: int = 0
    created_at: float = field(default_factory=time.time)


class ReplicationManager:
    def __init__(self, metadata_service: MetadataService,
                 placement_strategy: BlockPlacementStrategy,
                 storage_nodes: Dict[str, StorageNode],
                 replica_count: int = 3):
        self.metadata_service = metadata_service
        self.placement_strategy = placement_strategy
        self.storage_nodes = storage_nodes
        self.replica_count = replica_count

        self._lock = threading.RLock()
        self._tasks: Dict[str, ReplicationTask] = {}
        self._node_heartbeats: Dict[str, float] = {}
        self._node_failure_count: Dict[str, int] = {}
        self._failed_nodes: Set[str] = set()

        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._replication_thread: Optional[threading.Thread] = None

        self._task_counter = 0

    def start(self):
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._replication_thread = threading.Thread(target=self._replication_loop, daemon=True)
        self._monitor_thread.start()
        self._replication_thread.start()
        logger.info("ReplicationManager started")

    def stop(self):
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)
        if self._replication_thread:
            self._replication_thread.join(timeout=10)
        logger.info("ReplicationManager stopped")

    def report_heartbeat(self, node_id: str, stats: dict):
        with self._lock:
            self._node_heartbeats[node_id] = time.time()
            self._node_failure_count[node_id] = 0

            if node_id in self._failed_nodes:
                self._failed_nodes.remove(node_id)
                self.placement_strategy.set_node_alive(node_id, True)
                if node_id in self.storage_nodes:
                    self.storage_nodes[node_id].set_alive(True)
                logger.info(f"Node {node_id} recovered")

    def _monitor_loop(self):
        while not self._stop_event.is_set():
            try:
                self._check_node_health()
                self._check_replica_count()
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
            self._stop_event.wait(5)

    def _check_node_health(self):
        now = time.time()
        failure_threshold = 15

        with self._lock:
            for node_id in list(self.storage_nodes.keys()):
                last_heartbeat = self._node_heartbeats.get(node_id, 0)
                if now - last_heartbeat > failure_threshold:
                    if node_id not in self._failed_nodes:
                        self._node_failure_count[node_id] = \
                            self._node_failure_count.get(node_id, 0) + 1

                        if self._node_failure_count[node_id] >= 3:
                            self._failed_nodes.add(node_id)
                            self.placement_strategy.set_node_alive(node_id, False)
                            if node_id in self.storage_nodes:
                                self.storage_nodes[node_id].set_alive(False)
                            logger.warning(f"Node {node_id} marked as failed")

    def _check_replica_count(self):
        all_blocks = self.metadata_service.get_all_blocks()

        for block_id, block_info in all_blocks.items():
            alive_replicas = [
                r for r in block_info.replicas
                if r not in self._failed_nodes and r in self.storage_nodes
            ]

            if len(alive_replicas) < self.replica_count:
                self._schedule_replication(block_id, block_info, alive_replicas)

    def _schedule_replication(self, block_id: str, block_info: BlockInfo,
                              alive_replicas: List[str]):
        with self._lock:
            existing_tasks = [
                t for t in self._tasks.values()
                if t.block_id == block_id and t.status in ("pending", "running")
            ]
            if existing_tasks:
                return

            if not alive_replicas:
                logger.error(f"No alive replicas for block {block_id}")
                return

            source_node = alive_replicas[0]
            target_node = self.placement_strategy.find_replacement_node(
                failed_node_id="",
                block_size=block_info.size,
                existing_replicas=alive_replicas
            )

            if not target_node:
                logger.warning(f"No available target node for block {block_id}")
                return

            self._task_counter += 1
            task_id = f"repl_task_{self._task_counter}"
            task = ReplicationTask(
                task_id=task_id,
                block_id=block_id,
                source_node=source_node,
                target_node=target_node
            )
            self._tasks[task_id] = task
            logger.info(f"Scheduled replication task {task_id}: "
                        f"{block_id} from {source_node} to {target_node}")

    def _replication_loop(self):
        while not self._stop_event.is_set():
            try:
                self._process_replication_tasks()
            except Exception as e:
                logger.error(f"Error in replication loop: {e}")
            self._stop_event.wait(2)

    def _process_replication_tasks(self):
        with self._lock:
            pending_tasks = [
                t for t in self._tasks.values()
                if t.status == "pending"
            ]

        for task in pending_tasks:
            try:
                self._execute_replication(task)
            except Exception as e:
                logger.error(f"Replication task {task.task_id} failed: {e}")
                task.status = "failed"
                task.retry_count += 1

                if task.retry_count < 3:
                    task.status = "pending"
                else:
                    logger.error(f"Task {task.task_id} exceeded max retries")

    def _execute_replication(self, task: ReplicationTask):
        task.status = "running"
        logger.info(f"Executing replication task {task.task_id}")

        source_node = self.storage_nodes.get(task.source_node)
        target_node = self.storage_nodes.get(task.target_node)

        if not source_node or not target_node:
            task.status = "failed"
            return

        success, msg, data = source_node.read_block(task.block_id)
        if not success:
            task.status = "failed"
            return

        success, msg, version = target_node.write_block(task.block_id, data)
        if not success:
            task.status = "failed"
            return

        success, msg, block_info = self.metadata_service.get_block_info(task.block_id)
        if success and block_info:
            new_replicas = list(block_info.replicas)
            if task.target_node not in new_replicas:
                new_replicas.append(task.target_node)
            self.metadata_service.update_block_replicas(task.block_id, new_replicas)

        task.status = "completed"
        logger.info(f"Replication task {task.task_id} completed successfully")

    def create_initial_replicas(self, block_id: str, data: bytes,
                                replica_nodes: List[str]) -> Tuple[bool, str]:
        successful_nodes = []

        for node_id in replica_nodes:
            node = self.storage_nodes.get(node_id)
            if not node:
                continue

            try:
                success, msg, version = node.write_block(block_id, data)
                if success:
                    successful_nodes.append(node_id)
                else:
                    logger.warning(f"Failed to write block {block_id} to {node_id}: {msg}")
            except Exception as e:
                logger.error(f"Error writing to node {node_id}: {e}")

        if len(successful_nodes) == 0:
            return False, "Failed to write to any replica node"

        self.metadata_service.update_block_replicas(block_id, successful_nodes)

        if len(successful_nodes) < self.replica_count:
            logger.warning(f"Only {len(successful_nodes)}/{self.replica_count} "
                           f"replicas created for block {block_id}")

        return True, "Success"

    def get_failed_nodes(self) -> Set[str]:
        with self._lock:
            return set(self._failed_nodes)

    def get_active_tasks(self) -> List[dict]:
        with self._lock:
            return [
                {
                    "task_id": t.task_id,
                    "block_id": t.block_id,
                    "source_node": t.source_node,
                    "target_node": t.target_node,
                    "status": t.status,
                    "retry_count": t.retry_count
                }
                for t in self._tasks.values()
                if t.status in ("pending", "running")
            ]

    def get_replica_status(self) -> Dict[str, dict]:
        all_blocks = self.metadata_service.get_all_blocks()
        status = {}

        for block_id, block_info in all_blocks.items():
            alive_replicas = [
                r for r in block_info.replicas
                if r not in self._failed_nodes
            ]
            status[block_id] = {
                "expected_replicas": self.replica_count,
                "actual_replicas": len(alive_replicas),
                "replica_nodes": alive_replicas,
                "is_under_replicated": len(alive_replicas) < self.replica_count
            }

        return status

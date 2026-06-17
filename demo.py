import time
import sys
import os

from config import STORAGE_NODES, REPLICA_COUNT, BLOCK_SIZE
from metadata_service import MetadataService
from block_placement import BlockPlacementStrategy
from storage_node import StorageNode
from replication_manager import ReplicationManager
from client import DFSClient


def create_system():
    metadata_service = MetadataService()

    placement_strategy = BlockPlacementStrategy(replica_count=REPLICA_COUNT)

    storage_nodes = {}
    for node_config in STORAGE_NODES:
        node = StorageNode(
            node_id=node_config["id"],
            host=node_config["host"],
            port=node_config["port"],
            capacity=node_config["capacity"]
        )
        storage_nodes[node_config["id"]] = node
        placement_strategy.register_node(
            node_id=node_config["id"],
            host=node_config["host"],
            port=node_config["port"],
            capacity=node_config["capacity"]
        )

    replication_manager = ReplicationManager(
        metadata_service=metadata_service,
        placement_strategy=placement_strategy,
        storage_nodes=storage_nodes,
        replica_count=REPLICA_COUNT
    )

    client = DFSClient(
        metadata_service=metadata_service,
        placement_strategy=placement_strategy,
        replication_manager=replication_manager,
        storage_nodes=storage_nodes
    )

    return {
        "metadata_service": metadata_service,
        "placement_strategy": placement_strategy,
        "storage_nodes": storage_nodes,
        "replication_manager": replication_manager,
        "client": client
    }


def print_separator(title=""):
    width = 80
    if title:
        print("\n" + "=" * width)
        print(f"  {title}")
        print("=" * width)
    else:
        print("\n" + "=" * width)


def demo_metadata_operations(client):
    print_separator("1. 元数据操作演示")

    print("\n[创建目录] /data")
    success, msg = client.create_directory("/data")
    print(f"  结果: {success} - {msg}")

    print("\n[创建目录] /data/docs")
    success, msg = client.create_directory("/data/docs")
    print(f"  结果: {success} - {msg}")

    print("\n[创建目录] /data/images")
    success, msg = client.create_directory("/data/images")
    print(f"  结果: {success} - {msg}")

    print("\n[列举根目录] /")
    success, msg, children = client.list_directory("/", use_cache=False)
    print(f"  结果: {success} - {msg}")
    for child in children:
        print(f"    - {child.name} ({child.type.value})")

    print("\n[列举目录] /data")
    success, msg, children = client.list_directory("/data", use_cache=False)
    print(f"  结果: {success} - {msg}")
    for child in children:
        print(f"    - {child.name} ({child.type.value})")

    print("\n[文件元数据] /data")
    success, msg, stat = client.stat("/data")
    print(f"  结果: {success} - {msg}")
    if stat:
        print(f"    名称: {stat['name']}")
        print(f"    类型: {stat['type']}")
        print(f"    权限: {stat['permissions']}")
        print(f"    所有者: {stat['owner']}")

    print("\n[重命名目录] /data/images -> /data/photos")
    success, msg = client.rename("/data/images", "/data/photos")
    print(f"  结果: {success} - {msg}")

    print("\n[列举目录] /data (重命名后)")
    success, msg, children = client.list_directory("/data", use_cache=False)
    print(f"  结果: {success} - {msg}")
    for child in children:
        print(f"    - {child.name} ({child.type.value})")

    print("\n[删除目录] /data/photos")
    success, msg = client.delete("/data/photos")
    print(f"  结果: {success} - {msg}")

    print("\n[列举目录] /data (删除后)")
    success, msg, children = client.list_directory("/data", use_cache=False)
    print(f"  结果: {success} - {msg}")
    for child in children:
        print(f"    - {child.name} ({child.type.value})")


def demo_file_read_write(client):
    print_separator("2. 文件读写演示")

    test_content = b"Hello, Distributed File System!\n" * 100
    print(f"\n[写入文件] /data/docs/hello.txt")
    print(f"  文件大小: {len(test_content)} 字节")
    success, msg = client.write_file("/data/docs/hello.txt", test_content)
    print(f"  结果: {success} - {msg}")

    print("\n[读取文件] /data/docs/hello.txt")
    success, msg, data = client.read_file("/data/docs/hello.txt")
    print(f"  结果: {success} - {msg}")
    print(f"  读取大小: {len(data)} 字节")
    print(f"  内容前100字节: {data[:100]}...")

    print("\n[文件统计] /data/docs/hello.txt")
    success, msg, stat = client.stat("/data/docs/hello.txt")
    print(f"  结果: {success} - {msg}")
    if stat:
        print(f"    大小: {stat['size']} 字节")
        print(f"    块数: {stat['block_count']}")
        print(f"    版本: {stat['version']}")

    print("\n[获取文件块信息] /data/docs/hello.txt")
    success, msg, blocks = client.get_file_blocks("/data/docs/hello.txt", use_cache=False)
    print(f"  结果: {success} - {msg}")
    for i, block in enumerate(blocks):
        print(f"    块 {i}: {block.block_id} - 副本: {block.replicas}")

    print("\n[追加写入] /data/docs/hello.txt")
    append_content = b"\nAppended content line 1\nAppended content line 2\n"
    success, msg = client.append_file("/data/docs/hello.txt", append_content)
    print(f"  结果: {success} - {msg}")

    print("\n[读取追加后的文件] /data/docs/hello.txt")
    success, msg, data = client.read_file("/data/docs/hello.txt")
    print(f"  结果: {success} - {msg}")
    print(f"  总大小: {len(data)} 字节")

    print("\n[部分读取] offset=50, length=100")
    success, msg, data = client.read_file("/data/docs/hello.txt", offset=50, length=100)
    print(f"  结果: {success} - {msg}")
    print(f"  读取大小: {len(data)} 字节")
    print(f"  内容: {data}")


def demo_large_file(storage_nodes, client):
    print_separator("3. 大文件分块演示")

    block_count = 5
    file_size = block_count * BLOCK_SIZE + 1024
    print(f"\n[创建大文件] /data/large_file.dat")
    print(f"  文件大小: {file_size} 字节 ({file_size / 1024 / 1024:.2f} MB)")
    print(f"  块大小: {BLOCK_SIZE} 字节 ({BLOCK_SIZE / 1024 / 1024:.0f} MB)")
    print(f"  预期块数: {block_count + 1}")

    large_data = b"A" * file_size
    success, msg = client.write_file("/data/large_file.dat", large_data)
    print(f"  结果: {success} - {msg}")

    print("\n[获取文件块信息]")
    success, msg, blocks = client.get_file_blocks("/data/large_file.dat", use_cache=False)
    print(f"  结果: {success} - {msg}")
    print(f"  实际块数: {len(blocks)}")
    for i, block in enumerate(blocks):
        print(f"    块 {i}: {block.block_id} - 副本节点: {block.replicas}")

    print("\n[各存储节点块分布]")
    for node_id, node in storage_nodes.items():
        node_blocks = node.list_blocks()
        print(f"  {node_id}: {len(node_blocks)} 个块, 已用空间: {node.used_space / 1024 / 1024:.2f} MB")

    print("\n[验证读取]")
    success, msg, read_data = client.read_file("/data/large_file.dat")
    print(f"  读取结果: {success} - {msg}")
    print(f"  读取大小: {len(read_data)} 字节")
    print(f"  数据一致性: {read_data == large_data}")

    print("\n[随机位置读取验证]")
    test_positions = [0, 100, BLOCK_SIZE - 100, BLOCK_SIZE, 2 * BLOCK_SIZE + 500, file_size - 200]
    all_match = True
    for pos in test_positions:
        length = 100
        success, msg, data = client.read_file("/data/large_file.dat", offset=pos, length=length)
        expected = large_data[pos:pos + length]
        match = data == expected
        if not match:
            all_match = False
        print(f"  offset={pos}: {'✓' if match else '✗'} 大小={len(data)}")
    print(f"  全部匹配: {all_match}")


def demo_replication_and_fault_tolerance(storage_nodes, replication_manager, client):
    print_separator("4. 副本与容错演示")

    test_file = "/data/test_replica.txt"
    test_data = b"Test data for replication demo" * 10
    client.write_file(test_file, test_data)

    print(f"\n[初始状态] 文件: {test_file}")
    success, msg, blocks = client.get_file_blocks(test_file, use_cache=False)
    for block in blocks:
        print(f"  {block.block_id}: {len(block.replicas)} 个副本 - {block.replicas}")

    print("\n[存储节点状态]")
    for node_id, node in storage_nodes.items():
        stats = node.get_stats()
        print(f"  {node_id}: alive={stats['is_alive']}, blocks={stats['block_count']}")

    print("\n[模拟节点故障] 关闭 node-1")
    storage_nodes["node-1"].set_alive(False)

    time.sleep(1)

    print("\n[从其他节点读取验证]")
    success, msg, data = client.read_file(test_file)
    print(f"  读取结果: {success} - {msg}")
    print(f"  数据正确: {data == test_data}")

    print("\n[手动触发副本修复]")
    replica_status = replication_manager.get_replica_status()
    for block_id, status in replica_status.items():
        if status["is_under_replicated"]:
            print(f"  {block_id}: 副本不足 ({status['actual_replicas']}/{status['expected_replicas']})")

    print("\n[恢复节点] 重启 node-1")
    storage_nodes["node-1"].set_alive(True)
    replication_manager.report_heartbeat("node-1", {})

    print("\n[节点状态已恢复]")
    for node_id, node in storage_nodes.items():
        stats = node.get_stats()
        print(f"  {node_id}: alive={stats['is_alive']}")


def demo_metadata_caching(client):
    print_separator("5. 元数据缓存演示")

    test_dir = "/data/cache_test"
    test_file = "/data/cache_test/file.txt"
    client.create_directory(test_dir)
    client.write_file(test_file, b"cache test data")

    print("\n[缓存初始状态]")
    stats = client.get_cache_stats()
    print(f"  元数据缓存大小: {stats['metadata_cache_size']}")

    print("\n[第一次读取元数据 (未缓存)]")
    t0 = time.time()
    success, msg, inode = client.get_metadata(test_file, use_cache=True)
    t1 = time.time()
    print(f"  结果: {success} - {msg}")
    print(f"  耗时: {(t1 - t0) * 1000:.3f} ms")

    print("\n[第二次读取元数据 (已缓存)]")
    t0 = time.time()
    success, msg, inode = client.get_metadata(test_file, use_cache=True)
    t1 = time.time()
    print(f"  结果: {success} - {msg}")
    print(f"  耗时: {(t1 - t0) * 1000:.3f} ms")

    print("\n[缓存状态]")
    stats = client.get_cache_stats()
    print(f"  元数据缓存大小: {stats['metadata_cache_size']}")

    print("\n[修改文件后缓存失效]")
    client.write_file(test_file, b"updated data")

    print("\n[缓存失效后重新读取]")
    success, msg, inode = client.get_metadata(test_file, use_cache=True)
    print(f"  结果: {success} - {msg}")
    if inode:
        print(f"  文件版本: {inode.version}")

    print("\n[列举目录缓存]")
    success, msg, children = client.list_directory(test_dir, use_cache=True)
    print(f"  结果: {success} - {msg}")
    print(f"  子项数: {len(children)}")

    stats = client.get_cache_stats()
    print(f"  当前缓存大小: {stats['metadata_cache_size']}")


def demo_directory_operations(client):
    print_separator("6. 大目录高效列举演示")

    big_dir = "/data/big_directory"
    client.create_directory(big_dir)

    print(f"\n[创建大目录] {big_dir}")
    print("  创建 100 个子文件...")
    for i in range(100):
        filename = f"file_{i:04d}.txt"
        client.write_file(f"{big_dir}/{filename}", f"content of {filename}".encode())

    print("\n[列举大目录]")
    t0 = time.time()
    success, msg, children = client.list_directory(big_dir, use_cache=False)
    t1 = time.time()
    print(f"  结果: {success} - {msg}")
    print(f"  文件数: {len(children)}")
    print(f"  列举耗时: {(t1 - t0) * 1000:.3f} ms")

    print("\n[前10个文件:]")
    for child in sorted(children, key=lambda c: c.name)[:10]:
        print(f"    {child.name}")

    print("\n...")
    print("\n[后10个文件:]")
    for child in sorted(children, key=lambda c: c.name)[-10:]:
        print(f"    {child.name}")

    print("\n[使用缓存列举]")
    t0 = time.time()
    success, msg, children2 = client.list_directory(big_dir, use_cache=True)
    t1 = time.time()
    print(f"  结果: {success} - {msg}")
    print(f"  列举耗时: {(t1 - t0) * 1000:.3f} ms")


def main():
    print("=" * 80)
    print("  分布式文件系统 - 演示程序")
    print("  Distributed File System Demo")
    print("=" * 80)
    print(f"\n  配置:")
    print(f"    块大小: {BLOCK_SIZE / 1024 / 1024:.0f} MB")
    print(f"    副本数: {REPLICA_COUNT}")
    print(f"    存储节点: {len(STORAGE_NODES)} 个")

    system = create_system()
    client = system["client"]
    storage_nodes = system["storage_nodes"]
    replication_manager = system["replication_manager"]

    replication_manager.start()

    for node_id in storage_nodes:
        replication_manager.report_heartbeat(node_id, {})

    try:
        demo_metadata_operations(client)
        demo_file_read_write(client)
        demo_large_file(storage_nodes, client)
        demo_replication_and_fault_tolerance(storage_nodes, replication_manager, client)
        demo_metadata_caching(client)
        demo_directory_operations(client)

        print_separator("系统状态总览")

        print("\n[存储节点统计]")
        for node_id, node in sorted(storage_nodes.items()):
            stats = node.get_stats()
            used_mb = stats["used_space"] / 1024 / 1024
            total_mb = stats["capacity"] / 1024 / 1024
            print(f"  {node_id}: {stats['block_count']} blocks, "
                  f"{used_mb:.1f}/{total_mb:.0f} MB, "
                  f"alive={stats['is_alive']}")

        print("\n[缓存统计]")
        stats = client.get_cache_stats()
        print(f"  元数据缓存条目: {stats['metadata_cache_size']}")

        print("\n" + "=" * 80)
        print("  演示完成!")
        print("=" * 80)

    except KeyboardInterrupt:
        print("\n\n演示被用户中断")
    finally:
        replication_manager.stop()


if __name__ == "__main__":
    main()

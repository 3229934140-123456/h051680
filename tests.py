import sys
import os
import time

from config import STORAGE_NODES, REPLICA_COUNT, BLOCK_SIZE
from metadata_service import MetadataService
from block_placement import BlockPlacementStrategy
from storage_node import StorageNode
from replication_manager import ReplicationManager
from client import DFSClient


def create_test_system():
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


def test_header(name):
    width = 70
    print("\n" + "=" * width)
    print(f"  TEST: {name}")
    print("=" * width)


def assert_true(condition, msg=""):
    if condition:
        print(f"  ✓ PASS: {msg}")
        return True
    else:
        print(f"  ✗ FAIL: {msg}")
        return False


def assert_false(condition, msg=""):
    return assert_true(not condition, msg)


def assert_equal(a, b, msg=""):
    return assert_true(a == b, f"{msg} (expected {b}, got {a})")


def test_1_root_protection(client):
    test_header("1. 根目录保护")

    all_pass = True

    success, msg = client.delete("/")
    all_pass &= assert_false(success, "删除 / 应失败")
    all_pass &= assert_true("root" in msg.lower() or "cannot" in msg.lower(),
                            "错误信息应包含根目录保护提示")

    success, msg = client.rename("/", "/newname")
    all_pass &= assert_false(success, "将 / 改名应失败")

    success, msg = client.rename("/data", "/")
    all_pass &= assert_false(success, "改名到 / 应失败")

    success, msg = client.create_directory("/data")
    all_pass &= assert_true(success, "保护机制不影响创建目录")

    success, msg, children = client.list_directory("/", use_cache=False)
    all_pass &= assert_true(success, "保护机制不影响列举根目录")

    return all_pass


def test_2_read_parameter_validation(client):
    test_header("2. 读取参数校验")

    all_pass = True
    test_path = "/test_validation.txt"
    test_data = b"Hello World! Test data for validation."
    client.write_file(test_path, test_data)

    success, msg, data = client.read_file(test_path, offset=-1)
    all_pass &= assert_false(success, "offset=-1 应返回失败")
    all_pass &= assert_true("Invalid" in msg or "negative" in msg.lower(),
                            "错误信息应包含参数错误提示")
    all_pass &= assert_equal(len(data), 0, "返回数据应为空")

    success, msg, data = client.read_file(test_path, offset=-100)
    all_pass &= assert_false(success, "offset=-100 应返回失败")

    success, msg, data = client.read_file(test_path, offset=0, length=-1)
    all_pass &= assert_false(success, "length=-1 应返回失败")

    success, msg, data = client.read_file(test_path, offset=0, length=-999)
    all_pass &= assert_false(success, "length=-999 应返回失败")

    success, msg, data = client.read_file(test_path, offset=-5, length=-5)
    all_pass &= assert_false(success, "offset和length都为负应返回失败")

    success, msg, data = client.read_file(test_path, offset=0, length=5)
    all_pass &= assert_true(success, "正常参数应成功")
    all_pass &= assert_equal(data, test_data[:5], "正常读取内容正确")

    return all_pass


def test_3_write_atomicity_on_failure(client, storage_nodes):
    test_header("3. 覆盖写入失败时保留旧文件完整性")

    all_pass = True
    test_path = "/atomic_test.txt"
    original_data = b"ORIGINAL CONTENT - This must survive a failed write!" * 100
    original_size = len(original_data)

    success, msg = client.write_file(test_path, original_data)
    all_pass &= assert_true(success, "初始写入成功")

    success, msg, orig_read = client.read_file(test_path)
    all_pass &= assert_true(success, "初始文件可读")
    all_pass &= assert_equal(orig_read, original_data, "初始内容正确")

    success, msg, orig_blocks = client.get_file_blocks(test_path, use_cache=False)
    all_pass &= assert_true(success, "获取初始块列表成功")
    orig_block_ids = [b.block_id for b in orig_blocks]
    orig_block_count = len(orig_block_ids)

    print("\n  --- 模拟节点故障触发写入失败 ---")

    failed_nodes = ["node-1", "node-2", "node-3"]
    for nid in failed_nodes:
        storage_nodes[nid].set_alive(False)
    print(f"  已模拟节点故障: {failed_nodes}")

    new_data = b"NEW CONTENT - This write should fail completely!" * 200
    success, msg = client.write_file(test_path, new_data)
    all_pass &= assert_false(success, f"新写入应失败 (节点不足)")
    print(f"  写入返回: {msg}")

    for nid in failed_nodes:
        storage_nodes[nid].set_alive(True)

    print("\n  --- 验证旧文件完整性 ---")

    success, msg, data_after = client.read_file(test_path)
    all_pass &= assert_true(success, "旧文件仍应可读")
    all_pass &= assert_equal(data_after, original_data, "旧文件内容不应被修改")
    all_pass &= assert_equal(len(data_after), original_size, "旧文件大小不应被修改")

    success, msg, blocks_after = client.get_file_blocks(test_path, use_cache=False)
    all_pass &= assert_true(success, "获取块信息成功")
    all_pass &= assert_equal(len(blocks_after), orig_block_count, "块数量应保持不变")

    block_ids_after = [b.block_id for b in blocks_after]
    all_pass &= assert_equal(block_ids_after, orig_block_ids, "块ID列表应保持不变")

    success, msg, stat = client.stat(test_path)
    all_pass &= assert_true(success, "stat 成功")
    all_pass &= assert_equal(stat["size"], original_size, "stat 大小保持原值")

    return all_pass


def test_4_write_guarantees_replica_count(client, metadata_service):
    test_header("4. 写入保证每个块达到配置副本数")

    all_pass = True
    test_path = "/replica_guarantee_test.dat"

    file_size = 3 * BLOCK_SIZE + 500
    test_data = b"X" * file_size

    success, msg = client.write_file(test_path, test_data)
    all_pass &= assert_true(success, "大文件写入成功")

    success, msg, blocks = client.get_file_blocks(test_path, use_cache=False)
    all_pass &= assert_true(success, "获取块信息成功")

    print(f"\n  预期块数: {4}, 实际块数: {len(blocks)}")

    all_blocks_ok = True
    for i, block in enumerate(blocks):
        replica_count = len(block.replicas)
        ok = replica_count >= REPLICA_COUNT
        all_blocks_ok &= ok
        print(f"  块 {i} ({block.block_id}): {replica_count} 副本 "
              f"{'✓' if ok else '✗'} (要求 >= {REPLICA_COUNT})")

        unique_nodes = set(block.replicas)
        all_blocks_ok &= len(unique_nodes) == len(block.replicas)

    all_pass &= assert_true(all_blocks_ok, f"所有块达到 {REPLICA_COUNT} 副本, 且分布在不同节点")

    success, msg, data = client.read_file(test_path)
    all_pass &= assert_true(success, "读取成功")
    all_pass &= assert_equal(data, test_data, "读取数据完整一致")

    for i, block in enumerate(blocks):
        readable_count = 0
        for node_id in block.replicas:
            success_r, _, bdata = client._read_block_with_retry(block)
            if bdata is not None:
                readable_count += 1
                break
        ok = readable_count >= 1
        all_pass &= assert_true(ok, f"块 {i} 至少有1个可读副本")

    return all_pass


def test_5_directory_rename_cache_invalidation(client):
    test_header("5. 目录改名后缓存立即失效")

    all_pass = True
    old_dir = "/rename_test_old"
    new_dir = "/rename_test_new"
    sub_file = f"{old_dir}/nested_file.txt"
    file_content = b"Content in nested file under directory to be renamed"

    client.create_directory(old_dir)
    client.write_file(sub_file, file_content)

    print(f"\n  --- 改名前验证 ---")
    success, msg, stat = client.get_metadata(old_dir, use_cache=True)
    all_pass &= assert_true(success, f"旧路径 {old_dir} 存在 (cache)")

    success, msg, data = client.read_file(sub_file)
    all_pass &= assert_true(success, f"旧路径文件可读: {sub_file}")

    print(f"\n  --- 执行改名: {old_dir} -> {new_dir} ---")
    success, msg = client.rename(old_dir, new_dir)
    all_pass &= assert_true(success, "改名操作成功")

    print(f"\n  --- 改名后立即验证 (使用缓存) ---")
    new_sub_file = f"{new_dir}/nested_file.txt"

    success, msg, stat = client.get_metadata(old_dir, use_cache=True)
    all_pass &= assert_false(success, f"旧路径 {old_dir} 应不存在 (立即检查, use_cache=True)")

    success, msg, stat = client.get_metadata(new_dir, use_cache=True)
    all_pass &= assert_true(success, f"新路径 {new_dir} 应存在 (立即检查, use_cache=True)")

    success, msg, data = client.read_file(sub_file)
    all_pass &= assert_false(success, f"旧路径下的文件应不存在: {sub_file}")

    success, msg, data = client.read_file(new_sub_file)
    all_pass &= assert_true(success, f"新路径下的文件应可读: {new_sub_file}")
    all_pass &= assert_equal(data, file_content, "新路径文件内容正确")

    success, msg, children = client.list_directory(new_dir, use_cache=True)
    all_pass &= assert_true(success, f"列举新目录成功 (use_cache=True)")
    all_pass &= assert_equal(len(children), 1, "新目录包含1个文件")

    success, msg, children = client.list_directory(old_dir, use_cache=True)
    all_pass &= assert_false(success, f"列举旧目录应失败")

    success, msg, root_children = client.list_directory("/", use_cache=True)
    all_pass &= assert_true(success, "列举根目录成功")
    root_names = [c.name for c in root_children]
    all_pass &= assert_true("rename_test_new" in root_names, "根目录包含新目录名")
    all_pass &= assert_false("rename_test_old" in root_names, "根目录不包含旧目录名")

    return all_pass


def test_6_directory_delete_cache_invalidation(client):
    test_header("6. 目录删除后缓存立即失效")

    all_pass = True
    parent_dir = "/del_test_parent"
    child_dir = f"{parent_dir}/child"
    grandchild_file = f"{child_dir}/grandchild.txt"
    file_content = b"Deeply nested file content for deletion test"

    client.create_directory(parent_dir)
    client.create_directory(child_dir)
    client.write_file(grandchild_file, file_content)

    print(f"\n  --- 删除前验证 ---")
    for p in [parent_dir, child_dir, grandchild_file]:
        success, msg, _ = client.get_metadata(p, use_cache=True)
        all_pass &= assert_true(success, f"路径存在: {p}")

    print(f"\n  --- 执行删除: {parent_dir} ---")

    success, msg = client.delete(grandchild_file)
    all_pass &= assert_true(success, f"删除文件: {grandchild_file}")
    success, msg = client.delete(child_dir)
    all_pass &= assert_true(success, f"删除子目录: {child_dir}")
    success, msg = client.delete(parent_dir)
    all_pass &= assert_true(success, f"删除父目录: {parent_dir}")

    print(f"\n  --- 删除后立即验证 (使用缓存) ---")
    for p in [parent_dir, child_dir, grandchild_file]:
        success, msg, _ = client.get_metadata(p, use_cache=True)
        all_pass &= assert_false(success, f"路径应不存在: {p} (use_cache=True)")

        success, msg, data = client.read_file(p)
        all_pass &= assert_false(success, f"读取应失败: {p}")

    success, msg, root_children = client.list_directory("/", use_cache=True)
    all_pass &= assert_true(success, "列举根目录成功")
    root_names = [c.name for c in root_children]
    all_pass &= assert_false("del_test_parent" in root_names,
                             "根目录不包含已删除目录")

    print(f"\n  --- 验证根目录功能正常 ---")
    success, msg = client.create_directory("/del_test_verify")
    all_pass &= assert_true(success, "删除后仍能创建新目录")

    success, msg, _ = client.get_metadata("/del_test_verify", use_cache=True)
    all_pass &= assert_true(success, "新目录可查询")

    return all_pass


def test_7_post_root_op_integrity(client):
    test_header("7. 根目录操作失败后系统完整性")

    all_pass = True

    success, msg = client.delete("/")
    all_pass &= assert_false(success, "删除根目录失败")

    success, msg = client.rename("/", "/tmp")
    all_pass &= assert_false(success, "改名根目录失败")

    print("\n  --- 验证所有操作仍正常 ---")

    test_paths = [
        "/integrity_test",
        "/integrity_test/subdir",
        "/integrity_test/file1.txt",
        "/integrity_test/subdir/file2.txt"
    ]

    success, msg = client.create_directory(test_paths[0])
    all_pass &= assert_true(success, f"创建目录: {test_paths[0]}")

    success, msg = client.create_directory(test_paths[1])
    all_pass &= assert_true(success, f"创建子目录: {test_paths[1]}")

    success, msg = client.write_file(test_paths[2], b"File 1 content")
    all_pass &= assert_true(success, f"写文件: {test_paths[2]}")

    success, msg = client.write_file(test_paths[3], b"File 2 content in subdir")
    all_pass &= assert_true(success, f"写文件: {test_paths[3]}")

    for p in test_paths:
        success, msg, _ = client.get_metadata(p, use_cache=False)
        all_pass &= assert_true(success, f"可查询: {p}")

    success, msg, children = client.list_directory("/", use_cache=False)
    all_pass &= assert_true(success, "列举根目录成功")
    names = [c.name for c in children]
    all_pass &= assert_true("integrity_test" in names, "根目录包含新建目录")

    return all_pass


def main():
    print("=" * 70)
    print("  DFS 改进验证测试套件")
    print("=" * 70)

    all_tests_pass = True
    results = {}

    def run_test(name, func, *args):
        nonlocal all_tests_pass
        try:
            passed = func(*args)
            results[name] = passed
            if not passed:
                all_tests_pass = False
            return passed
        except Exception as e:
            print(f"  ✗ EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
            results[name] = False
            all_tests_pass = False
            return False

    sys1 = create_test_system()
    run_test("根目录保护", test_1_root_protection, sys1["client"])

    sys2 = create_test_system()
    run_test("读取参数校验", test_2_read_parameter_validation, sys2["client"])

    sys3 = create_test_system()
    run_test("覆盖写入原子性", test_3_write_atomicity_on_failure,
             sys3["client"], sys3["storage_nodes"])

    sys4 = create_test_system()
    run_test("写入副本数保证", test_4_write_guarantees_replica_count,
             sys4["client"], sys4["metadata_service"])

    sys5 = create_test_system()
    run_test("目录改名缓存失效", test_5_directory_rename_cache_invalidation,
             sys5["client"])

    sys6 = create_test_system()
    run_test("目录删除缓存失效", test_6_directory_delete_cache_invalidation,
             sys6["client"])

    sys7 = create_test_system()
    run_test("根目录操作后完整性", test_7_post_root_op_integrity,
             sys7["client"])

    print("\n" + "=" * 70)
    print("  测试结果汇总")
    print("=" * 70)

    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")

    print("-" * 70)
    total = len(results)
    passed_count = sum(1 for v in results.values() if v)
    print(f"  总计: {passed_count}/{total} 测试通过")
    print("=" * 70)

    if all_tests_pass:
        print("\n  🎉 所有测试通过!")
        sys.exit(0)
    else:
        print(f"\n  ⚠️  有 {total - passed_count} 个测试失败!")
        sys.exit(1)


if __name__ == "__main__":
    main()

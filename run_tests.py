import sys
from config import STORAGE_NODES, REPLICA_COUNT
from metadata_service import MetadataService
from block_placement import BlockPlacementStrategy
from storage_node import StorageNode
from replication_manager import ReplicationManager
from client import DFSClient


def create_system():
    ms = MetadataService()
    ps = BlockPlacementStrategy(replica_count=REPLICA_COUNT)
    sn = {}
    for nc in STORAGE_NODES:
        n = StorageNode(nc['id'], nc['host'], nc['port'], nc['capacity'])
        sn[nc['id']] = n
        ps.register_node(nc['id'], nc['host'], nc['port'], nc['capacity'])
    rm = ReplicationManager(ms, ps, sn, REPLICA_COUNT)
    cl = DFSClient(ms, ps, rm, sn)
    return ms, ps, sn, rm, cl


def section(title):
    print()
    print('=' * 60)
    print('  ' + title)
    print('=' * 60)


def check(cond, msg):
    if cond:
        print('  [OK] ' + msg)
        return True
    else:
        print('  [FAIL] ' + msg)
        return False


def main():
    all_pass = True

    section('TEST 1: Root Protection')
    ms, ps, sn, rm, cl = create_system()
    r, m = cl.delete('/')
    all_pass &= check(r == False, 'delete / should fail')
    all_pass &= check('root' in m.lower() or 'cannot' in m.lower(),
                      'error msg mentions root protection: ' + m)
    r, m = cl.rename('/', '/x')
    all_pass &= check(r == False, 'rename / to /x should fail')
    r, m = cl.rename('/a', '/')
    all_pass &= check(r == False, 'rename to / should fail')
    r, m = cl.create_directory('/test1')
    all_pass &= check(r == True, 'can still create dir: ' + m)
    r, m, _ = cl.list_directory('/')
    all_pass &= check(r == True, 'can still list root: ' + m)

    section('TEST 2: Read Parameter Validation')
    ms, ps, sn, rm, cl = create_system()
    cl.write_file('/t.txt', b'hello')
    r, m, d = cl.read_file('/t.txt', offset=-1)
    all_pass &= check(r == False, 'offset=-1 should fail')
    all_pass &= check('Invalid' in m or 'negative' in m.lower(),
                      'error mentions invalid arg: ' + m)
    all_pass &= check(len(d) == 0, 'empty data on fail')
    r, m, d = cl.read_file('/t.txt', length=-1)
    all_pass &= check(r == False, 'length=-1 should fail')
    r, m, d = cl.read_file('/t.txt', length=-999)
    all_pass &= check(r == False, 'length=-999 should fail')
    r, m, d = cl.read_file('/t.txt', offset=-5, length=-5)
    all_pass &= check(r == False, 'both negative should fail')
    r, m, d = cl.read_file('/t.txt', 0, 5)
    all_pass &= check(r == True and d == b'hello', 'normal read works')

    section('TEST 3: Write Atomicity on Failure')
    ms, ps, sn, rm, cl = create_system()
    orig = b'ORIGINAL CONTENT KEEP ME INTACT' * 100
    cl.write_file('/atomic.txt', orig)
    r, m, blocks = cl.get_file_blocks('/atomic.txt', False)
    orig_block_ids = [b.block_id for b in blocks]
    orig_size = len(orig)

    sn['node-1'].set_alive(False)
    sn['node-2'].set_alive(False)
    sn['node-3'].set_alive(False)

    new_data = b'NEW DATA SHOULD FAIL COMPLETELY' * 500
    r, m = cl.write_file('/atomic.txt', new_data)
    all_pass &= check(r == False, 'write with 3 nodes down should fail: ' + m)

    sn['node-1'].set_alive(True)
    sn['node-2'].set_alive(True)
    sn['node-3'].set_alive(True)

    r, m, data = cl.read_file('/atomic.txt')
    all_pass &= check(r == True, 'original file still readable')
    all_pass &= check(data == orig, 'original content unchanged')
    all_pass &= check(len(data) == orig_size, 'original size unchanged')

    r, m, blocks = cl.get_file_blocks('/atomic.txt', False)
    new_block_ids = [b.block_id for b in blocks]
    all_pass &= check(orig_block_ids == new_block_ids,
                      'block list unchanged: orig=' + str(orig_block_ids) + ' now=' + str(new_block_ids))

    r, m, stat = cl.stat('/atomic.txt')
    all_pass &= check(stat['size'] == orig_size, 'stat size unchanged')

    section('TEST 4: Replica Count Guarantee')
    ms, ps, sn, rm, cl = create_system()
    data = b'X' * (3 * 64 * 1024 * 1024 + 5000)
    r, m = cl.write_file('/replica_test.bin', data)
    all_pass &= check(r == True, 'big file write succeeded')

    r, m, blocks = cl.get_file_blocks('/replica_test.bin', False)
    all_pass &= check(len(blocks) == 4, 'expected 4 blocks, got ' + str(len(blocks)))
    all_ok = True
    for i, b in enumerate(blocks):
        cnt = len(b.replicas)
        unique = len(set(b.replicas))
        ok = cnt >= REPLICA_COUNT and unique == cnt
        print(f'  block {i}: {cnt} replicas (unique={unique}) nodes={b.replicas} ' + ('OK' if ok else 'BAD'))
        if not ok:
            all_ok = False
    all_pass &= check(all_ok, 'all blocks have ' + str(REPLICA_COUNT) + ' unique replicas')

    r, m, read_data = cl.read_file('/replica_test.bin')
    all_pass &= check(r == True, 'can read back')
    all_pass &= check(read_data == data, 'data integrity verified')

    section('TEST 5: Rename Cache Immediate Invalidation')
    ms, ps, sn, rm, cl = create_system()
    cl.create_directory('/olddir')
    cl.write_file('/olddir/f.txt', b'subfile_content_here')
    r, _, _ = cl.get_metadata('/olddir', True)
    all_pass &= check(r == True, 'old dir cached before rename')

    r, m = cl.rename('/olddir', '/newdir')
    all_pass &= check(r == True, 'rename succeeded')

    r, _, _ = cl.get_metadata('/olddir', True)
    all_pass &= check(r == False, '/olddir should NOT exist (use_cache=True)')

    r, _, _ = cl.get_metadata('/newdir', True)
    all_pass &= check(r == True, '/newdir SHOULD exist (use_cache=True)')

    r, _, d = cl.read_file('/olddir/f.txt')
    all_pass &= check(r == False, 'old path file should not exist')

    r, _, d = cl.read_file('/newdir/f.txt')
    all_pass &= check(r == True, 'new path file exists')
    all_pass &= check(d == b'subfile_content_here', 'new path content correct')

    r, _, children = cl.list_directory('/newdir', True)
    all_pass &= check(r == True, 'list new dir with cache')
    all_pass &= check(len(children) == 1, 'new dir has 1 child')

    r, _, children = cl.list_directory('/olddir', True)
    all_pass &= check(r == False, 'list old dir fails with cache')

    r, _, root_c = cl.list_directory('/', True)
    names = [c.name for c in root_c]
    all_pass &= check('newdir' in names, 'root has newdir (cached)')
    all_pass &= check('olddir' not in names, 'root no olddir (cached)')

    section('TEST 6: Delete Cache Immediate Invalidation')
    ms, ps, sn, rm, cl = create_system()
    cl.create_directory('/parent')
    cl.create_directory('/parent/child')
    cl.write_file('/parent/child/gc.txt', b'grandchild_file')
    for p in ['/parent', '/parent/child', '/parent/child/gc.txt']:
        r, _, _ = cl.get_metadata(p, True)
        all_pass &= check(r == True, 'exists before del: ' + p)

    cl.delete('/parent/child/gc.txt')
    cl.delete('/parent/child')
    cl.delete('/parent')

    for p in ['/parent', '/parent/child', '/parent/child/gc.txt']:
        r, _, _ = cl.get_metadata(p, True)
        all_pass &= check(r == False, 'NOT exist after del (cached): ' + p)

    r, _, root_c = cl.list_directory('/', True)
    names = [c.name for c in root_c]
    all_pass &= check('parent' not in names, 'root has no parent dir')

    r, m = cl.create_directory('/after_del_test')
    all_pass &= check(r == True, 'can create new dir after delete')

    section('TEST 7: Post-Root-Op Integrity')
    ms, ps, sn, rm, cl = create_system()
    cl.delete('/')
    cl.rename('/', '/xxx')

    paths_ok = True
    r, m = cl.create_directory('/int_test')
    paths_ok &= r
    r, m = cl.create_directory('/int_test/sub')
    paths_ok &= r
    r, m = cl.write_file('/int_test/a.txt', b'aaa')
    paths_ok &= r
    r, m = cl.write_file('/int_test/sub/b.txt', b'bbb')
    paths_ok &= r
    all_pass &= check(paths_ok, 'all create operations work after root op attempts')

    for p in ['/int_test', '/int_test/sub', '/int_test/a.txt', '/int_test/sub/b.txt']:
        r, _, _ = cl.get_metadata(p)
        all_pass &= check(r == True, 'path queryable: ' + p)

    r, _, root_c = cl.list_directory('/')
    names = [c.name for c in root_c]
    all_pass &= check('int_test' in names, 'root lists new dir correctly')

    section('SUMMARY')
    if all_pass:
        print('  ALL TESTS PASSED!')
        sys.exit(0)
    else:
        print('  SOME TESTS FAILED!')
        sys.exit(1)


if __name__ == '__main__':
    main()

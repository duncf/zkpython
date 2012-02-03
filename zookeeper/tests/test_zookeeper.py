import contextlib
import socket
import time
import threading
import unittest2 as unittest

import zookeeper

class TestZooKeeperClient(unittest.TestCase):

    SERVER_PORT = 22182

    TIMEOUT = 10000

    def setUp(self):
        self.host = "localhost:%d" % self.SERVER_PORT

        self.zk = self.create_zk(self.host, self.TIMEOUT)

        self.delete_tree('/test')
        self.zk.create('/test', 'hi', zookeeper.ACL_OPEN_ACL_UNSAFE)

    def delete_tree(self, path):
        try:
            children = self.zk.get_children(path)
        except zookeeper.exceptions.NoNodeException:
            return

        for child in children:
            self.delete_tree(path + '/' + child)
        self.zk.delete(path)

    def create_zk(self, *args, **kwargs):
        cv = threading.Condition()
        connected = []
        def watcher(event):
            with cv:
                connected.append(True)
                cv.notify()

        with cv:
            zk = zookeeper.ZooKeeper(*args, watcher=watcher, **kwargs)
            self.addCleanup(self._close_zk, zk)
            cv.wait(15.0)

        self.assertTrue(connected, 'Could not connect to host %s' %
                        (self.host,))
        return zk

    def _close_zk(self, zk):
        try:
            zk.close()
        except zookeeper.exceptions.ZooKeeperException:
            pass

    def test_init(self):
        self.assertIsInstance(self.zk, zookeeper.ZooKeeper)

    def test_client_id_and_register(self):
        """Test that another client can steal the session using a client id.

        This also tests register().
        """
        client_id = self.zk.get_client_id()
        self.assertIsInstance(client_id, tuple)
        self.assertEqual(len(client_id), 2)

        # Verify that opening the new session affects the old one.
        cv = threading.Condition()
        disconnected = []
        def disconnect_handler(event):
            with cv:
                disconnected.append(True)
                cv.notify()

        self.zk.register(disconnect_handler)

        cv.acquire()
        new_zk = self.create_zk(self.host, client_id=client_id)
        self.assertIsInstance(new_zk, zookeeper.ZooKeeper)

        new_zk.close()
        cv.wait(15.0)
        cv.release()

    def test_get_session_timeout(self):
        self.assertEqual(self.zk.get_session_timeout(), self.TIMEOUT)

    def test_add_auth_info(self):
        self.zk.add_auth_info('digest', 'user:secret')

    def test_close(self):
        self.zk.close()
        self.assertRaises(zookeeper.exceptions.ZooKeeperException,
                          self.zk.get_children, '/')

    def test_handle_reuse(self):
        self.zk.close()
        new_zk = self.create_zk(self.host)
        self.assertRaises(zookeeper.exceptions.ZooKeeperException,
                          self.zk.get_children, '/')

    def test_create_regular(self):
        self.zk.create('/test/create_regular', 'hello world',
                       zookeeper.ACL_OPEN_ACL_UNSAFE)
        data, stat = self.zk.get('/test/create_regular')
        self.assertIsInstance(stat, zookeeper.Stat)
        self.assertEqual(data, 'hello world')

    def test_create_ephemeral(self):
        self.zk.create('/test/create_ephemeral', str(int(time.time())),
                       zookeeper.ACL_OPEN_ACL_UNSAFE, ephemeral=True)

        new_zk = self.create_zk(self.host)
        stat = new_zk.exists('/test/create_ephemeral')
        self.assertIsInstance(stat, zookeeper.Stat)

        self.zk.close()
        time.sleep(0.001)

        stat = new_zk.exists('/test/create_ephemeral')
        self.assertIsNone(stat)

    def test_create_sequential(self):
        path = self.zk.create('/test/create_sequential-1-',
                               str(int(time.time())),
                               zookeeper.ACL_OPEN_ACL_UNSAFE, sequential=True)
        self.assertEqual(path, '/test/create_sequential-1-0000000000')
        path = self.zk.create('/test/create_sequential-2-',
                              str(int(time.time())),
                              zookeeper.ACL_OPEN_ACL_UNSAFE, sequential=True)
        self.assertEqual(path, '/test/create_sequential-2-0000000001')

        children = self.zk.get_children('/test')
        self.assertItemsEqual(children,
                              ['create_sequential-1-0000000000',
                               'create_sequential-2-0000000001'])

    def test_create_exists_error(self):
        self.zk.create('/test/create_exists_error', 'hello world',
                       zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self.assertRaises(zookeeper.exceptions.NodeExistsException):
            self.zk.create('/test/create_exists_error', 'hello world',
                           zookeeper.ACL_OPEN_ACL_UNSAFE)

    def test_create_perms_error(self):
        self.zk.create('/test/perms', 'hello world',
                       [zookeeper.ACL(zookeeper.ACL.PERM_READ,
                                      zookeeper.ID_ANYONE_ID_UNSAFE)])

        with self.assertRaises(zookeeper.exceptions.NoAuthException):
            self.zk.create('/test/perms/foo', 'hello world',
                           zookeeper.ACL_OPEN_ACL_UNSAFE)

    def test_create_both(self):
        path = self.zk.create('/test/create_both-', str(int(time.time())),
                       zookeeper.ACL_OPEN_ACL_UNSAFE, sequential=True,
                       ephemeral=True)
        self.assertEqual(path, '/test/create_both-0000000000')

        new_zk = self.create_zk(self.host)
        children = new_zk.get_children('/test')
        self.assertEqual(children, ['create_both-0000000000'])

        self.zk.close()
        time.sleep(0.001)

        children = new_zk.get_children('/test')
        self.assertEqual(children, [])

    def test_create_too_big(self):
        too_big = 'x' * 1024 * 1024 + 'x'
        self.assertRaises(zookeeper.exceptions.ZooKeeperException,
                          self.zk.create, '/test/create_too_big', too_big,
                          zookeeper.ACL_OPEN_ACL_UNSAFE)

    def test_delete(self):
        path = self.zk.create('/test/foo', 'hi', zookeeper.ACL_OPEN_ACL_UNSAFE)
        data, stat = self.zk.get('/test/foo')
        self.assertEqual(stat.version, 0)

        with self.assertRaises(zookeeper.exceptions.BadVersionException):
            self.zk.delete(path, 123)

        self.zk.delete(path, stat.version)

        with self.assertRaises(zookeeper.exceptions.NoNodeException):
            self.zk.delete(path)

    def test_exists(self):
        self.assertIsNone(self.zk.exists('/test/exists'))
        self.assertIsInstance(self.zk.exists('/test'), zookeeper.Stat)

    def test_get(self):
        data, stat = self.zk.get('/test')
        self.assertEqual(data, 'hi')
        self.assertIsInstance(stat, zookeeper.Stat)

    def test_get_no_node(self):
        self.assertRaises(zookeeper.exceptions.NoNodeException, self.zk.get,
                          '/doesnotexist')

    def test_set(self):
        stat = self.zk.set('/test', 'fooish bar')
        self.assertIsInstance(stat, zookeeper.Stat)
        self.assertEqual(stat.version, 1)

        data, stat2 = self.zk.get('/test')
        self.assertEqual(data, 'fooish bar')
        self.assertEqual(stat, stat2)

    def test_set_with_version(self):
        stat = self.zk.set('/test', 'fooish bar again', 0)
        self.assertIsInstance(stat, zookeeper.Stat)
        self.assertEqual(stat.version, 1)

    def test_set_wrong_version(self):
        self.assertRaises(zookeeper.exceptions.BadVersionException,
                          self.zk.set, '/test', 'bad version', 1234)

    def test_set_big_data(self):
        too_big = 'x' * 1024 * 1024 + 'x'
        self.assertRaises(zookeeper.exceptions.ZooKeeperException,
                          self.zk.set, '/test', too_big)

    def test_get_acl(self):
        stat, acls = self.zk.get_acl('/test')
        self.assertIsInstance(stat, zookeeper.Stat)
        self.assertIsInstance(acls, list)
        self.assertItemsEqual(acls, zookeeper.ACL_OPEN_ACL_UNSAFE)

    def test_set_acl(self):
        acl_list = [zookeeper.ACL(
            zookeeper.ACL.PERM_READ | zookeeper.ACL.PERM_ADMIN,
            zookeeper.ID_ANYONE_ID_UNSAFE)]
        self.zk.set_acl('/test', acl_list)

        stat, acls = self.zk.get_acl('/test')
        self.assertItemsEqual(acls, acl_list)

    def test_set_acl_version(self):
        stat = self.zk.exists('/test')
        self.zk.set_acl('/test', zookeeper.ACL_OPEN_ACL_UNSAFE,
                        version=stat.version)

        stat, acls = self.zk.get_acl('/test')
        self.assertItemsEqual(acls, zookeeper.ACL_OPEN_ACL_UNSAFE)

    def test_set_acl_error(self):
        self.assertRaises(zookeeper.exceptions.BadVersionException,
                          self.zk.set_acl, '/test',
                          zookeeper.ACL_OPEN_ACL_UNSAFE, version=999)

    def test_set_bad_acl(self):
        self.assertRaises(zookeeper.exceptions.InvalidACLException,
                          self.zk.set_acl, '/test',
                          [])

    def test_get_children(self):
        self.zk.create('/test/foo', 'hi', zookeeper.ACL_OPEN_ACL_UNSAFE)
        self.assertEqual(self.zk.get_children('/test'), ['foo'])

    def test_get_children_no_node(self):
        self.assertRaises(zookeeper.exceptions.NoNodeException,
                          self.zk.get_children, '/test/foo')

    def test_sync(self):
        self.zk.sync('/test')

    def test_get_state(self):
        self.assertEqual(self.zk.get_state(),
                         zookeeper.constants.CONNECTED_STATE)

    def test_get_state(self):
        self.assertEqual(self.zk.get_state(),
                         zookeeper.constants.CONNECTED_STATE)

    def test_get_state_not_connected(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(('127.0.0.1', 22181))
            s.listen(1)
            zk = zookeeper.ZooKeeper('127.0.0.1:22181', self.TIMEOUT)
            self.addCleanup(self._close_zk, zk)
            time.sleep(0.01)
            self.assertEqual(zk.get_state(),
                             zookeeper.constants.ASSOCIATING_STATE)
        finally:
            s.close()

    @contextlib.contextmanager
    def _watch_test(self, func, *args, **kwargs):
        # This is perhaps more of a hack than I'd like.
        class YieldVal(object):
            __slots__ = 'retval', 'event'
        yield_val = YieldVal()

        events = []
        cv = threading.Condition()
        def watcher(event):
            with cv:
                events.append(event)
                cv.notify()

        return_val = func(*args, watcher=watcher, **kwargs)

        yield_val.retval = return_val

        with cv:
            yield yield_val
            cv.wait(15.0)

        self.assertEqual(len(events), 1)
        yield_val.event = events.pop()

    def test_exists_node_created_watch(self):
        with self._watch_test(self.zk.exists, '/test/node_created_watch') as w:
            self.zk.create('/test/node_created_watch', 'hi',
                        zookeeper.ACL_OPEN_ACL_UNSAFE)
        self.assertIsNone(w.retval)
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_created_watch')
        self.assertEqual(w.event.event_type, w.event.CREATED_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)

    def test_exists_node_deleted_watch(self):
        self.zk.create('/test/node_deleted_watch', 'hi',
                    zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self._watch_test(self.zk.exists, '/test/node_deleted_watch') as w:
            self.zk.delete('/test/node_deleted_watch')
        self.assertIsInstance(w.retval, zookeeper.Stat)
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_deleted_watch')
        self.assertEqual(w.event.event_type, w.event.DELETED_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)

    def test_exists_node_data_changed_watch(self):
        self.zk.create('/test/node_data_changed_watch', 'hi',
                    zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self._watch_test(
                self.zk.exists, '/test/node_data_changed_watch') as w:
            self.zk.set('/test/node_data_changed_watch', 'hello')
        self.assertIsInstance(w.retval, zookeeper.Stat)
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_data_changed_watch')
        self.assertEqual(w.event.event_type, w.event.CHANGED_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)

    def test_get_node_deleted_watch(self):
        self.zk.create('/test/node_deleted_watch', 'hi',
                    zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self._watch_test(
                self.zk.get, '/test/node_deleted_watch') as w:
            self.zk.delete('/test/node_deleted_watch')
        self.assertIsInstance(w.retval, tuple)
        self.assertEqual(w.retval[0], 'hi')
        self.assertIsInstance(w.retval[1], zookeeper.Stat)
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_deleted_watch')
        self.assertEqual(w.event.event_type, w.event.DELETED_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)

    def test_get_node_data_changed_watch(self):
        self.zk.create('/test/node_data_changed_watch', 'hi',
                    zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self._watch_test(
                self.zk.get, '/test/node_data_changed_watch') as w:
            self.zk.set('/test/node_data_changed_watch', 'hello')
        self.assertIsInstance(w.retval, tuple)
        self.assertEqual(w.retval[0], 'hi')
        self.assertIsInstance(w.retval[1], zookeeper.Stat)
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_data_changed_watch')
        self.assertEqual(w.event.event_type, w.event.CHANGED_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)

    def test_children_node_deleted_watch(self):
        self.zk.create('/test/node_deleted_watch', 'hi',
                       zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self._watch_test(
                self.zk.get_children, '/test/node_deleted_watch') as w:
            self.zk.delete('/test/node_deleted_watch')
        self.assertEqual(w.retval, [])
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_deleted_watch')
        self.assertEqual(w.event.event_type, w.event.DELETED_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)

    def test_children_child_created_watch(self):
        self.zk.create('/test/node_children_watch', 'hi',
                       zookeeper.ACL_OPEN_ACL_UNSAFE)
        self.zk.create('/test/node_children_watch/bar', 'hi',
                       zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self._watch_test(
                self.zk.get_children, '/test/node_children_watch') as w:
            self.zk.create('/test/node_children_watch/asdf', 'asdf',
                           zookeeper.ACL_OPEN_ACL_UNSAFE)
        self.assertEqual(w.retval, ['bar'])
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_children_watch')
        self.assertEqual(w.event.event_type, w.event.CHILD_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)

    def test_children_child_deleted_watch(self):
        self.zk.create('/test/node_children_watch', 'hi',
                       zookeeper.ACL_OPEN_ACL_UNSAFE)
        self.zk.create('/test/node_children_watch/bar', 'hi',
                       zookeeper.ACL_OPEN_ACL_UNSAFE)
        with self._watch_test(
                self.zk.get_children, '/test/node_children_watch') as w:
            self.zk.delete('/test/node_children_watch/bar')
        self.assertEqual(w.retval, ['bar'])
        self.assertEqual(w.event.zookeeper, self.zk)
        self.assertEqual(w.event.path, '/test/node_children_watch')
        self.assertEqual(w.event.event_type, w.event.CHILD_EVENT)
        self.assertEqual(w.event.state, w.event.CONNECTED_STATE)


if __name__ == '__main__':
    unittest.main()

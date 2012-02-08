"""Distributed lock using ZooKeeper."""

import functools
import logging
import threading
import time

import zookeeper

RETRY_COUNT = 10

def _retry(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        for i in xrange(RETRY_COUNT):
            try:
                return func(*args, **kwargs)
            except zookeeper.exceptions.ConnectionLossException:
                continue
        raise
    return wrapped

def _sort_key_for_sequence(value):
    return value.rsplit('-', 1)[-1]


class LockException(AssertionError):
    pass


class Lock(object):

    def __init__(self, zookeeper_obj, node_dir, data, acls, unique_id=None,
                 log=None):
        self._zk = zookeeper_obj
        self._node_dir = node_dir
        self._data = data
        self._acls = acls

        if not unique_id:
            unique_id = self._zk.get_client_id()[0]
        self._prefix = 'lock-%s-' % (unique_id,)

        self._closed = False
        self._my_node = None
        self._my_node_name = None

        if not log:
            self._log = logging.getLogger('zookeeper.recipes.lock.Lock')
        else:
            self._log = log

        self._callback = None

        self._thread_lock = threading.RLock()

    def try_cleanup(self):
        """Unlock or stop acquiring the lock.

        Note: This may fail if the ZooKeeper connection is closed.  If the
        caller intends to keep the zookeeper session alive, this should be
        called repeatedly until True is returned.
        """
        self._closed = True
        if self._zk.get_state() == zookeeper.constants.EXPIRED_SESSION_STATE:
            # Unlocked since the ephemeral nodes have been cleaned up.
            return True

        try:
            self._zk.delete(self._my_node, -1)
            self._my_node = None
            self._my_node_name = None
            return True
        except zookeeper.exceptions.NoNodeException:
            return True
        except zookeeper.exceptions.ConnectionLossException:
            return False

    @_retry
    def cleanup(self):
        self._closed = True

        if self._zk.get_state() == zookeeper.constants.EXPIRED_SESSION_STATE:
            # Unlocked since the ephemeral nodes have been cleaned up.
            return True

        self._log.debug('Deleting my node %s', self._my_node)
        self._zk.delete(self._my_node, -1)
        self._my_node = None
        self._my_node_name = None

    def _find_or_create_lock_node(self):
        names = self._zk.get_children(self._node_dir)
        for name in names:
            if name.startswith(self._prefix):
                self._my_node = '/'.join((self._node_dir, name))
                self._log.debug('Found node created last time: %s',
                                self._my_node)

        if self._my_node is None:
            self._my_node = self._zk.create(
                '/'.join((self._node_dir, self._prefix)), self._data,
                self._acls, ephemeral=True, sequential=True)
            self._log.debug('Created node %s', self._my_node)

        self._my_node_name = self._my_node.rsplit('/', 1)[-1]

    def lock(self, lock_callback=None):
        """Requests to lock the lock object.

        The lock may or may not be acquired right away.  Regardless of when it
        is acquired, lock_callback() will be called when the lock is acquired.
        (Possibly before this call returns.)

        Not thread-safe.

        `lock()` will return `True` if the lock is acquired, `False` otherwise.
        """
        self._callback = lock_callback
        self._ensure_dir_exists()
        self._create_lock_node()
        return self._lock()

    @_retry
    def _ensure_dir_exists(self):
        self._ensure_node_exists(self._node_dir)

    def _ensure_node_exists(self, node):
        parent = node.rsplit('/', 1)[0]
        if parent:
            self._ensure_node_exists(parent)

        if not self._zk.exists(node):
            self._zk.create(node, '', self._acls)

    @_retry
    def _create_lock_node(self):
        if self._my_node is None:
            self._find_or_create_lock_node()

    @_retry
    def _lock(self):
        """Attempt to acquire lock.

        If we do not acquire the lock, sets a watcher to indicate it's time to
        try again. We must have already created a ZK node to claim lock.

        :Returns: True if lock is acquired, False otherwise
        """
        # Note: this could be called by the watcher while we're still in this
        # function, so we need to be careful about thread-safety and
        # idempotence.
        while True:
            # It should be impossible to get here without a node.
            assert self._my_node is not None

            children = self._zk.get_children(self._node_dir)

            # We just created children, this should never be empty.
            assert children

            children.sort(key=_sort_key_for_sequence)
            owner_name = children[0]

            # Only look at children before me.
            my_index = children.index(self._my_node_name)
            del children[my_index:]

            if children:
                last_child = children[-1]
                self._log.debug('Watching child: %s', last_child)
                stat = self._zk.exists(
                    '/'.join((self._node_dir, last_child)),
                    self._node_watcher)

                if not stat:
                    self._log.debug('Child is missing, need to try again')
                    continue
                else:
                    # Successfully started waiting for lock.
                    return False
            else:
                assert owner_name == self._my_node_name
                if self._callback is not None:
                    self._log.info('Have lock (%s), calling callback.',
                                   owner_name)
                    self._callback()
                return True

    def _node_watcher(self, event):
        self._log.debug('Watched node %s changed', event.path)
        if self._closed:
            self._log.debug('Lock closed, ignoring')
            return

        # Depending on what happened, this may add a new watcher, refresh the
        # same watcher or acquire the lock.
        self._lock()

if __name__ == '__main__':
    import random
    logging.basicConfig(level=logging.DEBUG)

    zk = zookeeper.ZooKeeper('127.0.0.1:22182')

    def my_thread(i):
        lock = Lock(zk, '/lock/asdf/asdf', 'lock-%d' % (i,),
                    zookeeper.ACL_OPEN_ACL_UNSAFE, i,
                    log=logging.getLogger('lock-%d' % (i,)))
        event = threading.Event()
        ret = lock.lock(event.set)
        if not ret and i % 2:
            # let's not wait
            logging.info('lock %d lost interest', i)
            lock.cleanup()
            return
        event.wait(600)
        if event.isSet():
            sleepy_time = random.randint(1, 6000) / 1000.0
            logging.info('lock %d is locked, sleeping for %f' % (i, sleepy_time,))
            time.sleep(sleepy_time)
            lock.cleanup()
        else:
            print '%d timed out' % (i,)

    threads = []
    for i in xrange(10):
        t = threading.Thread(target=my_thread, args=(i,))
        threads.append(t)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

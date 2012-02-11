"""Higher-level helper functions to assist with ZooKeeper operations.

:Author: duncan
"""

import functools
import time

import zookeeper

def retry(count=10, delay=0.5):

    def decorator(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            for attempt in xrange(count):
                try:
                    return func(*args, **kwargs)
                except zookeeper.exceptions.ConnectionLossException:
                    time.sleep(delay)
                    continue
            # Broke out of loop; must have been an exception.
            raise
        return wrapped
    return decorator


# Create path
@retry()
def ensure_exists(zookeeper_obj, path,
                  acls=zookeeper.ACL_OPEN_ACL_UNSAFE):
    _ensure_exists(zookeeper_obj, path, acls)

def _ensure_exists(zookeeper_obj, path, acls):
    if zookeeper_obj.exists(path):
        return

    parent = path.rsplit('/', 1)[0]
    if parent:
        _ensure_exists(zookeeper_obj, parent, acls)

    try:
        zookeeper_obj.create(path, '', acls)
    except zookeeper.exceptions.NodeExistsException:
        pass

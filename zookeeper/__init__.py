"""ZooKeeper bindings for Python.

This adds Pythonic objects around the bindings for the C API.

:Author: Duncan Findlay <duncan@duncf.ca>
"""

import _zookeeper
import zookeeper.constants
import zookeeper.exceptions

__version__ = _zookeeper.__version__

class Stat(object):

    __slots__ = ('czxid', 'mzxid', 'ctime', 'mtime', 'version', 'cversion',
                 'aversion', 'ephemeral_owner', 'data_length', 'num_children',
                 'pzxid')

    def __init__(self, czxid, mzxid, ctime, mtime, version, cversion, aversion,
                 ephemeralOwner, dataLength, numChildren, pzxid):
        self.czxid = czxid
        self.mzxid = mzxid
        self.ctime = ctime
        self.mtime = mtime
        self.version = version
        self.cversion = cversion
        self.aversion = aversion
        self.ephemeral_owner = ephemeralOwner
        self.data_length = dataLength
        self.num_children = numChildren
        self.pzxid = pzxid


class Event(object):

    __slots__ = ('event_type', 'state', 'path')

    # Event types
    CHANGED_EVENT = zookeeper.constants.CHANGED_EVENT
    CHILD_EVENT = zookeeper.constants.CHILD_EVENT
    CREATED_EVENT = zookeeper.constants.CREATED_EVENT
    DELETED_EVENT = zookeeper.constants.DELETED_EVENT
    NOTWATCHING_EVENT = zookeeper.constants.NOTWATCHING_EVENT
    SESSION_EVENT = zookeeper.constants.SESSION_EVENT

    # States
    ASSOCIATING_STATE = zookeeper.constants.ASSOCIATING_STATE
    AUTH_FAILED_STATE = zookeeper.constants.AUTH_FAILED_STATE
    CONNECTED_STATE = zookeeper.constants.CONNECTED_STATE
    CONNECTING_STATE = zookeeper.constants.CONNECTING_STATE
    EXPIRED_SESSION_STATE = zookeeper.constants.EXPIRED_SESSION_STATE

    # Maps
    EVENTS = {CHANGED_EVENT: 'CHANGED_EVENT',
	      CHILD_EVENT: 'CHILD_EVENT',
	      CREATED_EVENT: 'CREATED_EVENT',
	      DELETED_EVENT: 'DELETED_EVENT',
	      NOTWATCHING_EVENT: 'NOTWATCHING_EVENT'}
    STATES = {ASSOCIATING_STATE: 'ASSOCIATING_STATE',
	      AUTH_FAILED_STATE: 'AUTH_FAILED_STATE',
	      CONNECTED_STATE: 'CONNECTED_STATE',
	      CONNECTING_STATE: 'CONNECTING_STATE',
	      EXPIRED_SESSION_STATE: 'EXPIRED_SESSION_STATE'}

    def __init__(self, event_type, state, path=None):
	self.event_type = event_type
	self.state = state
	self.path = path

    def __repr__(self):
	event_string = self.EVENTS.get(self.event_type, str(self.event_type))
	state_string = self.STATES.get(self.state, str(self.state))
	return '<Event event=%s state=%s path=%s>' % \
	    (event_string, state_string, self.path)


class ZooKeeper(object):

    _NOTSET = object()

    def __init__(self, connect_string, session_timeout, watcher,
                 client_id=None):
        """Create a ZooKeeper client object.

        To create a ZooKeeper client object, the application needs to pass a
        connection string containing a comma separated list of host:port pairs,
        each corresponding to a ZooKeeper server.

        Session establishment is asynchronous. This constructor will initiate
        connection to the server and return immediately - potentially (usually)
        before the session is fully established. The watcher argument specifies
        the watcher that will be notified of any changes in state. This
        notification can come at any point before or after the constructor call
        has returned.

        The instantiated ZooKeeper client object will pick an arbitrary server
        from the connect_string and attempt to connect to it. If establishment
        of the connection fails, another server in the connect string will be
        tried (the order is non-deterministic, as we random shuffle the list),
        until a connection is established. The client will continue attempts
        until the session is explicitly closed (or the session is expired by
        the server).

        Added in 3.2.0: An optional "chroot" suffix may also be appended to the
        connection string. This will run the client commands while interpreting
        all paths relative to this root (similar to the unix chroot command).

        It is possible to connect using an existing client connection. Use
        `get_client_id()` to get a (session_id, password) tuple for an
        established client connection to get parameters. This tuple can be
        passed to ``__init__()`` as `client_id` to reconnect.

        :Parameters:
            - `connect_string`: comma separated host:port pairs, each
              corresponding to a zk server.
            - `session_timeout`: session timeout in milliseconds
            - `watcher`: a watcher function which will be notified of state
              changes -- it will be passed an Event object
            - `client_id`: session id and passwd tuple to use if reconnecting

        :Exceptions:
            - TODO
        """

	watcher = self._wrap_watcher(watcher)

        if client_id:
            session_id, session_passwd = client_id
            self._zk_handle = _zookeeper.init(
                connect_string, watcher, session_timeout, session_id,
                session_passwd)
        else:
            self._zk_handle = _zookeeper.init(
                connect_string, watcher, session_timeout)

        assert isinstance(self._zk_handle, int)

    def _wrap_watcher(self, watcher):
	"""Create a watcher function of the type expected by the C API.

	Python watcher functions interfacing with this library should expect
	to receive only an Event object.
	"""
	def _wrapped(zk_handle, event_type, state, path):
	    assert zk_handle == self._zk_handle
	    event = Event(event_type, state, path)
	    return watcher(event)
	return _wrapped

    def get_client_id(self):
        """Get client ID (username, passwd) tuple.

        Not thread-safe (TODO: Why?).
        """
        val = _zookeeper.client_id(self._zk_handle)
        assert isinstance(val, tuple) and len(val) == 2
        return val

    def get_session_timeout(self):
        """Get session timeout value.

        The negotiated session timeout for this ZooKeeper client instance. The
        value returned is not valid until the client connects to a server and
        may change after a re-connect. This method is NOT thread safe.
        """
        val = _zookeeper.recv_timeout(self._zk_handle)
        assert isinstance(val, int)
        return val

    def add_auth_info(self, scheme, auth):
        """Add the specified scheme:auth information to this connection.

        This method is NOT thread safe.

        :Parameters:
            - `scheme`
            - `auth`
        """
        # TODO: add_auth callback?
        error_code = _zookeeper.add_auth(self._zk_handle, scheme, auth, None)
        assert error_code == zookeeper.constants.OK

    def register(self, watcher):
        """Specify the default watcher for the connection.

        This overrides the one specified during construction.
        """
        val = _zookeeper.set_watcher(
	    self._zk_handle, self._wrap_watcher(watcher))
        assert val is None

    def close(self):
        """Close this client object.

        Once the client is closed, its session becomes invalid. All the
        ephemeral nodes in the ZooKeeper server associated with the session
        will be removed. The watches left on those nodes (and on their parents)
        will be triggered.
        """
        val = _zookeeper.close(self._zk_handle)
        # Note: requires change in zookeeper.c
        assert val == zookeeper.constants.OK

    def create(self, path, data, acls, ephemeral=False, sequential=False,
               callback=_NOTSET):
        """Create a node with the given path.

        The node data will be the given data, and node acl will be the given
        acls.

        The `ephemeral` and `sequential` arguments specify what types of nodes
        will be created.

        An ephemeral node will be removed by the ZooKeeper automatically when
        the session associated with the creation of the node expires.

        The flags argument can also specify to create a sequential node. The
        actual path name of a sequential node will be the given path plus a
        suffix "i" where i is the current sequential number of the node. The
        sequence number is always fixed length of 10 digits, 0 padded. Once
        such a node is created, the sequential number will be incremented by
        one.

        This operation, if successful, will trigger all the watches left on the
        node of the given path by `exists()` and `get_data()` API calls, and
        the watches left on the parent node by `get_children()` API calls.

        The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
        Arrays larger than this will cause a ZooKeeperExecption to be
        thrown. (TODO: what type?)

        If `callback` is specified (even if it is set to None), the create will
        be done asynchronously.

        :Parameters:
            - `path`: Name of node to create
            - `data`: Data to store in node.
            - `acls`: List of ACLs for the node.
            - `ephemeral`: Created node should be ephemeral.
            - `sequential`: Created node should be sequential.

        :Exceptions:
            - `NodeExistsException`: if a node with the same path already
              exists (impossible if `sequential` is True)
            - `NoNodeException`: if parent node does not exist
            - `NoChildrenForEphemeralsException`: if parent node of the path is
              ephemeral (ephemeral nodes may not have children)
            - `ZooKeeperException`: *TODO* if size is too big

        :Returns:
            Path for created node
        """
        flags = 0
        if ephemeral:
            flags |= zookeeper.constants.EPHEMERAL
        if sequential:
            flags |= zookeeper.constants.SEQUENCE

        created_path = _zookeeper.create(
            self._zk_handle, path, data, acls, flags)

        return created_path

    def delete(self, path, version=-1):
        """Delete the node with the given path.

        The call will succeed if such a node exists, and the given version
        matches the node's `version` (if given).

        This operation, if successful, will trigger all the watches on the node
        of the given path left by `exists()` API calls, and the watches on the
        parent node left by `get_children()` API calls.

        :Parameters:
            - `path`: the path of the node to be deleted
            - `version`: the expected node version (or `None` to ignore
              version)

        :Exceptions:
            - `NoNodeException`: node to be deleted does not exist.
            - `BadVersionException`: given version does not match the node's
              version
            - `NotEmptyException`: node cannot be deleted because it has
              children
        """
        val = _zookeeper.delete(self._zk_handle, path, version)
        assert val == zookeeper.constants.OK

    def exists(self, path, watcher=None):
        """Return the state of the node of the given path.

        If the watcher is not None and the call is successful (no exception is
        thrown), a watch will be left on the node with the given path. The
        watch will be triggered by a successful operation that creates/delete
        the node or sets the data on the node.

        :Parameters:
            - `path`: node path
            - `watcher`: (optional) watcher function

        :Returns:
            Stat object of the node of the given path; returns None if no such
            node exists.

        :Exceptions:
            TODO
        """
        if watcher:
            stat = _zookeeper.exists(self._zk_handle, path,
				     self._wrap_watcher(watcher))
        else:
            stat = _zookeeper.exists(self._zk_handle, path)

        return Stat(**stat)

    def get(self, path, watcher=None):
        """Return the data and the stat of the node of the given path.

        If the watch is true and the call is successful (no exception is
        thrown), a watch will be left on the node with the given path. The
        watch will be triggered by a successful operation that sets data on
        the node, or deletes the node.

        :Parameters:
            - `path`: node path
            - `watcher`: (optional) watcher function

        :Returns:
            (data, stat) for the node

        :Exceptions:
            - `NoNodeException`: given path does not exist
        """
        if watcher:
            data, stat = _zookeeper.get(self._zk_handle, path,
					self._wrap_watcher(watcher))
        else:
            data, stat = _zookeeper.get(self._zk_handle, path)
        return data, Stat(**stat)

    def set(self, path, data, version=-1):
        """Set the data for the node of the given path.

        Set the data for the node of the given path if such a node exists and
        the given version matches the version of the node (if the given version
        is -1 (default), node version is ignored).

        This operation, if successful, will trigger all the watches on the node
        of the given path left by `get()` calls.

        The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
        Arrays larger than this will cause a `ZooKeeperExecption` to be thrown.

        :Parameters:
            - `path`: path of the node
            - `data`: data to set
            - `version`: expected version (-1 (default) ignored)

        :Returns:
            Stat object for the node

        :Exceptions:
            - `NoNodeException`: thrown if no node with the given path exists
            - `BadVersionException`: thrown if the version does not match
        """
        stat_dict = _zookeeper.set2(self._zk_handle, path, data, version)
        return Stat(**stat_dict)

    def get_acl(self, path):
        """Return the ACL and stat of the node of the given path.

        :Parameters:
            - `path`: the given path for the node

        :Returns:
	    (Stat object, acl list) for the node

        :Exceptions:
            - `NoNodeException` if the node does not exist.
	"""
	stat_dict, acl_list = _zookeeper.get_acl(self._zk_handle, path)
	return Stat(**stat_dict), acl_list

    def set_acl(self, path, version, acls):
	"""Set the ACL for the node of the given path.

        Set the ACL for the node of the given path if such a node exists and
	the given version matches the version of the node.

        :Parameters:
            - `path`: path to node
	    - `version`: expected node version to update
	    - `acls`: list of acls to apply

        :Returns:
	    - Stat object for the node

        :Throws:
	    - `InvalidACLException`: if the acl is invalid
	    - `NoNodeException`: if the given path does not exist
	    - `BadVersionException`: if the node is of a different version
	"""
	stat_dict = _zookeeper.set_acl(self._zk_handle, path, version, acls)
	return Stat(**stat_dict)

    def get_children(self, path, watcher=None):
	"""Return the list of the children of the node of the given path.

        If the `watcher` function is provided and the call is successful (no
	exception is thrown), a watch will be left on the node with the given
	path. The watch will be triggered by a successful operation that
	deletes the node of the given path or creates/delete a child under the
	node.

        The list of children returned is not sorted and no guarantee is
	provided as to its natural or lexical order.

        :Parameters:
	    - `path`: path to node
	    - `watcher`: (optional) watcher function to call when changes occur

        :Return:
	    list of children nodes

	:Exceptions:
	    - `NoNodeException`: if node does not exist
        """
	if watcher:
	    watcher = self._wrap_watcher(watcher)
	children = _zookeeper.get_children(self._zk_handle, path, watcher)
	assert isinstance(children, list)
	return children

    def sync(self, path):
	"""Asynchronous sync.

        Flushes channel between process and leader.
	"""
	val = _zookeeper.async(self._zk_handle, path)
	assert val == zookeeper.constants.OK

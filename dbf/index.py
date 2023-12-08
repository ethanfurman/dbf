from bisect import bisect_left, bisect_right
from functools import partial
import struct
import weakref

class IndexFile(_Navigation):
    pass

class BytesType(object):

    def __init__(self, offset):
        self.offset = offset

    def __get__(self, inst, cls=None):
        if inst is None:
            return self
        start = self.offset
        end = start + self.size
        byte_data = inst._data[start:end]
        return self.from_bytes(byte_data)

    def __set__(self, inst, value):
        start = self.offset
        end = start + self.size
        byte_data = self.to_bytes(value)
        inst._data = inst._data[:start] + byte_data + inst._data[end:]


class IntBytesType(BytesType):
    """
    add big_endian and neg_one to __init__
    """

    def __init__(self, offset, big_endian=False, neg_one_is_none=False, one_based=False):
        self.offset = offset
        self.big_endian = big_endian
        self.neg_one_is_none = neg_one_is_none
        self.one_based = one_based

    def from_bytes(self, byte_data):
        if self.neg_one_is_none and byte_data == '\xff' * self.size:
            return None
        if self.big_endian:
            value = struct.unpack('>%s' % self.code, byte_data)[0]
        else:
            value = struct.unpack('<%s' % self.code, byte_data)[0]
        if self.one_based:
            # values are stored one based, convert to standard Python zero-base
            value -= 1
        return value

    def to_bytes(self, value):
        if value is None:
            if self.neg_one_is_none:
                return '\xff\xff'
            raise DbfError('unable to store None in %r' % self.__name__)
        limit = 2 ** (self.size * 8) - 1
        if self.one_based:
            limit -= 1
        if value > 2 ** limit:
            raise DataOverflowError("Maximum Integer size exceeded.  Possible: %d.  Attempted: %d" % (limit, value))
        if self.one_based:
            value += 1
        if self.big_endian:
            return struct.pack('>%s' % self.code, value)
        else:
            return struct.pack('<%s' % self.code, value)


class Int8(IntBytesType):
    """
    1-byte integer
    """

    size = 1
    code = 'B'


class Int16(IntBytesType):
    """
    2-byte integer
    """

    size = 2
    code = 'H'


class Int32(IntBytesType):
    """
    4-byte integer
    """

    size = 4
    code = 'L'


class Bytes(BytesType):

    def __init__(self, offset, size=0, fill_to=0, strip_null=False):
        if not (size or fill_to):
            raise DbfError("either size or fill_to must be specified")
        self.offset = offset
        self.size = size
        self.fill_to = fill_to
        self.strip_null = strip_null

    def from_bytes(self, byte_data):
        if self.strip_null:
            return byte_data.rstrip('\x00')
        else:
            return byte_data

    def to_bytes(self, value):
        if not isinstance(value, bytes):
            raise DbfError('value must be bytes [%r]' % value)
        if self.strip_null and len(value) < self.size:
            value += '\x00' * (self.size - len(value))
        return value


class DataBlock(object):
    """
    adds _data as a str to class
    binds variable name to BytesType descriptor
    """

    def __init__(self, size):
        self.size = size

    def __call__(self, cls):
        fields = []
        initialized = stringified = False
        for name, thing in cls.__dict__.items():
            if isinstance(thing, BytesType):
                thing.__name__ = name
                fields.append((name, thing))
            elif name in ('__init__', '__new__'):
                initialized = True
            elif name in ('__repr__', ):
                stringified = True
        fields.sort(key=lambda t: t[1].offset)
        for _, field in fields:
            offset = field.offset
            if not field.size:
                field.size = field.fill_to - offset
        total_field_size = field.offset + field.size
        if self.size and total_field_size > self.size:
            raise DbfError('Fields in %r are using %d bytes, but only %d allocated' % (cls, total_field_size, self.size))
        total_field_size = self.size or total_field_size
        cls._data = str('\x00' * total_field_size)
        cls.__len__ = lambda s: len(s._data)
        cls._size_ = total_field_size
        if not initialized:
            def init(self, data):
                if len(data) != self._size_:
                    raise Exception('%d bytes required, received %d' % (self._size_, len(data)))
                self._data = data
            cls.__init__ = init
        if not stringified:
            def repr(self):
                clauses = []
                for name, _ in fields:
                    value = getattr(self, name)
                    if isinstance(value, str) and len(value) > 12:
                        value = value[:9] + '...'
                    clauses.append('%s=%r' % (name, value))
                return ('%s(%s)' % (cls.__name__, ', '.join(clauses)))
            cls.__repr__ = repr
        return cls


class LruCache(object):
    """
    keep the most recent n items in the dict

    based on code from Raymond Hettinger: http://stackoverflow.com/a/8334739/208880
    """

    class Link(object):
        __slots__ = 'prev_link', 'next_link', 'key', 'value'
        def __init__(self, prev=None, next=None, key=None, value=None):
            self.prev_link, self.next_link, self.key, self.value = prev, next, key, value

        def __iter__(self):
            return iter((self.prev_link, self.next_link, self.key, self.value))

        def __repr__(self):
            value = self.value
            if isinstance(value, str) and len(value) > 15:
                value = value[:12] + '...'
            return 'Link<key=%r, value=%r>' % (self.key, value)

    def __init__(self, maxsize, func=None):
        self.maxsize = maxsize
        self.mapping = {}
        self.tail = self.Link()                      # oldest
        self.head = self.Link(self.tail)             # newest
        self.head.prev_link = self.tail
        self.func = func
        if func is not None:
            self.__name__ = func.__name__
            self.__doc__ = func.__doc__

    def __call__(self, *func):
        if self.func is None:
            [self.func] = func
            self.__name__ = func.__name__
            self.__doc__ = func.__doc__
            return self
        mapping, head, tail = self.mapping, self.head, self.tail
        link = mapping.get(func, head)
        if link is head:
            value = self.func(*func)
            if len(mapping) >= self.maxsize:
                old_prev, old_next, old_key, old_value = tail.next_link
                tail.next_link = old_next
                old_next.prev_link = tail
                del mapping[old_key]
            behind = head.prev_link
            link = self.Link(behind, head, func, value)
            mapping[func] = behind.next_link = head.prev_link = link
        else:
            link_prev, link_next, func, value = link
            link_prev.next_link = link_next
            link_next.prev_link = link_prev
            behind = head.prev_link
            behind.next_link = head.prev_link = link
            link.prev_link = behind
            link.next_link = head
        return value


class Idx(object):
    # default numeric storage is little-endian
    # numbers used as key values, and the 4-byte numbers in leaf nodes are big-endian

    @DataBlock(512)
    class Header(object):
        root_node = Int32(0)
        free_node_list = Int32(4, neg_one_is_none=True)
        file_size = Int32(8)
        key_length = Int16(12)
        index_options = Int8(14)
        index_signature = Int8(15)
        key_expr = Bytes(16, 220, strip_null=True)
        for_expr = Bytes(236, 220, strip_null=True)

    @DataBlock(512)
    class Node(object):
        attributes = Int16(0)
        num_keys = Int16(2)
        left_peer = Int32(4, neg_one_is_none=True)
        right_peer = Int32(8, neg_one_is_none=True)
        pool = Bytes(12, fill_to=512)
        def __init__(self, byte_data, node_key, record_key):
            if len(byte_data) != 512:
                raise DbfError("incomplete header: only received %d bytes" % len(byte_data))
            self._data = byte_data
            self._node_key = node_key
            self._record_key = record_key
        def is_leaf(self):
            return self.attributes in (2, 3)
        def is_root(self):
            return self.attributes in (1, 3)
        def is_interior(self):
            return self.attributes in (0, 1)
        def keys(self):
            result = []
            if self.is_leaf():
                key = self._record_key
            else:
                key = self._node_key
            key_len = key._size_
            for i in range(self.num_keys):
                start = i * key_len
                end = start + key_len
                result.append(key(self.pool[start:end]))
            return result

    def __init__(self, table, filename, size_limit=100):
        self.table = weakref.ref(table)
        self.filename = filename
        self.limit = size_limit
        with open(filename, 'rb') as idx:
            self.header = header = self.Header(idx.read(512))
            # offset = 512
            @DataBlock(header.key_length+4)
            class NodeKey(object):
                key = Bytes(0, header.key_length)
                rec_no = Int32(header.key_length, big_endian=True)
            @DataBlock(header.key_length+4)
            class RecordKey(object):
                key = Bytes(0, header.key_length)
                rec_no = Int32(header.key_length, big_endian=True, one_based=True)
            self.NodeKey = NodeKey
            self.RecordKey = RecordKey
            # set up root node
            idx.seek(header.root_node)
            self.root_node = self.Node(idx.read(512), self.NodeKey, self.RecordKey)
        # set up node reader
        self.read_node = LruCache(maxsize=size_limit, func=self.read_node)
        # set up iterating members
        self.current_node = None
        self.current_key = None

    def __iter__(self):
        # find the first leaf node
        table = self.table()
        if table is None:
            raise DbfError('the database linked to %r has been closed' % self.filename)
        node = self.root_node
        if not node.num_keys:
            yield
            return
        while "looking for a leaf":
            # travel the links down to the first leaf node
            if node.is_leaf():
                break
            node = self.read_node(node.keys()[0].rec_no)
        while "traversing nodes":
            for key in node.keys():
                yield table[key.rec_no]
            next_node = node.right_peer
            if next_node is None:
                return
            node = self.read_node(next_node)
    forward = __iter__

    def read_node(self, offset):
        """
        reads the sector indicated, and returns a Node object
        """
        with open(self.filename, 'rb') as idx:
            idx.seek(offset)
            return self.Node(idx.read(512), self.NodeKey, self.RecordKey)

    def backward(self):
        # find the last leaf node
        table = self.table()
        if table is None:
            raise DbfError('the database linked to %r has been closed' % self.filename)
        node = self.root_node
        if not node.num_keys:
            yield
            return
        while "looking for last leaf":
            # travel the links down to the last leaf node
            if node.is_leaf():
                break
            node = self.read_node(node.keys()[-1].rec_no)
        while "traversing nodes":
            for key in reversed(node.keys()):
                yield table[key.rec_no]
            prev_node = node.left_peer
            if prev_node is None:
                return
            node = self.read_node(prev_node)




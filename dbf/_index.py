class IndexFile(object):
    "provides read/write access to a custom index file."
    _last_header_block = 0       # block to write new master index entries
    _free_node_chain = 0         # beginning of free node chain
    indices = None              # indices in this file
    def __init__(self, dbf):
        "creates .pdx file if it doesn't exist"
        filename = os.path.splitext(dbf.filename)[0]
        filename += '.pdx'
        if not os.path.exists(filename):
            self.index_file = open(filename, 'r+b')
            self.index_file.write('\xea\xaf\x37\xbf'    # signature
                '\x00'*8                                # two non-existant lists
                '\x00'*500)                             # and no indices
            return
        index_file = self.index_file = open(filename, 'r+b')
        header = index_file.read(512)
        if header[:4] != '\xea\xaf\x37\xbf':
            raise IndexFileError("Wrong signature -- unable to use index file %r" % filename)
        more_header = unpack_long_int(header[4:8])
        free_nodes = self.free_nodes = unpack_long_int(header[8:12])
        indices = header[12:]
        while more_header:
            self.last_header_block = more_header    # block to add new indices to
            index_file.seek(more_header)
            header = index_file.read(512)
            more_header = unpack_long_int(header[:4])
            indices += header[4:]

class ContainedIndex(_Navigation):
    "an individual index in a .pdx (plentiful index) file"
    def __init__(self, table, key_func, key_text, index_file, root_node, id):
        self.__doc__ = key_text
        self._meta = table._meta      # keep for other info functions
        self.key = key_func
        self.file = index_file
        self.root = root_node
        self.id = id




    def __call__(self, record):
        rec_num = recno(record)
        key = self.key(record)
        if not isinstance(key, tuple):
            key = (key, )
        if rec_num in self._records:
            if self._records[rec_num] == key:
                return
            old_key = self._records[rec_num]
            vindex = bisect_left(self._values, old_key)
            self._values.pop(vindex)
            self._rec_by_val.pop(vindex)
            del self._records[rec_num]
            assert rec_num not in self._records
        if key == (DoNotIndex, ):
            return
        vindex = bisect_right(self._values, key)
        self._values.insert(vindex, key)
        self._rec_by_val.insert(vindex, rec_num)
        self._records[rec_num] = key
    def __contains__(self, data):
        if not isinstance(data,(Record, RecordTemplate, tuple, dict)):
            raise TypeError("%r is not a record, templace, tuple, nor dict" % (data, ))
        if isinstance(data, Record) and source_table(data) is self._table:
            return recno(data) in self._records
        else:
            try:
                value = self.key(data)
                return value in self._values
            except Exception:
                for record in self:
                    if record == data:
                        return True
                return False
    def __getitem__(self, key):
        if isinstance(key, int):
            count = len(self._values)
            if not -count <= key < count:
                raise IndexError("Record %d is not in list." % key)
            rec_num = self._rec_by_val[key]
            return self._table[rec_num]
        elif isinstance(key, slice):
            result = List()
            start, stop, step = key.start, key.stop, key.step
            if start is None: start = 0
            if stop is None: stop = len(self._rec_by_val)
            if step is None: step = 1
            if step < 0:
                start, stop = stop - 1, -(stop - start + 1)
            for loc in range(start, stop, step):
                record = self._table[self._rec_by_val[loc]]
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            return result
        elif isinstance (key, (str, unicode, tuple, Record)):
            if isinstance(key, Record):
                key = self.key(key)
            elif not isinstance(key, tuple):
                key = (key, )
            loc = self.find(key)
            if loc == -1:
                raise KeyError(key)
            return self._table[self._rec_by_val[loc]]
        else:
            raise TypeError('indices must be integers, match objects must by strings or tuples')
    def __enter__(self):
        self._table.open()
        return self
    def __exit__(self, *exc_info):
        self._table.close()
        return False
    def __iter__(self):
        return Iter(self)
    def __len__(self):
        return len(self._records)
    def _clear(self):
        "removes all entries from index"
        self._values[:] = []
        self._rec_by_val[:] = []
        self._records.clear()
    def _nav_check(self):
        "raises error if table is closed"
        if self._table._meta.status == CLOSED:
            raise DbfError('indexed table %s is closed' % self.filename)
    def _partial_match(self, target, match):
        target = target[:len(match)]
        if isinstance(match[-1], (str, unicode)):
            target = list(target)
            target[-1] = target[-1][:len(match[-1])]
            target = tuple(target)
        return target == match
    def _purge(self, rec_num):
        value = self._records.get(rec_num)
        if value is not None:
            vindex = bisect_left(self._values, value)
            del self._records[rec_num]
            self._values.pop(vindex)
            self._rec_by_val.pop(vindex)
    def _reindex(self):
        "reindexes all records"
        for record in self._table:
            self(record)
    def _search(self, match, lo=0, hi=None):
        if hi is None:
            hi = len(self._values)
        return bisect_left(self._values, match, lo, hi)
    def index(self, record, start=None, stop=None):
        """returns the index of record between start and stop
        start and stop default to the first and last record"""
        if not isinstance(record, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(record))
        self._still_valid_check()
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        for i in range(start, stop):
            if record == (self[i]):
                return i
        else:
            raise NotFoundError("dbf.Index.index(x): x not in Index", data=record)
    def query(self, criteria):
        """criteria is a callback that returns a truthy value for matching record"""
        return pql(self, criteria)
    def search(self, match, partial=False):
        "returns dbf.List of all (partially) matching records"
        result = List()
        if not isinstance(match, tuple):
            match = (match, )
        loc = self._search(match)
        if loc == len(self._values):
            return result
        while loc < len(self._values) and self._values[loc] == match:
            record = self._table[self._rec_by_val[loc]]
            result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            loc += 1
        if partial:
            while loc < len(self._values) and self._partial_match(self._values[loc], match):
                record = self._table[self._rec_by_val[loc]]
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
                loc += 1
        return result



from core.spark import WithSpark


def _hdfs_path(func):
    def inner(cls, path, *args, **kwargs):
        path = cls.get_spark()._jvm.org.apache.hadoop.fs.Path(path)
        return func(cls, path, *args, **kwargs)

    return inner


class HDFileSystem:

    __doc__ = """ HDFS with spark

    official api doc: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html

    """

    _spark = None
    _fs = None

    @classmethod
    def get_spark(cls):
        if cls._spark is None:
            cls._spark = WithSpark._init_spark()
        return cls._spark

    @classmethod
    def get_fs(cls):
        if cls._fs is None:
            spark = cls.get_spark()
            cls._fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            )
        return cls._fs

    @classmethod
    @_hdfs_path
    def rm(cls, path: str, recursive=False):
        cls.get_fs().delete(path, recursive)

    @classmethod
    @_hdfs_path
    def mkdirs(cls, path: str):
        cls.get_fs().mkdirs(path)

    @classmethod
    @_hdfs_path
    def exists(cls, path: str):
        return cls.get_fs().exists(path)

    @classmethod
    @_hdfs_path
    def open(cls, path: str, mode: str = 'r'):
        return HDFSIOWrapper(path, cls.get_fs(), mode)


class HDFSInputStream:
    def __init__(self, input_stream):
        self._stream = input_stream

    def read(self):
        # return self._stream.readUTF()
        return '\n'.join(self.readlines())

    def seek(self, pos: int):
        self._stream.seek(pos)

    def readlines(self):
        lines = []
        while 1:
            line = self._stream.readLine()
            if line is None:
                break
            lines.append(line)
        return lines

    def close(self):
        return self._stream.close()


class HDFSOutputStream:
    def __init__(self, output_stream):
        self._stream = output_stream

    def write(self, s: str):
        self._stream.writeBytes(s)

    def writeln(self, s: str):
        self._stream.writeBytes(s + '\n')

    def flush(self):
        self._stream.flush()

    def close(self):
        self._stream.close()


class HDFSIOWrapper(HDFSInputStream, HDFSOutputStream):
    def __init__(self, path, fs, mode: str):
        if mode == 'r':
            stream = fs.open(path)
            HDFSInputStream.__init__(self, stream)
        elif mode == 'w':
            stream = fs.create(path)
            HDFSOutputStream.__init__(self, stream)
        else:
            raise ValueError(f'known mode: {mode}')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

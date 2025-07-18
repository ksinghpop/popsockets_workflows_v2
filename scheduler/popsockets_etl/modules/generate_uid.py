import os
import socket
import struct
import time
import threading
import hashlib

class ObjectIdGenerator:
    _lock = threading.Lock()
    _counter = struct.unpack(">I", os.urandom(4))[0] & 0xFFFFFF  # 3-byte counter

    def __init__(self):
        self.machine_id = self._get_machine_id()
        self.pid = os.getpid() & 0xFFFF  # 2-byte process id

    def _get_machine_id(self):
        hostname = socket.gethostname()
        hash_bytes = hashlib.md5(hostname.encode()).digest()
        return hash_bytes[:3]  # 3 bytes

    def generate(self):
        with self._lock:
            ts = int(time.time())
            counter = ObjectIdGenerator._counter
            ObjectIdGenerator._counter = (ObjectIdGenerator._counter + 1) & 0xFFFFFF  # wrap at 3 bytes

        # Pack components
        ts_bytes = struct.pack(">I", ts)
        pid_bytes = struct.pack(">H", self.pid)
        counter_bytes = struct.pack(">I", counter)[1:]  # take last 3 bytes

        oid = ts_bytes + self.machine_id + pid_bytes + counter_bytes
        return oid.hex()  # return 24-char hex string like MongoDB

# Example usage
generator = ObjectIdGenerator()
print(generator.generate())

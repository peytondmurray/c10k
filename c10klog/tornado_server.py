import asyncio
import logging
import pickle
import struct

from rich.logging import RichHandler
from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpserver import TCPServer

logging.basicConfig(
    level="DEBUG",
    format="[pid=%(process)d] %(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)


class LogServer(TCPServer):
    async def handle_stream(self, stream: IOStream, address: str = "localhost"):
        while True:
            try:
                chunk = await stream.read_bytes(4)
                if len(chunk) < 4:
                    break
                struct_length = struct.unpack(">L", chunk)[0]
                chunk = await stream.read_bytes(struct_length)

                obj = self.deserialize(chunk)
                record = logging.makeLogRecord(obj)

                if isinstance(record, logging.LogRecord):
                    logger = logging.getLogger(
                        getattr(self, "logger_name", record.name)
                    )
                    logger.handle(record)

            except StreamClosedError:
                break

    def deserialize(self, data):
        return pickle.loads(data)


async def main():
    server = LogServer()
    server.listen(logging.handlers.DEFAULT_TCP_LOGGING_PORT)
    await asyncio.Event().wait()


asyncio.run(main())

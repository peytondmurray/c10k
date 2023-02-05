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

driver_logger = logging.getLogger(__name__)


class BadChunkException(Exception):
    pass


class LogServer(TCPServer):
    async def handle_stream(self, stream: IOStream, address: str = "localhost"):
        while True:
            try:
                record = await self.read_logs_from_stream(stream)

                if isinstance(record, logging.LogRecord):
                    logger = logging.getLogger(
                        getattr(self, "logger_name", record.name)
                    )
                    logger.handle(record)

            except StreamClosedError:
                break

            except BadChunkException:
                break

    def deserialize(self, data: bytes):
        """Deserialize a log record instance.

        Args:
            data: A bunch of bytes representing a LogRecord

        Returns:
            A deserialized LogRecord instance
        """
        return pickle.loads(data)

    async def read_logs_from_stream(self, stream: IOStream) -> logging.LogRecord:
        """Read serialized LogRecords from a stream.

        Args:
            stream: Stream containing serialized LogRecords. The first four bytes are the length of
            the serialized LogRecord.

        Returns:
            The next log record read from the stream
        """
        chunk = await stream.read_bytes(4)
        if len(chunk) < 4:
            driver_logger.error("Couldn't read a chunk")
            raise BadChunkException
        struct_length = struct.unpack(">L", chunk)[0]
        chunk = await stream.read_bytes(struct_length)
        obj = self.deserialize(chunk)
        return logging.makeLogRecord(obj)


async def main():
    server = LogServer()
    server.listen(logging.handlers.DEFAULT_TCP_LOGGING_PORT)
    await asyncio.Event().wait()


asyncio.run(main())

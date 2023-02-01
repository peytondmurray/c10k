import asyncio
import logging
import pickle

from rich.logging import RichHandler
from tornado.iostream import StreamClosedError
from tornado.tcpserver import TCPServer

logging.basicConfig(
    level="DEBUG", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)


class LogServer(TCPServer):
    async def handle_stream(self, stream, address: str = "localhost"):
        while True:
            try:
                data = await stream.read_until(b"\n")
                record = self.deserialize(data)

                if self.logger_name is not None:
                    name = self.logger_name
                else:
                    name = record.name

                logger = logging.getLogger(name)
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

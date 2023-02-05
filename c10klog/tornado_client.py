from collections.abc import Awaitable
from concurrent.futures import ThreadPoolExecutor
# import asyncio
import pickle
import logging
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpclient import TCPClient



class TCPClientHandler(logging.Handler):
    """Lazyily connect via TCP to the driver when a log message is emitted."""
    def __init__(
        self,
        host: str,
        port: int = logging.handlers.DEFAULT_TCP_LOGGING_PORT
    ):
        super().__init__(self)
        self.client = TCPClient()
        self.host = host
        self.port = port
        self.stream = None

    async def emit(self, record: logging.LogRecord) -> Awaitable[None]:
        """Emit a log record.

        Args:
            record:
        """
        try:
            msg = self.format(record)
            if not self.stream:
                self.stream = await self.client.connect(self.host, self.port)

            if self.stream:
                await self.stream.write(
                    await self.serialize(msg)
                )
        except Exception as e:
            print(e)

    async def serialize(log: logging.LogRecord) -> Awaitable[bytes]:
        """Serialize a log record in a separate thread, and await the result.

        Args:
            log: Log record to be serialized

        Returns:
            The serialized log record wrapped in an awaitable
        """

        return await IOLoop.current().run_in_executor(None, pickle.dumps, log)
        # with ThreadPoolExecutor() as executor:
            # return await asyncio.run_in_executor(executor, pickle.dumps, log)

    async def handle(self, record: logging.LogRecord) -> bool:
        rv = self.filter(record)
        if rv:
            self.acquire()
            try:
                await self.emit(record)
            finally:
                self.release()
        return rv


# socket_handler = logging.handlers.SocketHandler(
#     "localhost", logging.handlers.DEFAULT_TCP_LOGGING_PORT
# )

stream_handler = logging.StreamHandler()
logging.basicConfig(
    level="DEBUG",
    format="%(asctime)s %(levelname)s %(name)s::%(message)s",
    datefmt="[%X]",
    handlers=[
        socket_handler,
        stream_handler,
    ],
)

# while True:
#     logging.info("Jackdaws love my big sphinx of quartz.", extra={"pid": os.getpid()})
#     time.sleep(1)

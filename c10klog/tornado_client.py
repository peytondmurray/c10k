from typing import Optional
from collections.abc import Awaitable
import asyncio
import pickle
import logging
from tornado.tcpclient import TCPClient
import time


class TCPClientHandler(logging.Handler):
    """Lazily connect via TCP to the driver when a log message is emitted."""
    def __init__(
        self,
        host: Optional[str] = 'localhost',
        port: Optional[int] = logging.handlers.DEFAULT_TCP_LOGGING_PORT,
    ):
        super().__init__()
        self.client = TCPClient()
        self.host = host
        self.port = port
        self.stream = None
        self.loop = None

    async def emit(self, record: logging.LogRecord) -> Awaitable[None]:
        """Emit a log record.

        Args:
            record: Log record to emit.
        """
        print(f'calling emit {record.msg}')
        try:
            msg = self.format(record)
            print('formatted')
            if not self.stream:
                print('attempting connection')
                self.stream = await self.client.connect(self.host, self.port)
                print('connection made')

            if self.stream:
                await self.stream.write(
                    self.serialize(msg)
                )
        except Exception as e:
            print(e)

    def serialize(self, log: logging.LogRecord) -> Awaitable[bytes]:
        """Serialize a log record in a separate thread, and await the result.

        Args:
            log: Log record to be serialized

        Returns:
            The serialized log record wrapped in an awaitable
        """
        print('calling serialize')
        return pickle.dumps(log)

    def handle(self, record: logging.LogRecord) -> bool:
        if record.name == '__main__':
            if self.loop is None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                # print('running forever')
                # loop.run_forever()

            loop.create_task(self._handle(record))
            loop.run_until_complete(loop.shutdown_asyncgens())

    async def _handle(self, record: logging.LogRecord) -> bool:
        print('calling _handle')
        rv = self.filter(record)
        if rv:
            self.acquire()
            try:
                await self.emit(record)
            finally:
                self.release()
        return rv


handler = TCPClientHandler()
logging.basicConfig(
    level="DEBUG",
    format="%(asctime)s %(levelname)s %(name)s::%(message)s",
    datefmt="[%X]",
    handlers=[
        handler,
        logging.StreamHandler(),
    ],
)
logging.getLogger('asyncio').setLevel("DEBUG")

logger = logging.getLogger(__name__)
logger.info("Jackdaws love my big sphinx of quartz.")
# while True:
#     logger.info("Jackdaws love my big sphinx of quartz.")
#     time.sleep(5)

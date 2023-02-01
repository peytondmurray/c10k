import logging
import logging.handlers
import os
import time

socket_handler = logging.handlers.SocketHandler(
    "localhost", logging.handlers.DEFAULT_TCP_LOGGING_PORT
)
stream_handler = logging.StreamHandler()

# don't bother with a formatter for socket_handler, since a socket handler
# sends the event as an unformatted pickle
logging.basicConfig(
    level="DEBUG",
    format="%(asctime)s %(levelname)s %(name)s::%(message)s",
    datefmt="[%X]",
    handlers=[
        socket_handler,
        stream_handler,
    ],
)


while True:
    logging.info("Jackdaws love my big sphinx of quartz.", extra={"pid": os.getpid()})
    time.sleep(1)

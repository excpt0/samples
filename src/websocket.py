import asyncio
import time

from sanic.exceptions import InvalidUsage
from sanic.log import log
from sanic.websocket import WebSocketProtocol as SanicWSProtocol
from websockets import handshake
from websockets.exceptions import ConnectionClosed, InvalidHandshake
from websockets.protocol import WebSocketCommonProtocol
from websockets.framing import OP_CLOSE, OP_PING, OP_PONG, parse_close
from websockets.protocol import OPEN as WS_OPEN

from settings import settings
from .async import PeriodicCallback


class PingPongProtocol(WebSocketCommonProtocol):
    last_ping = 0
    last_pong = 0

    def on_pong(self, frame_data):
        # Do not acknowledge pings on unsolicited pongs.
        if frame_data in self.pings:
            # Acknowledge all pings up to the one matching this pong.
            ping_id = None
            while ping_id != frame_data:
                ping_id, waiter = self.pings.popitem(0)
                if not waiter.cancelled():
                    waiter.set_result(None)
                self.last_pong = time.time()

    def ping(self, *args, **kwargs):
        self.last_ping = time.time()
        return super(PingPongProtocol, self).ping(*args, **kwargs)

    @asyncio.coroutine
    def read_data_frame(self, max_size):
        # Deal with control frames automatically and return next data frame.
        # 6.2. Receiving Data
        while True:
            frame = yield from self.read_frame(max_size)

            # 5.5. Control Frames
            if frame.opcode == OP_CLOSE:
                # Make sure the close frame is valid before echoing it.
                code, reason = parse_close(frame.data)
                if self.state == WS_OPEN:
                    # 7.1.3. The WebSocket Closing Handshake is Started
                    yield from self.write_frame(OP_CLOSE, frame.data)
                self.close_code, self.close_reason = code, reason
                self.closing_handshake.set_result(True)
                return

            elif frame.opcode == OP_PING:
                # Answer pings.
                yield from self.pong(frame.data)

            elif frame.opcode == OP_PONG:
                self.on_pong(frame.data)

            # 5.6. Data Frames
            else:
                return frame


class WebSocketProtocol(SanicWSProtocol):
    def __init__(self, *args, **kwargs):
        self.ping_interval = kwargs.pop('ping_interval', settings.PING_INTERVAL)
        self.ping_timeout = kwargs.pop('ping_timeout', settings.PING_INTERVAL)
        super().__init__(*args, **kwargs)
        self.websocket = None

    async def websocket_handshake(self, request):
        # let the websockets package do the handshake with the client
        headers = []

        def get_header(k):
            return request.headers.get(k, '')

        def set_header(k, v):
            headers.append((k, v))

        try:
            key = handshake.check_request(get_header)
            handshake.build_response(set_header, key)
        except InvalidHandshake:
            raise InvalidUsage('Invalid websocket request')

        # write the 101 response back to the client
        rv = b'HTTP/1.1 101 Switching Protocols\r\n'
        for k, v in headers:
            rv += k.encode('utf-8') + b': ' + v.encode('utf-8') + b'\r\n'
        rv += b'\r\n'
        request.transport.write(rv)

        # hook up the websocket protocol
        self.websocket = PingPongProtocol()
        self.websocket.connection_made(request.transport)
        log.debug('Websocket handshake done.')
        # self.start_pinging()
        return self.websocket

    def start_pinging(self):
        """Start sending periodic pings to keep the connection alive"""
        if self.ping_interval > 0:
            log.debug('Start pinging with interval %d' % self.ping_interval)
            self.websocket.last_ping = self.websocket.last_pong = time.time()
            self.ping_callback = PeriodicCallback(
                self.periodic_ping, self.ping_interval)
            self.ping_callback.start()

    @asyncio.coroutine
    def periodic_ping(self):
        """Send a ping to keep the websocket alive

        Called periodically if the websocket_ping_interval is set and non-zero.
        """

        if self.websocket and not self.websocket.open and self.ping_callback is not None:
            log.debug('Stop pinging. Websocket is closed.')
            self.ping_callback.stop()
            return

        # Check for timeout on pong. Make sure that we really have
        # sent a recent ping in case the machine with both server and
        # client has been suspended since the last ping.
        now = time.time()
        since_last_pong = now - self.websocket.last_pong
        since_last_ping = now - self.websocket.last_ping
        if (since_last_ping < 2*self.ping_interval and
                since_last_pong > self.ping_timeout):
            self.ping_callback.stop()
            self.connection_lost(ConnectionClosed(code=1000, reason='ping_timeout'))
            return

        yield from self.websocket.ping()

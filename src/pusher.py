import asyncio
from random import randint
from ujson import loads

from sanic.exceptions import InvalidUsage
from websockets import ConnectionClosed, InvalidState

from common.prefixes import WS_CHANNEL
from common.rdb import get_rdb_subscription
from core.exceptions import ValidationError, MsgTransportError
from core.protocol import APIRequest
from models.notification import RedisNotification
from services.ws import ConnectionsStorage
from settings import settings, constants
from utils.dates import utctimets
from .log import log


class SanicWSPusher(object):
    def __init__(self):
        self.is_running = False
        self.storage = ConnectionsStorage()
        self._listen_task = None

    def run(self, loop=None):
        """Run Websocket Pusher listening."""
        if not loop:
            loop = asyncio.get_event_loop()
        log.info('Pusher listening run')
        self._listen_task = loop.create_task(self.listen())
        self.is_running = True

    def stop(self):
        log.info('Pusher listening stop')
        self._listen_task.cancel()
        self.is_running = False

    @asyncio.coroutine
    def listen(self):
        """Start listening loop. Read and process the messages from Redis channel."""
        self.subscription = yield from get_rdb_subscription([WS_CHANNEL % settings.SENDER_ID])
        while True:
            message = yield from self.subscription.next_published()
            yield from self.process_message(message.value)
            yield from asyncio.sleep(0.001)

    @asyncio.coroutine
    def process_message(self, msg):
        """Callback for incoming messages

        :param msg: JSON string
        """
        try:
            n = RedisNotification.from_dict(loads(msg))
        except (TypeError, ValidationError):
            log.error('Cant parse notification.', exc_info=True)
        else:
            try:
                yield from self.do_send(n)
            except MsgTransportError as e:
                log.error(e, exc_info=True)

    @asyncio.coroutine
    def do_send(self, notification):
        """Send notification to connections assigned to the user.

        :param: RedisNotification object
        """
        connections = self.storage.get_user_conns(notification.user_id)
        if not connections:
            raise MsgTransportError('No connections for user "%d" ' % notification.user_id)
        for conn_id, c in connections.items():
            if (notification.connection_id and conn_id != notification.connection_id) \
                    or c.session_id in notification.exclude_session \
                    or conn_id in notification.exclude_connection:
                log.info('Skip sending to user "%s" %s'
                          % (str(c.request_sender), notification.to_dict()))
                continue
            log.info(
                'Sending data to client "%s" connection uuid "%s" %s'%
                (str(c.request_sender), str(c.conn_id),
                 notification.to_dict())
            )
            try:
                yield from c.send(
                    APIRequest(
                        method=notification.method,
                        payload=notification.payload,
                        token=None,
                        api_ver=constants.API_VERSION,
                        ts=utctimets(),
                        request_id=randint(1, 999999),

                    ).to_dict()
                )
            except (InvalidUsage, asyncio.CancelledError, ConnectionClosed, InvalidState) as e:
                log.error(e, exc_info=True)
                raise MsgTransportError('Failed ws sending to user "%d". Reason: %s' % (notification.user_id, str(e)))

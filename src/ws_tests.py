import asyncio
import logging
import traceback
import unittest
from functools import partial
from ujson import dumps, loads

from sanic import Sanic
from sanic.response import json
from websockets import connect

from common.auth import JWTData
from common.prefixes import WS_CHANNEL
from common.websocket import WebSocketProtocol
from core.rpc import SanicRPCErrorHandler
from models import dbmodels
from services.sender.handlers import feed
from services.sender.pusher import SanicWSPusher
from services.sender.storage import ConnectionsStorage
from settings import constants, errmsg, settings
from tests.base import SetupTestMixin, create_user, testlog, AsyncioTestCase, Stub, async_test
from utils.dates import utctimets

logging.getLogger('sanic').setLevel(logging.CRITICAL)
logging.getLogger('raven.base.Client').setLevel(logging.CRITICAL)


async def before_start(app, loop):
    await settings.init_redis_connections_35()
    await settings.init_pg_connection_35()
    ConnectionsStorage().clear()


def fake_handler(r):
    return json({'code': 200, 'payload': {'result': 'fake'}, })


class WSApiTestCase(SetupTestMixin, unittest.TestCase):
    server_kwargs = dict(
        protocol=WebSocketProtocol,
        before_start=before_start,
    )
    def tearDown(self):
        self.loop.close()
        settings.pg_db.close()

    async def start_client(self, path='', **kwds):
        self.client = await connect('ws://localhost:%d/%s' % (42101, path), **kwds)

    def ws_request(self, data):
        result = [None, None]
        exceptions = []

        @self.app.listener('after_server_start')
        async def _collect_response(sanic, loop):
            try:
                await self.start_client()
                await self.client.send(data)
                result[-1] = await self.client.recv()
            except Exception as e:
                testlog.error(
                    'Exception:\n{}'.format(traceback.format_exc()))
                exceptions.append(e)
            await self.client.close()
            self.app.stop()

        self.app.run(host='127.0.0.1', debug=False, port=42101, **self.server_kwargs)
        self.app.listeners['after_server_start'].pop()
        return loads(result[-1])

    def setUp(self):
        super(WSApiTestCase, self).setUp()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.app = Sanic(error_handler=SanicRPCErrorHandler())
        self.app.add_websocket_route(partial(feed, handler_args=dict(allow_no_init=True)), '/')
        self.app.add_route(fake_handler, '/public', methods=frozenset({'POST'}))
        self.user = create_user('vasya', 79819002020)
        self.session = dbmodels.Session.create(user=self.user)
        self.access_token = JWTData(self.session.id, self.user.id, utctimets(minutes=30)).encode()

    def test_success_request(self):
        api_req = dict(
            token=self.access_token,
            api_ver=1,
            request_id=123,
            method='auth.test',
            ts=utctimets(),
            payload={},
        )
        resp = self.ws_request(dumps(api_req))
        self.assertEqual(resp['code'], 200)
        self.assertEqual(resp['payload']['result'], 'fake')

    def test_init_success(self):
        api_req = dict(
            token=self.access_token,
            api_ver=1,
            request_id=123,
            method='initConnection',
            ts=utctimets(),
            payload={},
        )
        resp = self.ws_request(dumps(api_req))
        self.assertEqual(resp['code'], 200)
        self.assertEqual(resp['payload']['result'], True)

    def test_init_auth_fail(self):
        api_req = dict(
            token='somewrongtoken',
            api_ver=1,
            request_id=123,
            method='initConnection',
            ts=utctimets(),
            payload={},
        )
        resp = self.ws_request(dumps(api_req))
        self.assertEqual(resp['code'], 401)
        self.assertEqual(resp['payload']['result'], errmsg.ACCESS_TOKEN_UNREGISTERED)


class WSLimitsApiTestCase(SetupTestMixin, unittest.TestCase):
    server_kwargs = dict(
        protocol=WebSocketProtocol,
        before_start=before_start,
    )

    def setUp(self):
        super(WSLimitsApiTestCase, self).setUp()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.app = Sanic(error_handler=SanicRPCErrorHandler())
        self.app.add_websocket_route(partial(feed, handler_args=dict(allow_no_init=True)), '/')
        self.app.add_route(fake_handler, '/public', methods=frozenset({'POST'}))
        self.user = create_user('vasya', 79819002020)
        self.session = dbmodels.Session.create(user=self.user)
        self.access_token = JWTData(self.session.id, self.user.id, utctimets(minutes=30)).encode()

    def tearDown(self):
        self.loop.close()
        settings.pg_db.close()

    async def start_client(self, path='', **kwds):
        self.client = await connect('ws://localhost:%d/%s' % (42101, path), **kwds)

    def ws_twice_request(self, data):
        result = [None, None]
        exceptions = []

        @self.app.listener('after_server_start')
        async def _collect_response(sanic, loop):
            try:
                await self.start_client()
                await self.client.send(data)
                result[0] = await self.client.recv()
                await self.client.send(data)
                result[-1] = await self.client.recv()
            except Exception as e:
                testlog.error(
                    'Exception:\n{}'.format(traceback.format_exc()))
                exceptions.append(e)
            await self.client.close()
            self.app.stop()

        self.app.run(host='127.0.0.1', debug=False, port=42101, **self.server_kwargs)
        self.app.listeners['after_server_start'].pop()
        return loads(result[0]), loads(result[-1]),

    def test_limits(self):
        api_req = dict(
            token=self.access_token,
            api_ver=1,
            request_id=123,
            method='messages.sendMessage',
            ts=utctimets(),
            payload={},
        )
        resp, resp2 = self.ws_twice_request(dumps(api_req))
        self.assertEqual(resp['code'], 200)
        self.assertEqual(resp['payload']['result'], 'fake')

        self.assertEqual(resp2['code'], 429)
        self.assertEqual(resp2['payload'], {'result': 'TOO_MANY_REQUESTS'})


class PusherTestCase(SetupTestMixin, AsyncioTestCase):
    class __ConnStub():
        def __init__(self):
            self.stub_obj = Stub()
            self.send = asyncio.coroutine(self.stub_obj)
            self.session_id = 2
            self.request_sender = 1
            self.conn_id = 'stub_id'

    @async_test
    async def test_pusher(self):
        pusher = SanicWSPusher()
        pusher.run(self.loop)
        await asyncio.sleep(0.001)
        conn = self.__ConnStub()
        storage = ConnectionsStorage()
        storage.append(conn, 'foo')
        storage.set_authorized('foo', 1)
        notification = {
            'type': 'rpc_request',
            'user_id': 1,
            'method': 'updates.test',
            'payload': {'foobar': 'baz'}
        }
        await settings.redis.publish(WS_CHANNEL % settings.SENDER_ID, dumps(notification), )
        await asyncio.sleep(0.001)
        self.assertEqual(conn.stub_obj.exec_count, 1)
        notif = conn.stub_obj.last_call_args[0]
        notif.pop('ts')
        notif.pop('request_id')
        self.assertDictEqual(
            notif,
            dict(
                method=notification['method'],
                payload=notification['payload'],
                token=None,
                api_ver=constants.API_VERSION,
            )
        )

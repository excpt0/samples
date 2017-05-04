import aiohttp
from sanic import Sanic
from sanic.response import HTTPResponse

from core.rpc import SanicRPCErrorHandler, private_rpc_request
from services.sender.pusher import SanicWSPusher
from services.sender.websocket import WebSocketProtocol
from services.ws import feed
from settings import settings
from .async import PeriodicCallback


async def before_start(app, loop):
    await settings.init_redis_connections_35()
    await settings.init_pg_connection_35()
    ws_checker_ping()
    SanicWSPusher().run(loop)

def ws_checker_ping():
    session = aiohttp.ClientSession()
    _task = lambda: private_rpc_request(
        settings.ws_checker_url,
        'set_alive',
        {'sender_id': settings.SENDER_ID},
        session=session,
    )
    PeriodicCallback(_task, 1).start()

def health_handler(r):
    return HTTPResponse('ok')

if __name__ == "__main__":
    app = Sanic(error_handler=SanicRPCErrorHandler())
    app.add_websocket_route(feed, '/',)
    app.add_route(health_handler, '/health', methods=frozenset({'POST', 'GET'}))
    app.run(host="0.0.0.0", port=settings.WS_APP_PORT, protocol=WebSocketProtocol, before_start=before_start)

from datetime import datetime
from ujson import loads, dumps
from uuid import uuid4

import peewee_async

from common.auth import authenticate
from common.rdb import add_user_sender, set_session_status, remove_user_sender
from core.exceptions import ValidationError, APIError, SessionExpiredError, SessionInactiveError, \
    SessionUserBanned, SessionUserRequireActivation, AuthError, T4Exception
from core.protocol import APIRequest, ACK, APIResponse
from core.rpc import handle_t4_exception
from models.dbmodels import Connection, User, Session
from services.apihttp.handlers import rpc_exec
from services.ws import ConnectionsStorage
from settings import settings, constants, errmsg
from utils.dates import utctimets, utctime, td_seconds
from .log import log


WS_OPEN = 1
WS_AUTHORIZED = 2
WS_CLOSED = 3


class WSAPIHandler(object):
    """
    Main client-side requests handler. Validate incoming requests and proxify to inner services.
    """
    __slots__ = ['ws', 'conn_id', 'request_sender', 'exec_limits',
                 '_method_limits', 'session_id', 'request_id', 'status',
                 'allow_no_init', ]
    def __init__(self, ws, allow_no_init=False):
        self.ws = ws
        self.request_id = None
        self.session_id = None
        self.status = WS_OPEN
        self.allow_no_init = allow_no_init
        self.exec_limits = {'messages.sendMessage': 1}
        self._method_limits = {k: {'delta': 1 / v, 'last': None, 'count': 0} for k, v in self.exec_limits.items()}

    def on_open(self):
        self.conn_id = self.ws.conn_id = str(uuid4())
        self.request_sender = self.ws.request_sender = None
        self.ws.session_id = None
        ConnectionsStorage().append(self.ws, self.ws.conn_id)
        log.info('Connected client, connection_id "%s"' % self.ws.conn_id)

    async def authenticate(self, token):
        try:
            self.request_sender, self.session_id = await authenticate(token)
            self.status = WS_AUTHORIZED
            self.ws.request_sender = self.request_sender
            self.ws.session_id = self.session_id
        except SessionExpiredError:
            raise APIError(errmsg.SESSION_EXPIRED, 401)
        except SessionInactiveError:
            raise APIError(errmsg.SESSION_INACTIVE, 401)
        except SessionUserBanned:
            raise APIError(errmsg.USER_BANNED, 401)
        except SessionUserRequireActivation as e:
            raise APIError(errmsg.USER_REQUIRE_ACTIVATION, 403)
        except AuthError:
            raise APIError(errmsg.ACCESS_TOKEN_UNREGISTERED, 401)

    async def listen(self):
        while True:
            data = await self.ws.recv()
            try:
                response = await self.on_message(data)
            except T4Exception as e:
                response = handle_t4_exception(e, self.request_id)
            except Exception as e:
                log.error(e, exc_info=True)
                response = dict(
                    payload={'result': errmsg.SERVER_ERROR},
                    code=500,
                    request_id=self.request_id,
                    ts=utctimets(),
                )
            if response:
                await self.ws.send(dumps(response, ensure_ascii=False))

    async def send_frontend_request(self, api_request):
        if api_request.method == 'initConnection':
            if self.status == WS_AUTHORIZED:
                raise APIError(errmsg.CONNECTION_ALREADY_INITIALIZED, 400)
            else:
                await self.authenticate(api_request.token)
                ConnectionsStorage().set_authorized(self.ws.conn_id, self.request_sender)
                await add_user_sender(self.request_sender, settings.SENDER_ID)
                await self.create_connection_record(api_request)
                return APIResponse(
                    request_id=api_request.request_id,
                    code=200,
                    payload={'result': True},
                )
        elif self.allow_no_init or self.status == WS_AUTHORIZED:
            return (await rpc_exec(api_request))
        else:
            raise APIError(errmsg.FORBIDDEN, 403)

    async def on_close(self):
        self.status = WS_CLOSED
        if self.request_sender:
            await remove_user_sender(self.request_sender, settings.SENDER_ID)
            ConnectionsStorage().remove_authorized(self.conn_id, self.request_sender)
        else:
            ConnectionsStorage().remove_no_auth(self.conn_id)
        log.info('Disconnected user "%s" connection_id "%s", ' %
                     (str(self.request_sender or '-'), self.conn_id))

    async def create_connection_record(self, api_request):
        await peewee_async.create_object(
            Connection,
            user=User(id=self.request_sender),
            session=Session(id=self.session_id),
            app_version=api_request.payload.get('app_version', None),
            os=api_request.payload.get('os', None),
            device=api_request.payload.get('device', None),
        )
        inactive_sessions = await peewee_async.execute(
            Session.filter(
                user=User(id=self.request_sender),
                id__ne=self.session_id,
                expire__gte=utctime(),
            )
        )
        for s in inactive_sessions:
            await set_session_status(
                s.id,
                constants.ACCESS_TOKEN_INACTIVE,
            )

    async def on_message(self, message):
        self.request_id = None
        try:
            data = loads(message)
        except (ValueError, TypeError) as e:
            raise APIError(errmsg.CONTAINER_ERROR, 400)
        try:
            request = APIRequest.from_dict(data)
            self.request_id = request.request_id
            if request.api_ver and request.api_ver != constants.API_VERSION:
                raise APIError(errmsg.UNSUPPORTED_API_VERSION, )
        except ValidationError as e:
            pass
        else:
            if request.method in self.exec_limits:
                limit = self._method_limits[request.method]
                ct = datetime.utcnow()
                if limit['last'] \
                        and td_seconds(ct - limit['last']) <= limit['delta']:
                    limit['count'] += 1
                    limit['last'] = ct
                    if limit['count'] > self.exec_limits[request.method]:
                        raise APIError(errmsg.TOO_MANY_REQUESTS, 429)
                else:
                    limit['count'] = 1
                    limit['last'] = ct
            response = await self.send_frontend_request(request)
            log.info({
                'request_sender': str(self.request_sender),
                'incoming_data': data,
                'response': dumps(response.to_dict()),
                'connection_id': self.conn_id,
            })
            return response
        try:
            ack = ACK.from_dict(data)
            return None
        except ValidationError:
            pass
        try:
            resp = APIResponse.from_dict(data)
            return ACK(ack_id=resp.request_id)
        except ValidationError:
            pass
        raise APIError(errmsg.CONTAINER_ERROR, 400)


async def feed(request, ws, handler_args=None):
    if not handler_args:
        handler_args = {}
    handler = WSAPIHandler(ws, **handler_args)
    handler.on_open()
    await handler.listen()
    await handler.on_close()

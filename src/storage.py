class ConnectionsStorage():
    instance = None

    class __Storage:
        __slots__ = ['no_auth', 'authorized', ]

        def __init__(self):
            self.no_auth = {}
            self.authorized = {}

        def append(self, c, cid):
            '''Append ws connection to not authorized connection list.

            :param c: ws connection object
            :param cid: ws connection id
            '''
            self.no_auth[cid] = c

        def set_authorized(self, cid, uid):
            '''Set connection with passed cid authorized by user with uid.

            :param cid: ws connection id
            :param cid: user id
            '''
            if uid not in self.authorized:
                self.authorized[uid] = {}
            self.authorized[uid][cid] = self.no_auth.pop(cid)

        def remove_no_auth(self, cid):
            '''Remove not authorized connection with passed cid from storage.

            :param cid: ws connection id
            '''
            del self.no_auth[cid]

        def remove_authorized(self, cid, uid):
            '''Remove not authorized connection with passed cid from storage.

            :param cid: ws connection id
            :param cid: user id
            '''
            del self.authorized[uid][cid]

        def get_user_conns(self, uid):
            '''Return all user with passed uid connections.

            :param uid: user id
            :return: dict with connections {cid: connection_obj}
            '''
            return self.authorized[uid] if self.authorized[uid] else {}

        def clear(self):
            '''Clear the storage.'''
            self.no_auth = {}
            self.authorized = {}

    def __init__(self):
        if not self.instance:
            ConnectionsStorage.instance = ConnectionsStorage.__Storage()

    def __getattr__(self, item):
        return getattr(self.instance, item)
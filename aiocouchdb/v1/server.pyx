# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
from typing import Union

from aiocouchdb.authn import AuthProvider

from cpython cimport bool

from aiocouchdb.client.resource cimport Resource
from aiocouchdb.feeds cimport EventSourceFeed, JsonFeed
from .authdb cimport AuthDatabase
from .config cimport ServerConfig
from .database cimport Database
from .session cimport Session
from .utils cimport params_from_locals


__all__ = ('Server',)


cdef class Server(object):
    """Implementation of :ref:`CouchDB Server API <api/server>`."""

    __module__ = "aiocouchdb.v1.server"

    def __init__(
            self,
            url_or_resource='http://localhost:5984', *,
            authdb_class=None,
            authdb_name=None,
            config_class=None,
            database_class=None,
            loop=None,
            session_class=None):
        self.database_class = Database
        self.authdb_name = '_users'
        self.authdb_class = AuthDatabase
        self.config_class = ServerConfig
        self.session_class = Session

        if authdb_class is not None:
            self.authdb_class = authdb_class
        if authdb_name is not None:
            self.authdb_name = authdb_name
        if config_class is not None:
            self.config_class = config_class
        if database_class is not None:
            self.database_class = database_class
        if session_class is not None:
            self.session_class = session_class
        self.resource = (
            Resource(url_or_resource, loop=loop)
            if isinstance(url_or_resource, str)
            else url_or_resource)
        self._authdb = self.authdb_class(
            self.resource(self.authdb_name),
            dbname=self.authdb_name)
        self._config = self.config_class(self.resource)
        self._session = self.session_class(self.resource)

    def __getitem__(self, str dbname) -> Database:
        return self.database_class(
            self.resource(dbname), dbname=dbname)

    def __repr__(self) -> str:
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.resource.url,
            hex(id(self)))

    @property
    def authdb(self) -> AuthDatabase:
        """Proxy to the :class:`authentication database
        <aiocouchdb.v1.database.AuthDatabase>` instance.
        """
        return self._authdb

    @property
    def config(self):
        """Proxy to the related :class:`~aiocouchdb.v1.server.config_class`
        instance."""
        return self._config

    @property
    def session(self):
        """Proxy to the related
        :class:`~aiocouchdb.v1.server.Server.session_class` instance."""
        return self._session

    async def db(self, str dbname, *, auth=None) -> Database:
        db = self[dbname]
        resp = await db.resource.head(dict(auth=auth, maybe_raise=False))
        if resp.status != 404:
            await resp.maybe_raise_error()
        await resp.release()
        return db

    async def info(self, *, auth: AuthProvider = None) -> dict:
        """Returns server :ref:`meta information and welcome message
        <api/server/root>`.
        """
        resp = await self.resource.get(dict(auth=auth))
        return await resp.json()

    async def active_tasks(self, *, auth: AuthProvider = None) -> list:
        """Returns list of :ref:`active tasks <api/server/active_tasks>`
        which runs on server.
        """
        resp = await self.resource.get(dict(path='_active_tasks', auth=auth))
        return await resp.json()

    async def all_dbs(self, *, auth: AuthProvider = None) -> list:
        """Returns list of available :ref:`databases <api/server/all_dbs>`
        on server.
        """
        resp = await self.resource.get(dict(path='_all_dbs', auth=auth))
        return await resp.json()

    async def db_updates(
            self, *,
            auth: AuthProvider = None,
            feed_buffer_size: int = None,
            feed: str = None,
            timeout: int = None,
            heartbeat: bool = None) -> Union[dict, JsonFeed, EventSourceFeed]:
        """Emits :ref:`databases events <api/server/db_updates>` for
        the related server instance.
        """
        params = {}
        if feed is not None:
            params['feed'] = feed
        if timeout is not None:
            params['timeout'] = timeout
        if heartbeat is not None:
            params['heartbeat'] = heartbeat
        resp = await self.resource.get(
            dict(path='_db_updates',
                 auth=auth,
                 params=params))
        if feed == 'continuous':
            return JsonFeed(resp, buffer_size=feed_buffer_size)
        elif feed == 'eventsource':
            return EventSourceFeed(resp, buffer_size=feed_buffer_size)
        else:
            return await resp.json()

    async def log(
            self, *,
            bytes: int = None,
            offset: int = None,
            auth: AuthProvider = None) -> str:
        """Returns a chunk of data from the tail of :ref:`CouchDB's log
        <api/server/log>` file.
        """
        params = {}
        if bytes is not None:
            params['bytes'] = bytes
        if offset is not None:
            params['offset'] = offset
        resp = await self.resource.get(dict(path='_log', auth=auth, params=params))
        return (await resp.read()).decode('utf-8')

    async def replicate(
            self,
            source: str,
            target: str,
            *,
            auth: AuthProvider = None,
            cancel: bool = None,
            continuous: bool = None,
            create_target: bool = None,
            doc_ids: list = None,
            filter: str = None,
            headers=None,
            proxy: str = None,
            query_params: dict = None,
            since_seq: int = None,
            checkpoint_interval: int = None,
            connection_timeout: int = None,
            http_connections: int = None,
            retries_per_request: int = None,
            socket_options: str = None,
            use_checkpoints: bool = None,
            worker_batch_size: int = None,
            worker_processes: int = None) -> dict:
        """:ref:`Runs a replication <api/server/replicate>` from ``source``
        to ``target``.
        .. _checkpoint_interval: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/checkpoint_interval
        .. _connection_timeout: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/connection_timeout
        .. _http_connections: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/http_connections
        .. _retries_per_request: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/retries_per_request
        .. _socket_options: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/socket_options
        .. _use_checkpoints: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/use_checkpoints
        .. _worker_batch_size: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/worker_batch_size
        .. _worker_processes: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/worker_processes

        """
        cdef dict doc = {'source': source, 'target': target}
        doc.update(
            params_from_locals(
                locals(),
                ('self', 'doc', 'source', 'target', 'auth', 'params')))
        resp = await self.resource.post(dict(path='_replicate', auth=auth, data=doc))
        return await resp.json()

    async def restart(self, *, auth: AuthProvider = None) -> dict:
        """:ref:`Restarts <api/server/restart>` server instance.
        """
        resp = await self.resource.post(dict(path='_restart', auth=auth))
        return await resp.json()

    async def stats(
            self,
            metric: str = None, *,
            auth: AuthProvider = None,
            flush: bool = None,
            range: int = None) -> dict:
        """Returns :ref:`server statistics <api/server/stats>`.
        .. _Sampling range: http://docs.couchdb.org/en/latest/config/misc.html#stats/samples
        """
        path = ['_stats']
        params = {}
        if metric is not None:
            if '/' in metric:
                path.extend(metric.split('/', 1))
            else:
                raise ValueError('invalid metric name. try "httpd/requests"')
        if flush is not None:
            params['flush'] = flush
        if range is not None:
            params['range'] = range
        resp = await self.resource(*path).get(dict(auth=auth, params=params))
        return await resp.json()

    async def uuids(
            self, *,
            auth: AuthProvider = None,
            count: int = None) -> list:
        """Returns :ref:`UUIDs <api/server/uuids>` generated on server.
        """
        params = {}
        if count is not None:
            params['count'] = count
        resp = await self.resource.get(dict(path='_uuids', auth=auth, params=params))
        return (await resp.json())['uuids']

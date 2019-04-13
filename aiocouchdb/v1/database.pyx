# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2016 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import uuid
from typing import Iterable, Union

from aiocouchdb.authn import AuthProvider
from aiocouchdb.feeds import (
    ContinuousChangesFeed, EventSourceChangesFeed)

from .utils import chunkify

from aiocouchdb.client.resource cimport Resource
from aiocouchdb.feeds cimport (
    ChangesFeed, LongPollChangesFeed)
from aiocouchdb.views cimport View
from .designdoc cimport DesignDocument
from .document cimport Document
from .security cimport DatabaseSecurity
from .utils cimport params_from_locals


__all__ = ('Database', )


cdef class Database(object):
    """Implementation of :ref:`CouchDB Database API <api/db>`."""

    __module__ = "aiocouchdb.v1.database"

    def __init__(
            self, url_or_resource, *,
            dbname: str = None,
            document_class: type(Document) = None,
            design_document_class: type(DesignDocument) = None,
            loop: asyncio.AbstractEventLoop = None,
            security_class: type(DatabaseSecurity) = None,
            view_class: type(View) = None):
        self.document_class = document_class or Document
        self.design_document_class = design_document_class or DesignDocument
        self.view_class = view_class or View
        self.security_class = security_class or DatabaseSecurity
        self.resource = (
            Resource(url_or_resource, loop=loop)
            if isinstance(url_or_resource, str)
            else url_or_resource)
        self._security = self.security_class(self.resource)
        self._dbname = dbname

    def __getitem__(self, str docid) -> Union[Document, DesignDocument]:
        return (
            self.design_document_class(
                self.resource(*docid.split('/', 1)),
                docid=docid)
        if docid.startswith('_design/')
        else self.document_class(
                self.resource(docid),
                docid=docid))

    def __repr__(self) -> str:
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,
            self.resource.url,
            hex(id(self)))

    @property
    def name(self) -> str:
        """Returns a database name specified in class constructor."""
        return self._dbname

    @property
    def security(self) -> DatabaseSecurity:
        """Proxy to the related
        :class:`~aiocouchdb.v1.database.Database.security_class` instance."""
        return self._security

    async def all_docs(
            self, *keys,
            auth: AuthProvider = None,
            feed_buffer_size: int = None,
            att_encoding_info: bool = None,
            attachments: bool = None,
            conflicts: bool = None,
            descending: bool = None,
            endkey: Union[str, type(Ellipsis)] = ...,
            endkey_docid: str = None,
            include_docs=None,
            inclusive_end: bool = None,
            limit: int = None,
            skip: int = None,
            stale: str = None,
            startkey: Union[str, type(Ellipsis)] = ...,
            startkey_docid: str = None,
            update_seq: bool = None) -> ViewFeed:
        """Iterates over :ref:`all documents view <api/db/all_docs>`.

        :param str keys: List of document ids to fetch. This method is smart
                         enough to use `GET` or `POST` request depending on
                         amount of ``keys``
        :param str include_docs: Include document body for each row
        """
        params = locals()
        for key in ('self', 'auth', 'feed_buffer_size'):
            params.pop(key)
        return (
            await self.view_class(
                self.resource('_all_docs')).request(
                    auth=auth,
                    feed_buffer_size=feed_buffer_size,
                    params=params))

    async def bulk_docs(
            self,
            docs: Iterable,
            *,
            auth: AuthProvider = None,
            all_or_nothing: bool = None,
            new_edits: bool = None) -> list:
        """:ref:`Updates multiple documents <api/db/bulk_docs>` using a single
        request.
        :ref:`all-or-nothing <api/db/bulk_docs/semantics>` semantics
        """
        resp = await self.resource.post(
            dict(path='_bulk_docs',
                 auth=auth,
                 data=chunkify(
                     docs, all_or_nothing, new_edits)))
        return await resp.json()

    async def changes(
            self, *doc_ids,
            auth: AuthProvider = None,
            feed_buffer_size: int = None,
            att_encoding_info: bool = None,
            attachments: bool = None,
            conflicts: bool = None,
            descending: bool = None,
            feed: str = None,
            filter: str =  None,
            headers: dict = None,
            heartbeat: int = None,
            include_docs: bool = None,
            limit: int = None,
            params: dict = None,
            since=None,
            style: str = None,
            timeout: int = None,
            view: str = None) -> ChangesFeed:
        """Emits :ref:`database changes events<api/db/changes>`.

        :param str doc_ids: Document IDs to filter for. This method is smart
                            enough to use `GET` or `POST` request depending
                            if any ``doc_ids`` were provided or not and
                            automatically sets ``filter`` param to ``_doc_ids``
                            value.
        :param since: Starts listening changes feed since given
                      `update sequence` value
        """
        params = dict(params or {})
        params.update(
            params_from_locals(
                locals(),
                ('self', 'doc_ids', 'auth', 'headers', 'params')))
        data = None
        request = self.resource.get
        if doc_ids:
            data = {'doc_ids': doc_ids}
            params['filter'] = params.get('filter', '_doc_ids')
            assert params['filter'] == '_doc_ids'
            request = self.resource.post
        if 'view' in params:
            params['filter'] = params.get('filter', '_view')
            assert params['filter'] == '_view'
        resp = await request(
            dict(path='_changes',
                 auth=auth,
                 data=data,
                 headers=headers,
                 params=params))
        Feed = ChangesFeed
        if feed == 'continuous':
            Feed = ContinuousChangesFeed
        elif feed == 'eventsource':
            Feed = EventSourceChangesFeed
        elif feed == 'longpoll':
            Feed = LongPollChangesFeed
        return Feed(resp, buffer_size=feed_buffer_size)

    async def compact(
            self,
            ddoc_name: str = None,
            *,
            auth: AuthProvider = None) -> dict:
        """Initiates :ref:`database <api/db/compact>`
        or :ref:`view index <api/db/compact/ddoc>` compaction.
        """
        path = ['_compact']
        if ddoc_name is not None:
            path.append(ddoc_name)
        resp = await self.resource(*path).post(dict(auth=auth))
        return await resp.json()

    async def create(self, *, auth: AuthProvider = None) -> dict:
        """`Creates a database`_.
        .. _Creates a database: http://docs.couchdb.org/en/latest/api/database/common.html#put--db
        """
        resp = await self.resource.put(dict(auth=auth))
        return await resp.json()

    async def ddoc(self, docid: str, *, auth: AuthProvider = None) -> DesignDocument:
        """Returns :class:`~aiocouchdb.v1.designdoc.DesignDocument` instance
        against specified document ID. This ID may startswith with ``_design/``
        prefix and if it's not prefix will be added automatically.

        If document isn't accessible for auth provided credentials, this method
        raises :exc:`aiocouchdb.errors.HttpErrorException` with the related
        response status code.
        """
        if not docid.startswith('_design/'):
            docid = '_design/' + docid
        resp = await self[docid].resource.head(dict(auth=auth, maybe_raise=False))
        if resp.status != 404:
            await resp.maybe_raise_error()
        # await resp.release()
        return self[docid]

    async def delete(self, *, auth: AuthProvider = None) -> dict:
        """`Deletes a database`_.
        .. _Deletes a database: http://docs.couchdb.org/en/latest/api/database/common.html#delete--db
        """
        resp = await self.resource.delete(dict(auth=auth))
        return await resp.json()

    async def doc(
            self,
            docid: str = None,
            *,
            auth: AuthProvider = None,
            idfun=uuid.uuid4) -> Document:
        """Returns :class:`~aiocouchdb.v1.document.Document` instance against
        specified document ID.

        If document ID wasn't specified, the ``idfun`` function will be used
        to generate it.

        If document isn't accessible for auth provided credentials, this method
        raises :exc:`aiocouchdb.errors.HttpErrorException` with the related
        response status code.

        :param idfun: Document ID generation function.
                      Should return ``str`` or other object which could be
                      translated into string
        """
        if docid is None:
            docid = str(idfun())
        resp = await self[docid].resource.head(dict(auth=auth, maybe_raise=False))
        if resp.status != 404:
            await resp.maybe_raise_error()
        # await resp.release()
        return self[docid]

    async def ensure_full_commit(self, *, auth: AuthProvider = None) -> dict:
        """Ensures that all bits are :ref:`committed on disk
        <api/db/ensure_full_commit>`.
        """
        resp = await self.resource.post(
            dict(path='_ensure_full_commit',
                 auth=auth))
        return await resp.json()

    async def exists(self, *, auth: AuthProvider = None) -> bool:
        """Checks if `database exists`_ on server. Assumes success on receiving
        response with `200 OK` status.
        .. _database exists: http://docs.couchdb.org/en/latest/api/database/common.html#head--db
        """
        resp = await self.resource.head(dict(auth=auth))
        # await resp.release()
        return resp.status == 200

    async def info(self, *, auth: AuthProvider = None) -> dict:
        """Returns `database information`_.
        .. _database information: http://docs.couchdb.org/en/latest/api/database/common.html#get--db
        """
        resp = await self.resource.get(dict(auth=auth))
        return await resp.json()

    async def missing_revs(self, id_revs: dict, *, auth: AuthProvider = None) -> dict:
        """Returns :ref:`document missed revisions <api/db/missing_revs>`
        in the database by given document-revisions mapping.
        """
        resp = await self.resource.post(
            dict(path='_missing_revs',
                 auth=auth,
                 data=id_revs))
        return await resp.json()

    async def purge(self, id_revs: dict, *, auth: AuthProvider = None) -> dict:
        """:ref:`Permanently removes specified document revisions
        <api/db/purge>` from the database.
        """
        resp = await self.resource.post(
            dict(path='_purge',
                 auth=auth,
                 data=id_revs))
        return await resp.json()

    async def revs_diff(
            self,
            id_revs: dict,
            *,
            auth: AuthProvider = None) -> dict:
        """Returns :ref:`document revisions difference <api/db/revs_diff>`
        in the database by given document-revisions mapping.
        """
        resp = await self.resource.post(
            dict(path='_revs_diff',
                 auth=auth,
                 data=id_revs))
        return await resp.json()

    async def revs_limit(
            self,
            count: int = None,
            *,
            auth: AuthProvider = None) -> Union[int, dict]:
        """Returns the :ref:`limit of database revisions <api/db/revs_limit>`
        to store or updates it if ``count`` parameter was specified.
        """
        params = dict(path='_revs_limit', auth=auth)
        request = self.resource.get
        if count is not None:
            params['data'] = count
            request = self.resource.put
        resp = await request(params)
        return await resp.json()

    async def temp_view(
            self,
            str map_fun,
            red_fun: str = None,
            language: str = None,
            *,
            auth: AuthProvider = None,
            feed_buffer_size: int = None,
            att_encoding_info: bool = None,
            attachments: bool = None,
            conflicts: bool = None,
            descending: bool = None,
            endkey: Union[str, type(Ellipsis)] = ...,
            endkey_docid: str = None,
            group: bool = None,
            group_level: int = None,
            include_docs=None,
            inclusive_end: int = None,
            keys: Union[list, type(Ellipsis)] = ...,
            limit: int = None,
            reduce: bool = None,
            skip: int = None,
            stale: str = None,
            startkey: Union[str, type(Ellipsis)] = ...,
            startkey_docid: str = None,
            update_seq: bool = None) -> ViewFeed:
        """Executes :ref:`temporary view <api/db/temp_view>` and returns
        it results according specified parameters.
        :param str include_docs: Include document body for each row
        """
        params = params_from_locals(
            locals(),
            ('self', 'auth', 'map_fun', 'red_fun', 'language',
             'feed_buffer_size', 'params'))
        params['endkey'] = endkey
        params['startkey'] = startkey
        data = {'map': map_fun}
        if red_fun is not None:
            data['reduce'] = red_fun
        if language is not None:
            data['language'] = language
        return (
            await self.view_class(
                self.resource('_temp_view')).request(
                    auth=auth,
                    feed_buffer_size=feed_buffer_size,
                    data=data,
                    params=params))

    async def view_cleanup(self, *, auth: AuthProvider = None) -> dict:
        """:ref:`Removes outdated views <api/db/view_cleanup>` index files.
        """
        resp = await self.resource.post(
            dict(path='_view_cleanup',
                 auth=auth))
        return await resp.json()

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
import base64
from io import RawIOBase
from typing import IO, Union

from aiocouchdb.authn import AuthProvider
from aiocouchdb.hdrs import (
    ACCEPT_RANGES,
    CONTENT_ENCODING,
    CONTENT_TYPE,
    IF_NONE_MATCH,
    RANGE)

from aiocouchdb.client.resource cimport Resource


__all__ = ('Attachment', )


cdef class Attachment(object):
    """Implementation of :ref:`CouchDB Attachment API <api/doc/attachment>`."""

    def __init__(self, url_or_resource: Union[Resource, str], *, name: str = None, loop=None):
        self.resource = (
            Resource(url_or_resource, loop=loop)
            if isinstance(url_or_resource, str)
            else url_or_resource)
        self._name = name

    def __repr__(self) -> str:
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,
            self.resource.url,
            hex(id(self)))

    @property
    def name(self) -> str:
        """Returns attachment name specified in class constructor."""
        return self._name

    async def accepts_range(
            self,
            str rev=None,
            *,
            auth: AuthProvider = None) -> bool:
        """Returns ``True`` if attachments accepts bytes range requests.
        """
        params = {}
        if rev is not None:
            params['rev'] = rev
        resp = await self.resource.head(dict(auth=auth, params=params))
        # await resp.release()
        return resp.headers.get(ACCEPT_RANGES) == 'bytes'

    async def delete(self, str rev, *, auth: AuthProvider = None) -> dict:
        """`Deletes an attachment`_.
        .. _Deletes an attachment: http://docs.couchdb.org/en/latest/api/document/attachments.html#delete--db-docid-attname
        """
        resp = await self.resource.delete(
            dict(auth=auth,
                 params=dict(rev=rev)))
        return await resp.json()

    async def exists(
            self,
            rev: str = None,
            *,
            auth: AuthProvider = None) -> bool:
        """Checks if `attachment exists`_. Assumes success on receiving response
        with `200 OK` status.
        .. _attachment exists: http://docs.couchdb.org/en/latest/api/document/attachments.html#head--db-docid-attname
        """
        params = {}
        if rev is not None:
            params['rev'] = rev
        resp = await self.resource.head(dict(auth=auth, params=params))
        # await resp.release()
        return resp.status == 200

    async def get(
            self,
            str rev=None,
            *,
            auth: AuthProvider = None,
            range: Union[slice, int, list, tuple] = None) -> AttachmentReader:
        """`Returns an attachment`_ reader object.
        .. _Returns an attachment: http://docs.couchdb.org/en/latest/api/document/attachments.html#get--db-docid-attname
        """
        headers = {}
        params = {}
        if rev is not None:
            params['rev'] = rev
        if range is not None:
            if isinstance(range, slice):
                start, stop = range.start, range.stop
            elif isinstance(range, int):
                start, stop = 0, range
            else:
                start, stop = range
            headers[RANGE] = 'bytes={}-{}'.format(start or 0, stop)
        resp = await self.resource.get(
            dict(auth=auth,
                 maybe_raise=False,
                 headers=headers,
                 params=params))
        await resp.maybe_raise_error()
        print("Creating reader fro response", resp)
        return AttachmentReader(resp)

    async def modified(
            self,
            digest: Union[str, bytes],
            *,
            auth: AuthProvider = None) -> bool:
        """Checks if `attachment was modified`_ by known MD5 digest.
        """
        if isinstance(digest, bytes):
            if len(digest) != 16:
                raise ValueError('MD5 digest has 16 bytes')
            digest = base64.b64encode(digest).decode()
        elif isinstance(digest, str):
            if not (len(digest) == 24 and digest.endswith('==')):
                raise ValueError(
                    'invalid base64 encoded MD5 digest')
        else:
            raise TypeError(
                'invalid `digest` type {}, bytes or str expected'
                ''.format(type(digest)))
        resp = await self.resource.head(
            dict(auth=auth,
                 headers={IF_NONE_MATCH: '"%s"' % digest}))
        # await resp.release()
        return resp.status != 304

    async def update(
            self,
            fileobj: IO,
            *,
            auth: AuthProvider = None,
            str content_encoding=None,
            str content_type='application/octet-stream',
            rev=None) -> dict:
        """`Attaches a file`_ to document.
        .. _Attaches a file: http://docs.couchdb.org/en/latest/api/document/attachments.html#put--db-docid-attname
        """
        assert hasattr(fileobj, 'read')
        params = {}
        if rev is not None:
            params['rev'] = rev
        headers = {CONTENT_TYPE: content_type}
        if content_encoding is not None:
            headers[CONTENT_ENCODING] = content_encoding
        resp = await self.resource.put(
            dict(auth=auth,
                 data=fileobj,
                 headers=headers,
                 params=params))
        return await resp.json()


class AttachmentReader(RawIOBase):
    """Attachment reader implements :class:`io.RawIOBase` interface
    with the exception that all I/O bound methods are coroutines."""

    def __init__(self, resp):
        super().__init__()
        self._resp = resp

    def close(self):
        """Closes attachment reader and underlying connection.

        This method has no effect if the attachment is already closed.
        """
        if not self.closed:
            self._resp.close()

    @property
    def closed(self) -> bool:
        """Return a bool indicating whether object is closed."""
        return self._resp.content.at_eof()

    def readable(self) -> bool:
        """Return a bool indicating whether object was opened for reading."""
        return True

    async def read(self, int size=-1) -> Union[bytes, None]:
        """Read and return up to n bytes, where `size` is an :func:`int`.

        Returns an empty bytes object on EOF, or None if the object is
        set not to block and has no data to read.
        """
        return await self._resp.content.read(size)

    async def readall(self, int size=8192) -> bytearray:
        """Read until EOF, using multiple :meth:`read` call."""
        acc = bytearray()
        while not self.closed:
            acc.extend((await self.read(size)))
        return acc

    async def readline(self) -> bytes:
        """Read and return a line of bytes from the stream.

        If limit is specified, at most limit bytes will be read.
        Limit should be an :func:`int`.

        The line terminator is always ``b'\\n'`` for binary files; for text
        files, the newlines argument to open can be used to select the line
        terminator(s) recognized.
        """
        return await self._resp.content.readline()

    async def readlines(self, hint: int = None) -> list:
        """Return a list of lines from the stream.

        `hint` can be specified to control the number of lines read: no more
        lines will be read if the total size (in bytes/characters) of all
        lines so far exceeds `hint`.
        """
        if hint is None or hint <= 0:
            acc = []
            while not self.closed:
                line = await self.readline()
                if line:
                    acc.append(line)
            return acc
        read = 0
        acc = []
        while not self.closed:
            line = await self.readline()
            if not line:
                continue
            acc.append(line)
            read += len(line)
            if read >= hint:
                break
        return acc

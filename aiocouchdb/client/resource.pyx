# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

import asyncio
import functools
from typing import Union

from aiocouchdb.authn import AuthProvider

from .utils cimport urljoin

from .session cimport HttpSession


__all__ = ('Resource', )


cdef tuple METHODS = (
    'head', 'get', 'post', 'put', 'delete', 'copy', 'params')


cdef class Resource(object):
    """HTTP resource representation. Accepts full ``url`` as argument.

    >>> res = Resource('http://localhost:5984')
    >>> res  # doctest: +ELLIPSIS
    <aiocouchdb.client.Resource(http://localhost:5984) object at ...>

    Able to construct new Resource instance by assemble base URL and path
    sections on call:

    >>> new_res = res('foo', 'bar/baz')
    >>> assert new_res is not res
    >>> new_res.url
    'http://localhost:5984/foo/bar%2Fbaz'

    Also holds a :class:`HttpSession` instance and shares it with subresources:

    >>> res.session is new_res.session
    True
    """

    __module__ = "aiocouchdb.client.resource"
    session_class = HttpSession

    def __init__(
            self,
            str url,
            *,
            bool debug=False,
            loop: asyncio.AbstractEventLoop = None,
            HttpSession session=None):
        self.METHODS = METHODS
        self._loop = loop
        self.url = url
        self.debug = debug
        self.session = session or self.session_class()

    def __call__(self, *path) -> Resource:
        return type(self)(
            urljoin(self.url, list(path)),
            loop=self._loop,
            session=self.session)

    def __repr__(self) -> str:
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,
            self.url,
            hex(id(self)))

    def __getattribute__(self, str k):
        if k in self.METHODS:
            return functools.partial(self._request, k.upper())
        return object.__getattribute__(self, k)

    cpdef _request(self, str method, dict kwargs=None):
        return self.request(method, **kwargs or {})

    cpdef log(self, str method, str url, auth: AuthProvider, dict params):
        if self.debug:
            print("aiocouchdb.request", method, auth, params)

    cpdef request(
            self,
            str method,
            str path=None,
            data: Union[bytes, str, dict] = None,
            dict headers=None,
            auth: AuthProvider = None,
            bool maybe_raise = True,
            dict params=None,
            dict options=None):
        """Makes a HTTP request to the resource.
        :returns: :class:`aiocouchdb.client.HttpResponse` instance
        """
        url = urljoin(self.url, [path]) if path else self.url
        options = options or {}
        self.log(method, url, auth, params)
        return self.session.request(
            method, url,
            auth=auth,
            kwargs=dict(
                data=data,
                maybe_raise=maybe_raise,
                headers=headers or {},
                params=params or {},
                loop=options.pop('loop', self._loop),
                **options))

# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

import asyncio

import aiohttp

from aiocouchdb.authn import AuthProvider, NoAuthProvider

from .utils import request


cdef class HttpSession(object):
    """HTTP client session which holds default :class:`Authentication Provider
    <aiocouchdb.authn.AuthProvider>` instance (if any) and :class:`TCP Connector
    <aiohttp.connector.TCPConnector>`."""

    def __init__(
            self,
            *,
            auth: AuthProvider = None,
            connector: aiohttp.BaseConnector = None,
            loop: asyncio.AbstractEventLoop = None):
        self._auth = auth or NoAuthProvider()
        self._loop = (
            asyncio.get_event_loop()
            if loop is None
            else loop)
        self.connector = (
            aiohttp.TCPConnector(
                force_close=False,
                loop=loop)
            if connector is None
            else connector)

    @property
    def auth(self) -> AuthProvider:
        """Default :class:`~aiocouchdb.authn.AuthProvider` instance to apply
        on the requests. By default, :class:`~aiocouchdb.authn.NoAuthProvider`
        is used assuming that actual provider will get passed with `auth` query
        parameter on :meth:`request` call, but user may freely override it
        with your own.

        .. warning::

            Try avoid to use :class:`~aiocouchdb.authn.CookieAuthProvider` here
            since currently :class:`HttpSession` cannot renew the cookie in case
            it get expired.

        """
        return self._auth

    @auth.setter
    def auth(self, value):
        if value is None:
            self._auth = NoAuthProvider()
        else:
            assert isinstance(value, AuthProvider)
            self._auth = value

    cpdef request(self, str method, str url, auth: AuthProvider = None, kwargs: dict = None):
        """Makes a HTTP request with applying authentication routines.
        :returns: :class:`aiocouchdb.client.HttpResponse` instance
        """
        return (auth or self._auth).wrap(
            request)(
                method,
                url,
                **kwargs or {})

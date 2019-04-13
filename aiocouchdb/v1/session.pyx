# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from aiocouchdb.authn import CookieAuthProvider


__all__ = ('Session', )


cdef class Session(object):
    """Implements :ref:`/_session <api/auth/session>` API.  Should be used
    via :attr:`server.session <aiocouchdb.v1.server.Server.session>` property.
    """

    cookie_auth_provider_class = CookieAuthProvider

    def __init__(self, resource):
        self.resource = resource('_session')

    def __repr__(self):
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.resource.url,
            hex(id(self)))

    async def open(self, str name, str password) -> CookieAuthProvider:
        """Opens session for cookie auth provider and returns the auth provider
        back for usage in further requests.
        """
        auth = self.cookie_auth_provider_class()
        doc = {'name': name, 'password': password}
        resp = await self.resource.post(dict(auth=auth, data=doc))
        await resp.release()
        return auth

    async def info(self, *, auth=None) -> dict:
        """Returns information about authenticated user.
        Usable for any :class:`~aiocouchdb.authn.AuthProvider`.
        """
        resp = await self.resource.get(dict(auth=auth))
        return await resp.json()

    async def close(self, *, auth=None):
        """Closes active cookie session.
        Uses for :class:`aiocouchdb.authn.CookieAuthProvider`."""
        resp = await self.resource.delete(dict(auth=auth))
        return await resp.json()

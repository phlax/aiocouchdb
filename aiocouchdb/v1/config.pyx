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
from typing import Union

from aiocouchdb.authn import AuthProvider


__all__ = ('ServerConfig', )


cdef class ServerConfig(object):
    """Implements :ref:`/_config/* <api/config>` API. Should be used via
    :attr:`server.config <aiocouchdb.v1.server.Server.config>` property."""

    def __init__(self, resource):
        self.resource = resource('_config')

    def __repr__(self) -> str:
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,
            self.resource.url,
            hex(id(self)))

    async def exists(
            self,
            str section,
            str key,
            *,
            auth: AuthProvider = None) -> bool:
        """Checks if :ref:`configuration option <api/config/section/key>`
        exists.
        """
        resp = await self.resource(
            section, key).head(dict(auth=auth))
        await resp.read()
        return resp.status == 200

    async def get(
            self,
            str section=None,
            str key=None,
            *,
            auth: AuthProvider = None) -> Union[dict, str]:
        """Returns :ref:`server configuration <api/config>`. Depending on
        specified arguments returns:

        - :ref:`Complete configuration <api/config>` if ``section`` and ``key``
          are ``None``

        - :ref:`Section options <api/config/section>` if ``section``
          was specified

        - :ref:`Option value <api/config/section/key>` if both ``section``
          and ``key`` were specified
        """
        path = []
        if section is not None:
            path.append(section)
        if key is not None:
            assert isinstance(section, str)
            path.append(key)
        resp = await self.resource(
            *path).get(
                dict(auth=auth))
        return await resp.json()

    async def update(
            self,
            str section,
            str key,
            str value,
            *,
            auth: AuthProvider = None) -> str:
        """Updates specific :ref:`configuration option <api/config/section/key>`
        value and returns the old one back.
        """
        resp = await self.resource(
            section).put(
                dict(path=key,
                     auth=auth,
                     data=value))
        return await resp.json()

    async def delete(
            self,
            str section,
            str key,
            *,
            auth: AuthProvider = None) -> str:
        """Deletes specific :ref:`configuration option <api/config/section/key>`
        and returns it value back.
        """
        resp = await self.resource(section).delete(dict(path=key, auth=auth))
        return await resp.json()

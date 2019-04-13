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
import json
from typing import Union

from aiocouchdb.authn import AuthProvider
from .feeds cimport ViewFeed


__all__ = (
    'View',
)


cdef class View(object):
    """Views requesting helper."""

    def __init__(self, resource):
        self.resource = resource

    async def request(
            self,
            *,
            auth: AuthProvider = None,
            feed_buffer_size: int = None,
            dict data=None,
            dict params=None) -> ViewFeed:
        """Requests a view associated with the owned resource.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        """
        if params is not None:
            params, data = self.handle_keys_param(params, data)
            params = self.prepare_params(params)
        request = (
            self.resource.post
            if data
            else self.resource.get)
        resp = await request(
            dict(auth=auth,
                 data=data,
                 params=params))
        return ViewFeed(resp, buffer_size=feed_buffer_size)

    @staticmethod
    def prepare_params(dict params) -> dict:
        json_params = {'key', 'keys', 'startkey', 'endkey'}
        result = {}
        for key, value in params.items():
            if key in json_params:
                if value is Ellipsis:
                    continue
                value = json.dumps(value)
            elif value is None:
                continue
            result[key] = value
        return result

    @staticmethod
    def handle_keys_param(dict params, data: Union[dict, None]) -> tuple:
        keys = params.pop('keys', ())
        if keys is None or keys is Ellipsis:
            return params, data
        assert not isinstance(keys, (bytes, str))

        if len(keys) >= 2:
            if data is None:
                data = {'keys': keys}
            elif isinstance(data, dict):
                data['keys'] = keys
            else:
                params['keys'] = keys
        elif keys:
            assert params.get('key') is None
            params['key'] = keys[0]

        return params, data

# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import io
import types
import unittest.mock as mock

from aioclient.request import ClientRequest
from aiohttp.payload import BytesIOPayload

import aiocouchdb.authn
import aiocouchdb.client

from . import utils

import yarl


class ResourceTestCase(utils.TestCase):

    _test_target = 'mock'

    async def test_head_request(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.head()
        self.assert_request_called_with('HEAD')

    async def test_get_request(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.get()
        self.assert_request_called_with('GET')

    async def test_post_request(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.post()
        self.assert_request_called_with('POST')

    async def test_put_request(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.put()
        self.assert_request_called_with('PUT')

    async def test_delete_request(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.delete()
        self.assert_request_called_with('DELETE')

    async def test_copy_request(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.copy()
        self.assert_request_called_with('COPY')

    async def __test_options_request(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.options()
        self.assert_request_called_with('OPTIONS')

    def test_to_str(self):
        res = aiocouchdb.client.Resource(self.url)
        self.assertEqual(
            '<aiocouchdb.client.resource.Resource(http://localhost:5984) object at {}>'
            ''.format(hex(id(res))),
            str(res))

    def test_on_call(self):
        res = aiocouchdb.client.Resource(self.url)
        new_res = res('foo', 'bar/baz')
        self.assertIsNot(res, new_res)
        self.assertEqual('http://localhost:5984/foo/bar%2Fbaz', new_res.url)

    def test_empty_call(self):
        res = aiocouchdb.client.Resource(self.url)
        new_res = res()
        self.assertIsNot(res, new_res)
        self.assertEqual('http://localhost:5984', new_res.url)

    async def test_request_with_path(self):
        res = aiocouchdb.client.Resource(self.url)
        await res.request('get', 'foo/bar')
        self.assert_request_called_with('get', 'foo/bar')

    async def test_override_request_class(self):
        class Thing(object):
            pass
        res = aiocouchdb.client.Resource(self.url)
        await res.request('get', options=dict(request_class=Thing))
        self.assert_request_called_with('get', request_class=Thing)

    async def test_override_response_class(self):
        class Thing(object):
            pass
        res = aiocouchdb.client.Resource(self.url)
        await res.request('get', options=dict(response_class=Thing))
        self.assert_request_called_with('get', response_class=Thing)


class ClientRequestTestCase(utils.TestCase):

    _test_target = 'mock'

    def test_encode_json_body(self):
        req = ClientRequest(
            'post', yarl.URL(self.url),
            headers={
                'content_type': "application/json"},
            data={'foo': 'bar'})
        self.assertEqual(b'{"foo": "bar"}', req.body._value)

    # BROKEN: this no longer seems to work out of the box
    def __test_correct_encode_boolean_params(self):
        req = ClientRequest(
            'get', yarl.URL(self.url),
            params={'foo': True})
        self.assertEqual('/?foo=true', req.path)

        req = ClientRequest(
            'get', yarl.URL(self.url),
            params={'bar': False})
        self.assertEqual('/?bar=false', req.path)

    def test_encode_chunked_json_body(self):
        from aiohttp.payload import AsyncIterablePayload
        from async_generator import async_generator, yield_

        @async_generator
        async def _payload():
            return yield_('{"foo": "bar"}')
        data = AsyncIterablePayload(_payload())
        req = ClientRequest(
            'post', yarl.URL(self.url),
            data=data)
        assert req.body == data

    def test_encode_readable_object(self):
        req = ClientRequest(
            'post', yarl.URL(self.url), data=io.BytesIO(b'foobarbaz'))
        self.assertIsInstance(req.body, BytesIOPayload)


class ClientResponseTestCase(utils.TestCase):

    _test_target = 'mock'

    async def test_read_body(self):
        with self.response(data=b'{"couchdb": "Welcome!"}') as resp:
            result = await resp.read()
        self.assertEqual(b'{"couchdb": "Welcome!"}', result)

    async def test_decode_json_body(self):
        with self.response(data=b'{"couchdb": "Welcome!"}') as resp:
            result = await resp.json()
        self.assertEqual({'couchdb': 'Welcome!'}, result)

    async def test_decode_json_from_empty_body(self):
        with self.response(data=b'') as resp:
            result = await resp.json()
        self.assertEqual(None, result)

# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import base64
import hashlib
import io

import aiocouchdb.client
import aiocouchdb.v1.attachment
import aiocouchdb.v1.document
from aiocouchdb.v1.attachment import AttachmentReader

from .base import AsyncMock
from . import utils


class AttachmentTestCase(utils.AttachmentTestCase):

    def request_path(self, att=None, *parts):
        attname = att.name if att is not None else self.attbin.name
        return [self.db.name, self.doc.id, attname] + list(parts)

    def test_init_with_url(self):
        self.assertIsInstance(self.attbin.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_att)
        att = aiocouchdb.v1.attachment.Attachment(res)
        self.assertIsInstance(att.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_att, att.resource.url)

    def test_init_with_name(self):
        res = aiocouchdb.client.Resource(self.url_att)
        att = aiocouchdb.v1.attachment.Attachment(res, name='foo.txt')
        self.assertEqual(att.name, 'foo.txt')

    async def test_init_with_name_from_doc(self):
        att = await self.doc.att('bar.txt')
        self.assertEqual(att.name, 'bar.txt')

    async def test_exists(self):
        result = await self.attbin.exists()
        self.assert_request_called_with('HEAD', *self.request_path())
        self.assertTrue(result)

    async def test_exists_rev(self):
        result = await self.attbin.exists(self.rev)
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        params={'rev': self.rev})
        self.assertTrue(result)

    @utils.with_fixed_admin_party('root', 'relax')
    async def test_exists_forbidden(self, root):
        with self.response():
            await self.db.security.update_members(names=['foo', 'bar'],
                                                       auth=root)
        with self.response(status=403):
            result = await self.attbin.exists()
            self.assert_request_called_with('HEAD', *self.request_path())
        self.assertFalse(result)

    async def test_exists_not_found(self):
        with self.response(status=404):
            attname = utils.uuid()
            result = await self.doc[attname].exists()
            self.assert_request_called_with(
                'HEAD', self.db.name, self.doc.id, attname)
        self.assertFalse(result)

    async def test_modified(self):
        digest = hashlib.md5(utils.uuid().encode()).digest()
        reqdigest = '"{}"'.format(base64.b64encode(digest).decode())
        result = await self.attbin.modified(digest)
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        headers={'If-None-Match': reqdigest})
        self.assertTrue(result)

    async def test_not_modified(self):
        digest = hashlib.md5(b'Time to relax!').digest()
        reqdigest = '"Ehemn5lWOgCMUJ/c1x0bcg=="'

        with self.response(status=304):
            result = await self.attbin.modified(digest)
            self.assert_request_called_with(
                'HEAD', *self.request_path(),
                headers={'If-None-Match': reqdigest})
        self.assertFalse(result)

    async def test_modified_with_base64_digest(self):
        digest = base64.b64encode(hashlib.md5(b'foo').digest()).decode()
        reqdigest = '"rL0Y20zC+Fzt72VPzMSk2A=="'
        result = await self.attbin.modified(digest)
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        headers={'If-None-Match': reqdigest})
        self.assertTrue(result)

    async def test_modified_invalid_digest(self):
        with self.assertRaises(TypeError):
            await self.attbin.modified({})

        with self.assertRaises(ValueError):
            await self.attbin.modified(b'foo')

        with self.assertRaises(ValueError):
            await self.attbin.modified('bar')

    async def test_accepts_range(self):
        with self.response(headers={'Accept-Ranges': 'bytes'}):
            result = await self.attbin.accepts_range()
            self.assert_request_called_with('HEAD', *self.request_path())
        self.assertTrue(result)

    async def test_accepts_range_not(self):
        result = await self.atttxt.accepts_range()
        self.assert_request_called_with('HEAD', *self.request_path(self.atttxt))
        self.assertFalse(result)

    async def test_accepts_range_with_rev(self):
        result = await self.atttxt.accepts_range(rev=self.rev)
        self.assert_request_called_with('HEAD', *self.request_path(self.atttxt),
                                        params={'rev': self.rev})
        self.assertFalse(result)

    async def test_get(self):
        result = await self.attbin.get()
        self.assert_request_called_with('GET', *self.request_path())
        self.assertIsInstance(result, AttachmentReader)

    async def test_get_rev(self):
        result = await self.attbin.get(self.rev)
        self.assert_request_called_with('GET', *self.request_path(),
                                        params={'rev': self.rev})
        self.assertIsInstance(result, AttachmentReader)

    async def test_get_range(self):
        await self.attbin.get(range=slice(12, 24))
        self.assert_request_called_with(
            'GET', *self.request_path(),
            headers={'Range': 'bytes=12-24'})

    async def test_get_range_from_start(self):
        await self.attbin.get(range=slice(42))
        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'Range': 'bytes=0-42'})

    async def test_get_range_iterable(self):
        await self.attbin.get(range=[11, 22])
        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'Range': 'bytes=11-22'})

    async def test_get_range_int(self):
        await self.attbin.get(range=42)
        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'Range': 'bytes=0-42'})

    async def test_get_bad_range(self):
        with self.response(status=416):
            with self.assertRaises(aiocouchdb.RequestedRangeNotSatisfiable):
                await self.attbin.get(range=slice(1024, 8192))

        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'Range': 'bytes=1024-8192'})

    async def test_update(self):
        await self.attbin.update(io.BytesIO(b''), rev=self.rev)
        self.assert_request_called_with(
            'PUT', *self.request_path(),
            data=Ellipsis,
            headers={'Content-Type': 'application/octet-stream'},
            params={'rev': self.rev})

    async def test_update_ctype(self):

        await self.attbin.update(
            io.BytesIO(b''),
            content_type='foo/bar',
            rev=self.rev)

        self.assert_request_called_with(
            'PUT', *self.request_path(),
            data=Ellipsis,
            headers={'Content-Type': 'foo/bar'},
            params={'rev': self.rev})

    async def test_update_with_encoding(self):
        await self.attbin.update(io.BytesIO(b''),
                                      content_encoding='gzip',
                                      rev=self.rev)
        self.assert_request_called_with(
            'PUT', *self.request_path(),
            data=Ellipsis,
            headers={'Content-Type': 'application/octet-stream',
                     'Content-Encoding': 'gzip'},
            params={'rev': self.rev})

    async def test_delete(self):
        await self.attbin.delete(self.rev)
        self.assert_request_called_with('DELETE', *self.request_path(),
                                        params={'rev': self.rev})


class AttachmentReaderTestCase(utils.TestCase):

    _test_target = 'mock'

    def setUp(self):
        super().setUp()
        self.att = AttachmentReader(self.request)

    def test_close(self):
        self.request.content.at_eof.return_value = False
        self.att.close()
        self.assertTrue(self.request.close.called)

    def test_closed(self):
        _ = self.att.closed
        self.assertTrue(self.request.content.at_eof.called)

    def test_close_when_closed(self):
        self.request.content.at_eof.return_value = True
        self.att.close()
        self.assertFalse(self.request.close.called)

    def test_readable(self):
        self.assertTrue(self.att.readable())

    def test_writable(self):
        self.assertFalse(self.att.writable())

    def test_seekable(self):
        self.assertFalse(self.att.seekable())

    async def test_read(self):
        self.request.content.read = AsyncMock()
        await self.att.read()
        self.request.content.read.assert_called_once_with(-1)

    async def test_read_some(self):
        self.request.content.read = AsyncMock()
        await self.att.read(10)
        self.request.content.read.assert_called_once_with(10)

    async def test_readall(self):
        with self.response(data=[b'...', b'---']) as resp:
            self.att._resp = resp
            res = await self.att.readall()

        resp.content.read.assert_called_with(8192)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertIsInstance(res, bytearray)

    async def test_readline(self):
        self.request.content.readline = AsyncMock()
        await self.att.readline()
        self.request.content.readline.assert_called_once_with()

    async def test_readlines(self):
        with self.response(data=[b'...', b'---']) as resp:
            resp.content.readline = resp.content.read
            self.att._resp = resp
            res = await self.att.readlines()

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertEqual(res, [b'...', b'---'])

    async def test_readlines_hint(self):
        with self.response(data=[b'...', b'---']) as resp:
            resp.content.readline = resp.content.read
            self.att._resp = resp
            res = await self.att.readlines(2)

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 1)
        self.assertEqual(res, [b'...'])

    async def test_readlines_hint_more(self):
        with self.response(data=[b'...', b'---']) as resp:
            resp.content.readline = resp.content.read
            self.att._resp = resp
            res = await self.att.readlines(42)

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertEqual(res, [b'...', b'---'])

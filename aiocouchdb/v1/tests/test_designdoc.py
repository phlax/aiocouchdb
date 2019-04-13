# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import json

import aiocouchdb.client
import aiocouchdb.feeds
import aiocouchdb.v1.database
import aiocouchdb.v1.document
import aiocouchdb.v1.designdoc

from aiocouchdb.client import urljoin
from . import utils


class DesignDocTestCase(utils.DesignDocumentTestCase):

    def request_path(self, *parts):
        return [self.db.name] + self.ddoc.id.split('/') + list(parts)

    def test_init_with_url(self):
        self.assertIsInstance(self.ddoc.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_ddoc)
        ddoc = aiocouchdb.v1.designdoc.DesignDocument(res)
        self.assertIsInstance(ddoc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_ddoc, ddoc.resource.url)

    def test_init_with_id(self):
        res = aiocouchdb.client.Resource(self.url_ddoc)
        ddoc = aiocouchdb.v1.designdoc.DesignDocument(res, docid='foo')
        self.assertEqual(ddoc.id, 'foo')

    async def test_init_with_id_from_database(self):
        db = aiocouchdb.v1.database.Database(urljoin(self.url, ['dbname']),
                                          dbname='dbname')
        ddoc = await db.ddoc('foo')
        self.assertEqual(ddoc.id, '_design/foo')

    def test_get_item_returns_attachment(self):
        att = self.ddoc['attname']
        with self.assertRaises(AssertionError):
            self.assert_request_called_with(
                'HEAD', *self.request_path('attname'))
        self.assertIsInstance(att, self.ddoc.document_class.attachment_class)

    def test_ddoc_name(self):
        res = aiocouchdb.client.Resource(self.url_ddoc)
        ddoc = aiocouchdb.v1.designdoc.DesignDocument(res, docid='_design/bar')
        self.assertEqual(ddoc.name, 'bar')

    def test_ddoc_bad_name_because_of_bad_id(self):
        res = aiocouchdb.client.Resource(self.url_ddoc)
        ddoc = aiocouchdb.v1.designdoc.DesignDocument(res, docid='bar')
        self.assertEqual(ddoc.name, None)

    def test_access_to_document_api(self):
        self.assertIsInstance(self.ddoc.doc, aiocouchdb.v1.document.Document)

    def test_access_to_custom_document_api(self):
        class CustomDoc(aiocouchdb.v1.document.Document):
            def __init__(self, resource, **kwargs):
                pass
        ddoc = aiocouchdb.v1.designdoc.DesignDocument(
            '',
            document_class=CustomDoc)
        self.assertIsInstance(ddoc.doc, CustomDoc)

    async def test_info(self):
        with self.response(data=b'{}'):
            result = await self.ddoc.info()
            self.assert_request_called_with('GET', *self.request_path('_info'))
        self.assertIsInstance(result, dict)

    async def test_view(self):
        with (await self.ddoc.view('viewname')) as view:
            self.assert_request_called_with(
                'GET', *self.request_path('_view', 'viewname'))
            self.assertIsInstance(view, aiocouchdb.feeds.ViewFeed)

    async def test_view_key(self):
        with (await self.ddoc.view('viewname', 'foo')) as view:
            self.assert_request_called_with(
                'GET', *self.request_path('_view', 'viewname'),
                params={'key': '"foo"'})
            self.assertIsInstance(view, aiocouchdb.feeds.ViewFeed)

    async def test_view_keys(self):
        with (await self.ddoc.view('viewname', 'foo', 'bar')) as view:
            self.assert_request_called_with(
                'POST', *self.request_path('_view', 'viewname'),
                data={'keys': ('foo', 'bar')})
            self.assertIsInstance(view, aiocouchdb.feeds.ViewFeed)

    async def test_view_startkey_none(self):
        with (await self.ddoc.view('viewname', startkey=None)):
            self.assert_request_called_with(
                'GET', *self.request_path('_view', 'viewname'),
                params={'startkey': 'null'})

    async def test_view_endkey_none(self):
        with (await self.ddoc.view('viewname', endkey=None)):
            self.assert_request_called_with(
                'GET', *self.request_path('_view', 'viewname'),
                params={'endkey': 'null'})

    @utils.run_for('mock')
    async def test_view_params(self):
        all_params = {
            'att_encoding_info': False,
            'attachments': False,
            'conflicts': True,
            'descending': True,
            'endkey': 'foo',
            'endkey_docid': 'foo_id',
            'group': False,
            'group_level': 10,
            'include_docs': True,
            'inclusive_end': False,
            'limit': 10,
            'reduce': True,
            'skip': 20,
            'stale': 'ok',
            'startkey': 'bar',
            'startkey_docid': 'bar_id',
            'update_seq': True
        }

        for key, value in all_params.items():
            result = await self.ddoc.view('viewname', **{key: value})
            if key in ('endkey', 'startkey'):
                value = json.dumps(value)
            self.assert_request_called_with(
                'GET', *self.request_path('_view', 'viewname'),
                params={key: value})
            self.assertIsInstance(result, aiocouchdb.feeds.ViewFeed)

    async def test_list(self):
        with (await self.ddoc.list('listname')) as resp:
            self.assert_request_called_with(
                'GET', *self.request_path('_list', 'listname'))
            self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    async def test_list_view(self):
        with (await self.ddoc.list('listname', 'viewname')) as resp:
            self.assert_request_called_with(
                'GET', *self.request_path('_list', 'listname', 'viewname'))
            self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    async def test_list_view_ddoc(self):
        with (await self.ddoc.list('listname', 'ddoc/view')) as resp:
            self.assert_request_called_with(
                'GET', *self.request_path('_list', 'listname', 'ddoc', 'view'))
            self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    @utils.run_for('mock')
    async def test_list_params(self):
        all_params = {
            'att_encoding_info': False,
            'attachments': False,
            'conflicts': True,
            'descending': True,
            'endkey': 'foo',
            'endkey_docid': 'foo_id',
            'format': 'json',
            'group': False,
            'group_level': 10,
            'include_docs': True,
            'inclusive_end': False,
            'limit': 10,
            'reduce': True,
            'skip': 20,
            'stale': 'ok',
            'startkey': 'bar',
            'startkey_docid': 'bar_id',
            'update_seq': True
        }

        for key, value in all_params.items():
            result = await self.ddoc.list('listname', 'viewname',
                                               **{key: value})
            if key in ('endkey', 'startkey'):
                value = json.dumps(value)
            self.assert_request_called_with(
                'GET', *self.request_path('_list', 'listname', 'viewname'),
                params={key: value})
            self.assertIsInstance(result, aiocouchdb.client.ClientResponse)

    async def test_list_custom_headers(self):
        with (await self.ddoc.list('listname', headers={'Foo': '1'})):
            self.assert_request_called_with(
                'GET', *self.request_path('_list', 'listname'),
                headers={'Foo': '1'})

    async def test_list_custom_params(self):
        with (await self.ddoc.list('listname', params={'foo': '1'})):
            self.assert_request_called_with(
                'GET', *self.request_path('_list', 'listname'),
                params={'foo': '1'})

    async def test_list_key(self):
        with (await self.ddoc.list('listname', 'viewname', 'foo')):
            self.assert_request_called_with(
                'GET', *self.request_path('_list', 'listname', 'viewname'),
                params={'key': '"foo"'})

    async def test_list_keys(self):
        with (await self.ddoc.list('listname', 'viewname', 'foo', 'bar')):
            self.assert_request_called_with(
                'POST', *self.request_path('_list', 'listname', 'viewname'),
                data={'keys': ('foo', 'bar')})

    async def test_show(self):
        with (await self.ddoc.show('time')) as resp:
            self.assert_request_called_with(
                'GET', *self.request_path('_show', 'time'))
            self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    async def test_show_docid(self):
        with (await self.ddoc.show('time', 'docid')) as resp:
            self.assert_request_called_with(
                'GET', *self.request_path('_show', 'time', 'docid'))
            self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    async def test_show_custom_method(self):
        with (await self.ddoc.show('time', method='HEAD')):
            self.assert_request_called_with(
                'HEAD', *self.request_path('_show', 'time'))

    async def test_show_custom_headers(self):
        with (await self.ddoc.show('time', headers={'foo': 'bar'})):
            self.assert_request_called_with(
                'GET', *self.request_path('_show', 'time'),
                headers={'foo': 'bar'})

    async def test_show_custom_data(self):
        with (await self.ddoc.show('time', data={'foo': 'bar'})):
            self.assert_request_called_with(
                'POST', *self.request_path('_show', 'time'),
                data={'foo': 'bar'})

    async def test_show_custom_params(self):
        with (await self.ddoc.show('time', params={'foo': 'bar'})):
            self.assert_request_called_with(
                'GET', *self.request_path('_show', 'time'),
                params={'foo': 'bar'})

    async def test_show_format(self):
        with (await self.ddoc.show('time', format='xml')):
            self.assert_request_called_with(
                'GET', *self.request_path('_show', 'time'),
                params={'format': 'xml'})

    async def test_update(self):
        with (await self.ddoc.update('fun')) as resp:
            self.assert_request_called_with(
                'POST', *self.request_path('_update', 'fun'))
            self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    async def test_update_docid(self):
        with (await self.ddoc.update('fun', 'docid')) as resp:
            self.assert_request_called_with(
                'PUT', *self.request_path('_update', 'fun', 'docid'))
            self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    async def test_update_custom_method(self):
        with (await self.ddoc.update('fun', method='HEAD')):
            self.assert_request_called_with(
                'HEAD', *self.request_path('_update', 'fun'))

    async def test_update_custom_headers(self):
        with (await self.ddoc.update('fun', headers={'foo': 'bar'})):
            self.assert_request_called_with(
                'POST', *self.request_path('_update', 'fun'),
                headers={'foo': 'bar'})

    async def test_update_custom_data(self):
        with (await self.ddoc.update('fun', data={'foo': 'bar'})):
            self.assert_request_called_with(
                'POST', *self.request_path('_update', 'fun'),
                data={'foo': 'bar'})

    async def test_update_custom_params(self):
        with (await self.ddoc.update('fun', params={'foo': 'bar'})):
            self.assert_request_called_with(
                'POST', *self.request_path('_update', 'fun'),
                params={'foo': 'bar'})

    async def test_rewrite(self):
        with (await self.ddoc.rewrite('rewrite', 'me')) as resp:
            self.assert_request_called_with(
                'GET', *self.request_path('_rewrite', 'rewrite', 'me'))
        self.assertIsInstance(resp, aiocouchdb.client.ClientResponse)

    async def test_rewrite_custom_method(self):
        with (await self.ddoc.rewrite('path', method='HEAD')):
            self.assert_request_called_with(
                'HEAD', *self.request_path('_rewrite', 'path'))

    async def test_rewrite_custom_headers(self):
        with (await self.ddoc.rewrite('path', headers={'foo': '42'})):
            self.assert_request_called_with(
                'GET', *self.request_path('_rewrite', 'path'),
                headers={'foo': '42'})

    async def test_rewrite_custom_data(self):
        with (await self.ddoc.rewrite('path', data={'foo': 'bar'})):
            self.assert_request_called_with(
                'POST', *self.request_path('_rewrite', 'path'),
                data={'foo': 'bar'})

    async def test_rewrite_custom_params(self):
        with (await self.ddoc.rewrite('path', params={'foo': 'bar'})):
            self.assert_request_called_with(
                'GET', *self.request_path('_rewrite', 'path'),
                params={'foo': 'bar'})

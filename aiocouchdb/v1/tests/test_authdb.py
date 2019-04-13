# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json

import aiocouchdb.v1.authdb

from aiocouchdb.client import urljoin
from . import utils


class AuthDatabaseTestCase(utils.ServerTestCase):

    def setUp(self):
        super().setUp()
        self.url_db = urljoin(self.url, ['_users'])
        self.db = aiocouchdb.v1.authdb.AuthDatabase(self.url_db)

    def test_get_doc_with_prefix(self):
        doc = self.db['test']
        self.assertEqual(doc.id, self.db.document_class.doc_prefix + 'test')

        doc = self.db[self.db.document_class.doc_prefix + 'test']
        self.assertEqual(doc.id, self.db.document_class.doc_prefix + 'test')


class UserDocumentTestCase(utils.ServerTestCase):

    def setUp(self):
        super().setUp()
        self.username = utils.uuid()
        docid = aiocouchdb.v1.authdb.UserDocument.doc_prefix + self.username
        self.url_doc = urljoin(self.url, ['_users', docid])
        self.doc = aiocouchdb.v1.authdb.UserDocument(self.url_doc, docid=docid)

    def tearDown(self):
        self.loop.run_until_complete(self.teardown_document())
        super().tearDown()

    async def setup_document(self, password, **kwargs):
        data = {
            '_id': self.doc.id,
            'name': self.doc.name,
            'password': password,
            'roles': [],
            'type': 'user'
        }
        data.update(kwargs)
        with self.response(data=b'{}'):
            resp = await self.doc.register(password, **kwargs)
        self.assert_request_called_with('PUT', '_users', self.doc.id, data=data)
        self.assertIsInstance(resp, dict)
        return resp

    async def teardown_document(self):
        if not (await self.doc.exists()):
            return
        with self.response(headers={'Etag': '"1-ABC"'}):
            rev = await self.doc.rev()
        await self.doc.delete(rev)

    def test_require_docid(self):
        with self.assertRaises(ValueError):
            aiocouchdb.v1.authdb.UserDocument(self.url_doc)

    def test_username(self):
        self.assertEqual(self.doc.name, self.username)

    async def test_register(self):
        await self.setup_document('s3cr1t')

    async def test_register_with_additional_data(self):
        await self.setup_document('s3cr1t', email='user@example.com')

    async def test_change_password(self):
        await self.setup_document('s3cr1t')
        with self.response(data=b'{}'):
            doc = await self.doc.get()
        data = json.dumps(doc).encode()

        with self.response(data=data):
            await self.doc.update_password('n3ws3cr1t')
            doc['password'] = 'n3ws3cr1t'
            self.assert_request_called_with(
                'PUT', '_users', self.doc.id,
                data=doc)

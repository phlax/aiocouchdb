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
import contextlib
import datetime
import functools
import os
import random
import unittest
import unittest.mock as mock
import uuid as _uuid
from collections import deque, defaultdict

import aiocouchdb.client
import aiocouchdb.errors
from aiocouchdb.client import urljoin, extract_credentials

import yarl

from .base import AsyncMock


TARGET = os.environ.get('AIOCOUCHDB_TARGET', 'mock')

_URL = yarl.URL('http://bar.foo')


def run_in_loop(f):
    @functools.wraps(f)
    def wrapper(testcase, *args, **kwargs):
        coro = asyncio.coroutine(f)
        future = asyncio.wait_for(coro(testcase, *args, **kwargs),
                                  timeout=testcase.timeout)
        return testcase.loop.run_until_complete(future)
    return wrapper


class MetaAioTestCase(type):

    def __new__(cls, name, bases, attrs):
        for key, obj in attrs.items():
            if key.startswith('test_'):
                attrs[key] = run_in_loop(obj)
        return super().__new__(cls, name, bases, attrs)


class TestCase(unittest.TestCase, metaclass=MetaAioTestCase):

    _test_target = TARGET
    timeout = 10
    url = 'http://localhost:5984'

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        wraps = None
        if self._test_target != 'mock':
            wraps = self._request_tracer(aiocouchdb.client.session.request)
        self._patch = mock.patch('aiocouchdb.client.session.request', wraps=wraps)
        self.request = self._patch.start()
        response = self.prepare_response()
        self._set_response(response)
        self._req_per_task = defaultdict(list)
        self.loop.run_until_complete(self.setup_env())

    def tearDown(self):
        self.loop.run_until_complete(self.teardown_env())
        self._patch.stop()
        self.loop.close()

    async def setup_env(self):
        sup = super()
        if hasattr(sup, 'setup_env'):
            await sup.setup_env()

    async def teardown_env(self):
        sup = super()
        if hasattr(sup, 'teardown_env'):
            await sup.teardown_env()

    def future(self, obj):
        fut = asyncio.Future(loop=self.loop)
        fut.set_result(obj)
        return fut

    def _request_tracer(self, f):

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            current_task = asyncio.Task.current_task(loop=self.loop)
            self._req_per_task[current_task].append((args, kwargs))
            return f(*args, **kwargs)
        return wrapper

    def prepare_response(self, *,
                         cookies=None,
                         data=b'',
                         err=None,
                         headers=None,
                         status=200):
        def make_side_effect(queue):
            def side_effect(*args, **kwargs):
                fut = asyncio.Future(loop=self.loop)
                if queue:
                    resp.content.at_eof.return_value = False
                    fut.set_result(queue.popleft())
                elif err:
                    fut.set_exception(err)
                else:
                    resp.content.at_eof.return_value = True
                    fut.set_result(b'')
                return fut
            return side_effect

        headers = headers or {}
        headers.setdefault('Content-Type', 'application/json')
        cookies = cookies or {}

        if isinstance(data, list):
            chunks_queue = deque(data)
            lines_queue = deque((b''.join(data)).splitlines(keepends=True))
        else:
            chunks_queue = deque([data])
            lines_queue = deque(data.splitlines(keepends=True))
        from aiocouchdb.client.response import Py__ClientResponse as ClientResponse
        from aioclient.info import RequestInfo
        resp = ClientResponse(
            'get',
            _URL,
            request_info=RequestInfo(),
            writer=None,
            continue100=None,
            timer=None,
            traces=[],
            loop=self.loop,
            session=None)

        # resp._post_init(self.loop)
        resp.status = status
        resp._headers = headers
        resp.cookies = cookies
        resp.content = unittest.mock.Mock()
        resp.content._buffer = bytearray()
        resp.content.at_eof.return_value = False
        resp.content.read.side_effect = make_side_effect(chunks_queue)
        resp.content.readany.side_effect = make_side_effect(chunks_queue)
        resp.content.readline = make_side_effect(lines_queue)
        resp.close = mock.Mock()
        return resp

    @contextlib.contextmanager
    def response(self, *,
                 cookies=None,
                 data=b'',
                 err=None,
                 headers=None,
                 status=200):
        resp = self.prepare_response(
            cookies=cookies,
            data=data,
            err=err,
            headers=headers,
            status=status)
        self._set_response(resp)
        yield resp
        resp.close()
        self._set_response(self.prepare_response())

    def _set_response(self, resp):
        if self._test_target == 'mock':
            self.request.return_value = self.future(resp)

    def assert_request_called_with(self, method, *path, **kwargs):
        self.assertTrue(self.request.called and self.request.call_count >= 1)

        current_task = asyncio.Task.current_task(loop=self.loop)
        if current_task in self._req_per_task:
            call_args, call_kwargs = self._req_per_task[current_task][-1]
        else:
            call_args, call_kwargs = self.request.call_args
        self.assertEqual((method, urljoin(self.url, list(path))), call_args)

        kwargs.setdefault('data', None)
        kwargs.setdefault('headers', {})
        kwargs.setdefault('params', {})
        for key, value in kwargs.items():
            try:
                self.assertIn(key, call_kwargs)
                if value is not Ellipsis:
                    self.assertEqual(value, call_kwargs[key])
            except:
                print("Failed key: %s" % key)
                raise


class ServerTestCase(TestCase):

    server_class = None
    url = os.environ.get('AIOCOUCHDB_URL', 'http://localhost:5984')

    async def setup_env(self):
        self.url, creds = extract_credentials(self.url)
        self.server = self.server_class(self.url, loop=self.loop)
        if creds is not None:
            self.cookie = await self.server.session.open(*creds)
        else:
            self.cookie = None
        sup = super()
        if hasattr(sup, 'setup_env'):
            await sup.setup_env()

    async def teardown_env(self):
        sup = super()
        if hasattr(sup, 'teardown_env'):
            await sup.teardown_env()


class DatabaseTestCase(ServerTestCase):

    database_class = None

    def new_dbname(self):
        return dbname(self.id().split('.')[-1])

    async def setup_env(self):
        await super().setup_env()
        dbname = self.new_dbname()
        self.url_db = urljoin(self.url, [dbname])
        self.db = self.database_class(
            self.url_db, dbname=dbname, loop=self.loop)
        await self.setup_database(self.db)

    async def setup_database(self, db):
        with self.response(data=b'{"ok": true}'):
            await db.create()

    async def teardown_env(self):
        await self.teardown_database(self.db)
        await super().teardown_env()

    async def teardown_database(self, db):
        with self.response(data=b'{"ok": true}'):
            try:
                await db.delete()
            except aiocouchdb.errors.ResourceNotFound:
                pass


class DocumentTestCase(DatabaseTestCase):

    document_class = None

    async def setup_env(self):
        await super().setup_env()
        docid = uuid()
        self.url_doc = urljoin(self.db.resource.url, [docid])
        self.doc = self.document_class(
            self.url_doc, docid=docid, loop=self.loop)
        await self.setup_document(self.doc)

    async def setup_document(self, doc):
        with self.response(data=b'{"rev": "1-ABC"}'):
            resp = await doc.update({})
        self.rev = resp['rev']


class DesignDocumentTestCase(DatabaseTestCase):

    designdoc_class = None

    async def setup_env(self):
        await super().setup_env()
        docid = '_design/' + uuid()
        self.url_ddoc = urljoin(self.db.resource.url, docid.split('/'))
        self.ddoc = self.designdoc_class(
            self.url_ddoc, docid=docid, loop=self.loop)
        await self.setup_document(self.ddoc)

    async def setup_document(self, ddoc):
        with self.response(data=b'{"rev": "1-ABC"}'):
            resp = await ddoc.doc.update({
                'views': {
                    'viewname': {
                        'map': 'function(doc){ emit(doc._id, null) }'
                    }
                }
            })
        self.rev = resp['rev']


class AttachmentTestCase(DocumentTestCase):

    attachment_class = None

    async def setup_env(self):
        await super().setup_env()
        self.attbin = self.attachment_class(
            urljoin(self.doc.resource.url, ['binary']),
            name='binary')
        self.atttxt = self.attachment_class(
            urljoin(self.doc.resource.url, ['text']),
            name='text')
        self.url_att = self.attbin.resource.url

    async def setup_document(self, doc):
        with self.response(data=b'{"rev": "1-ABC"}'):
            resp = await doc.update({
                '_attachments': {
                    'binary': {
                        'data': base64.b64encode(b'Time to relax!').decode(),
                        'content_type': 'application/octet-stream'
                    },
                    'text': {
                        'data': base64.b64encode(b'Time to relax!').decode(),
                        'content_type': 'text/plain'
                    }
                }
            })
        self.rev = resp['rev']


def modify_server(section, option, value):
    assert section != 'admins', 'use `with_fixed_admin_party` decorator'

    async def apply_config_changes(server, cookie):
        oldval = await server.config.update(section, option, value,
                                                 auth=cookie)
        return oldval

    async def revert_config_changes(server, cookie, oldval):
        if not oldval:
            try:
                await server.config.delete(section, option, auth=cookie)
            except aiocouchdb.errors.ResourceNotFound:
                pass
        else:
            if not (await server.config.exists(section, option)):
                return
            oldval = await server.config.update(section, option, oldval,
                                                     auth=cookie)
            assert oldval == value, ('{} != {}'.format(oldval, value))

    def decorator(f):

        @functools.wraps(f)
        async def wrapper(testcase, **kwargs):
            server, cookie = testcase.server, testcase.cookie
            oldval = await apply_config_changes(server, cookie)
            try:
                await f(testcase, **kwargs)
            finally:
                await revert_config_changes(server, cookie, oldval)
        return wrapper
    return decorator


def with_fixed_admin_party(username, password):

    async def apply_config_changes(server, cookie):
        oldval = await server.config.update('admins', username, password,
                                                 auth=cookie)
        cookie = await server.session.open(username, password)
        return oldval, cookie

    async def revert_config_changes(server, cookie, oldval):
        if not oldval:
            try:
                await server.config.delete('admins', username, auth=cookie)
            except aiocouchdb.errors.ResourceNotFound:
                pass
        else:
            await server.config.update('admins', username, oldval,
                                            auth=cookie)

    def decorator(f):

        @functools.wraps(f)
        async def wrapper(testcase, **kwargs):
            server, cookie = testcase.server, testcase.cookie
            oldval, cookie = await apply_config_changes(server, cookie)
            if cookie is not None:
                kwargs[username] = cookie
            try:
                await f(testcase, **kwargs)
            finally:
                await revert_config_changes(server, cookie, oldval)
        return wrapper
    return decorator


def using_database(dbarg='db'):

    async def create_database(server, cookie):
        db = server[dbname()]
        await db.create(auth=cookie)
        return db

    async def drop_database(db, cookie):
        try:
            await db.delete(auth=cookie)
        except aiocouchdb.errors.ResourceNotFound:
            pass

    def decorator(f):

        @functools.wraps(f)
        async def wrapper(testcase, **kwargs):
            server, cookie = testcase.server, testcase.cookie

            with testcase.response(data=b'{"ok": true}'):
                db = await create_database(server, cookie)

            assert dbarg not in kwargs, \
                'conflict: both {} and {} are referenced as {}'.format(
                    db, kwargs[dbarg], dbarg
                )

            kwargs[dbarg] = db

            try:
                await f(testcase, **kwargs)
            finally:
                with testcase.response(data=b'{"ok": true}'):
                    await drop_database(db, cookie)
        return wrapper
    return decorator


async def populate_database(db, docs_count):

    def generate_docs(count):
        for _ in range(count):
            dt = datetime.datetime.fromtimestamp(
                random.randint(1234567890, 2345678901)
            )
            dta = [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second]
            doc = {
                '_id': uuid(),
                'created_at': dta,
                'num': random.randint(0, 10),
                'type': random.choice(['a', 'b', 'c'])
            }
            yield doc

    if not (await db.exists()):
        await db.create()

    docs = list(generate_docs(docs_count))
    updates = await db.bulk_docs(docs)
    mapping = {doc['_id']: doc for doc in docs}
    if not updates:
        return {}
    for update in updates:
        mapping[update['id']]['_rev'] = update['rev']
    return mapping


def uuid():
    return _uuid.uuid4().hex


def dbname(idx=None, prefix='test/aiocouchdb'):
    if idx:
        return '/'.join((prefix, idx, uuid()))
    else:
        return '/'.join((prefix, uuid()))


def run_for(*targets):
    def decorator(f):
        @functools.wraps(f)
        @unittest.skipIf(TARGET not in targets,
                         'runs only for targets: %s' % ', '.join(targets))
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper
    return decorator


def skip_for(*targets):
    def decorator(f):
        @functools.wraps(f)
        @unittest.skipIf(TARGET in targets,
                         'skips for targets: %s' % ', '.join(targets))
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper
    return decorator

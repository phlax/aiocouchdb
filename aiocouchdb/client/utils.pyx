# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

import urllib.parse

import asyncio

import aiohttp
import yarl

from .request cimport ClientRequest
from .response cimport ClientResponse

from cpython cimport bool


async def request(
        str method,
        str url,
        *,
        bool allow_redirects=True,
        str compress=None,
        connector: aiohttp.BaseConnector = None,
        cookies=None,
        data=None,
        str encoding='utf-8',
        bool expect100=False,
        dict headers=None,
        loop: asyncio.AbstractEventLoop = None,
        int max_redirects=10,
        dict params=None,
        bool read_until_eof=True,
        request_class: type(ClientRequest) = None,
        response_class: type(ClientResponse) = None,
        bool maybe_raise=True,
        version=aiohttp.HttpVersion11):
    """
        :param cookies: Additional :class:`HTTP cookies
                        <http.cookies.SimpleCookie>`
        :param data: Payload data
        :param loop: AsyncIO event loop instance
        :param request_class: HTTP request maker class
        :param response_class: HTTP response processor class
    """
    redirects = 0
    method = method.upper()
    connector = connector or aiohttp.TCPConnector(force_close=True, loop=loop)
    request_class = request_class or ClientRequest
    response_class = response_class or ClientResponse
    while True:
        async with aiohttp.client.ClientSession(request_class=request_class, response_class=response_class) as session:
            resp = await session.request(
                method, url,
                compress=compress,
                cookies=cookies,
                data=data,
                # encoding=encoding,
                expect100=expect100,
                headers=headers,
                params=params)
            if maybe_raise:
                resp.maybe_raise_error()
            return resp


cpdef unicode urljoin(str base, list path):
    """Assemble a URI based on a base, any number of path segments, and query
    string parameters.

    >>> urljoin('http://example.org', '_all_dbs')
    'http://example.org/_all_dbs'

    A trailing slash on the uri base is handled gracefully:

    >>> urljoin('http://example.org/', '_all_dbs')
    'http://example.org/_all_dbs'

    And multiple positional arguments become path parts:

    >>> urljoin('http://example.org/', 'foo', 'bar')
    'http://example.org/foo/bar'

    All slashes within a path part are escaped:

    >>> urljoin('http://example.org/', 'foo/bar')
    'http://example.org/foo%2Fbar'
    >>> urljoin('http://example.org/', 'foo', '/bar/')
    'http://example.org/foo/%2Fbar%2F'

    >>> urljoin('http://example.org/', None) #doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError: argument 2 to map() must support iteration
    """
    base = base.rstrip('/')
    return (
        base
        if not path
        else '/'.join(
                [base]
                + [urllib.parse.quote(s, '')
                   for s in path]))


cpdef tuple extract_credentials(unicode url):
    """Extract authentication (user name and password) credentials from the
    given URL.

    >>> extract_credentials('http://localhost:5984/_config/')
    ('http://localhost:5984/_config/', None)
    >>> extract_credentials('http://joe:secret@localhost:5984/_config/')
    ('http://localhost:5984/_config/', ('joe', 'secret'))
    >>> extract_credentials('http://joe%40example.com:secret@localhost:5984/_config/')
    ('http://localhost:5984/_config/', ('joe@example.com', 'secret'))
    """
    parts = urllib.parse.urlsplit(url)
    netloc = parts[1]
    if '@' in netloc:
        creds, netloc = netloc.split('@')
        credentials = tuple(
            [urllib.parse.unquote(i)
             for i in creds.split(':')])
        parts = list(parts)
        parts[1] = netloc
    else:
        credentials = None
    return urllib.parse.urlunsplit(parts), credentials

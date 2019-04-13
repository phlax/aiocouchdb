# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2016 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiohttp
import aiohttp.http_exceptions
import aiohttp.http_parser
import aiohttp.log

from .response import ClientResponse
from .session import HttpSession
from .utils import extract_credentials, request, urljoin
from .resource import Resource


__all__ = (
    'ClientRequest',
    'ClientResponse',
    'HttpSession',
    'Resource',
    'extract_credentials',
    'urljoin'
)


# FIXME: workaround of decompressing empty payload.
# https://github.com/KeepSafe/aiohttp/pull/154
class HttpPayloadParser(aiohttp.http_parser.HttpPayloadParser):

    async def __call__(self, out, buf):
        # payload params
        length = self.message.headers.get(CONTENT_LENGTH, self.length)
        if SEC_WEBSOCKET_KEY1 in self.message.headers:
            length = 8

        # payload decompression wrapper
        if self.compression and self.message.compression:
            if self.response_with_body:  # the fix
                out = aiohttp.protocol.DeflateBuffer(out,
                                                     self.message.compression)

        # payload parser
        if not self.response_with_body:
            # don't parse payload if it's not expected to be received
            pass

        elif 'chunked' in self.message.headers.get(TRANSFER_ENCODING, ''):
            await self.parse_chunked_payload(out, buf)

        elif length is not None:
            try:
                length = int(length)
            except ValueError:
                raise aiohttp.errors.InvalidHeader(CONTENT_LENGTH) from None

            if length < 0:
                raise aiohttp.errors.InvalidHeader(CONTENT_LENGTH)
            elif length > 0:
                await self.parse_length_payload(out, buf, length)
        else:
            if self.readall and getattr(self.message, 'code', 0) != 204:
                await self.parse_eof_payload(out, buf)
            elif getattr(self.message, 'method', None) in ('PUT', 'POST'):
                aiohttp.log.internal_logger.warning(  # pragma: no cover
                    'Content-Length or Transfer-Encoding header is required')

        out.feed_eof()

aiohttp.HttpPayloadParser = HttpPayloadParser

# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

import io
import types

import rapidjson as json

from aioclient.request cimport ClientRequest as BaseClientRequest
from aiohttp import hdrs, multipart, payload


cdef class ClientRequest(BaseClientRequest):
    DEFAULT_HEADERS = {
        hdrs.ACCEPT: 'application/json',
        hdrs.ACCEPT_ENCODING: 'gzip, deflate',
        hdrs.CONTENT_TYPE: 'application/json'}
    CHUNK_SIZE = 8192

    cpdef update_body_from_data(self, data):
        """Encodes ``data`` as JSON if `Content-Type`
        is :mimetype:`application/json`."""
        if data is None:
            return
        if self.headers.get(hdrs.CONTENT_TYPE) == 'application/json':
            non_json_types = (
                types.GeneratorType,
                io.IOBase,
                multipart.MultipartWriter,
                payload.AsyncIterablePayload)
            if not isinstance(data, non_json_types):
                data = json.dumps(data)
        self._update_body_from_data(data)
        self.chunked = bool(
            hdrs.CONTENT_LENGTH in self.headers
            and isinstance(data, multipart.MultipartWriter))

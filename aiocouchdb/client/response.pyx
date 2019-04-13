# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: binding=True
# cython: language_level=3
# cython: profiling=True

import json

import aiohttp

from aioclient.response cimport ClientResponse as BaseClientResponse

from aiocouchdb.errors import maybe_raise_error


cdef class ClientResponse(BaseClientResponse):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    cpdef maybe_raise_error(self):
        """Raises an :exc:`HttpErrorException` if response status code is
        greater or equal `400`."""
        return maybe_raise_error(self)

    async def json(self, *, encoding='utf-8', loads=None):
        """Reads and decodes JSON response."""
        if self._body is None:
            await self.read()
        if not self._body.strip():
            return None
        return (loads or json.loads)(
            self._body.decode(encoding))

    async def read(self):
        """Read response payload."""
        if self._body is None:
            data = bytearray()
            try:
                while not self.content.at_eof():
                    data.extend(await self.content.read())
            except:
                self.close()
                raise
            else:
                self.close()
            self._body = data
        return self._body


class Py__ClientResponse(ClientResponse):
    pass

# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: binding=True
# cython: language_level=3
# cython: profiling=True

from aioclient.response cimport (
    ClientResponse as BaseClientResponse)


cdef class ClientResponse(BaseClientResponse):
    cpdef maybe_raise_error(self)

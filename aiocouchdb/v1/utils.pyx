# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-

import json


cpdef params_from_locals(_locals, exclude=None):
    params = {}
    params.update({
        key: value
        for key, value in _locals.items()
        if key not in (exclude or []) and value is not None})
    return params


def chunkify(docs, all_or_nothing, new_edits):
    # stream docs one by one to reduce footprint from jsonifying all
    # of them in single shot. useful when docs is generator of docs
    first_chunk = b'{'
    if all_or_nothing is True:
        first_chunk += b'"all_or_nothing": true, '
    if new_edits is False:
        first_chunk += b'"new_edits": false, '
    first_chunk += b'"docs": ['
    yield first_chunk
    idocs = iter(docs)
    yield json.dumps(next(idocs)).encode('utf-8')
    for doc in idocs:
        yield b',' + json.dumps(doc).encode('utf-8')
    yield b']}'

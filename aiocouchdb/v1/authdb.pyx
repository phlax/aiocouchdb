# distutils: define_macros=CYTHON_TRACE_NOGIL=1
# cython: linetrace=True
# cython: binding=True
# cython: language_level=3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
from typing import IO, Union

from aiocouchdb.authn import AuthProvider

from .database cimport Database
from .document cimport Document
from .designdoc cimport DesignDocument


__all__ = (
    'AuthDatabase',
    'UserDocument')


cdef class UserDocument(Document):
    """Represents user document for the :class:`authentication database
    <aiocouchdb.v1.database.AuthDatabase>`."""

    doc_prefix = 'org.couchdb.user:'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self._docid is None:
            raise ValueError('docid must be specified for User documents.')

    def __repr__(self) -> str:
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,
            self.resource.url,
            hex(id(self)))

    @property
    def name(self) -> str:
        """Returns username."""
        return self.id.split(self.doc_prefix, 1)[-1]

    async def register(
            self,
            str password,
            *,
            auth: AuthProvider = None,
            **additional_data) -> dict:
        """Helper method over :meth:`aiocouchdb.v1.document.Document.update`
        to change a user password.
        """
        data = {
            '_id': self.id,
            'name': self.name,
            'password': password,
            'roles': [],
            'type': 'user'}
        data.update(additional_data)
        return await self.update(data, auth=auth)

    async def update_password(
            self,
            str password,
            *,
            auth: AuthProvider = None) -> dict:
        """Helper method over :meth:`aiocouchdb.v1.document.Document.update`
        to change a user password.
        """
        data = await self.get(auth=auth)
        data['password'] = password
        return await self.update(data, auth=auth)


cdef class AuthDatabase(Database):
    """Represents system authentication database.
    Used via :attr:`aiocouchdb.v1.server.Server.authdb`."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.document_class = kwargs.get("document_class", UserDocument)

    def __getitem__(self, docid) -> Union[Document, DesignDocument]:
        DocumentClass = self.document_class
        if docid.startswith('_design/'):
            resource = self.resource(*docid.split('/', 1))
            DocumentClass = self.design_document_class
        else:
            if not docid.startswith(self.document_class.doc_prefix):
                docid = self.document_class.doc_prefix + docid
            resource = self.resource(docid)
        return DocumentClass(resource, docid=docid)

    def __repr__(self):
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.resource.url,
            hex(id(self)))

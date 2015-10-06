# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import sqlparse
from sqlparse import tokens as Token
from sqlparse.sql import Comment


log = logging.getLogger('replication_handler.components.sql_handler')


def mysql_statement_factory(query):
    parsed_query = sqlparse.parse(query)
    assert len(parsed_query) == 1
    statement = parsed_query[0]

    # The order here matters - the first match will be returned.
    # UnsupportedStatement is intentionally at the end, as a catch-all.
    statement_types = [
        CreateTableStatement,
        AlterTableStatement,
        DropTableStatement,
        CreateDatabaseStatement,
        AlterDatabaseStatement,
        DropDatabaseStatement,
        CreateIndexStatement,
        DropIndexStatement,
        RenameTableStatement,
        UnsupportedStatement
    ]

    for statement_type in statement_types:
        try:
            return statement_type(statement)
        except IncompatibleStatementError:
            pass
    raise IncompatibleStatementError()


class IncompatibleStatementError(ValueError):
    pass


class MysqlStatement(object):
    def __init__(self, statement):
        self.statement = statement

        if not self.token_matcher.matches(*self.matchers):
            raise IncompatibleStatementError()

    @property
    def keyword_tokens(self):
        return [
            token for token in self.statement.tokens
            if token.match(Token.Keyword, None) or token.match(Token.Keyword.DDL, None)
        ]

    @property
    def tokens(self):
        return [
            token for token in self.statement.tokens
            if not token.is_whitespace() and not isinstance(token, Comment)
        ]

    @property
    def token_matcher(self):
        return TokenMatcher(self.tokens)


class TokenMatcher(object):
    def __init__(self, tokens):
        self.tokens = tokens
        self.index = 0

    def matches(self, *args):
        return all(self._match(self._listify(match)) for match in args)

    def _match(self, match_vals):
        if isinstance(match_vals, Optional):
            return self._optional_match(match_vals)
        else:
            return self._required_match(match_vals)

    def _optional_match(self, match_vals):
        if not self.has_next():
            return True

        if self._has_next_token_match(match_vals):
            self.pop()

        return True

    def _required_match(self, match_vals):
        if self.has_next() and self._has_next_token_match(match_vals):
            self.pop()
            return True
        else:
            return False

    def _has_next_token_match(self, match_vals):
        normed_value = self.peek().value.upper()
        return any(normed_value == value.upper() for value in match_vals)

    def _listify(self, match):
        if not isinstance(match, list):
            match = [match]
        return match

    def pop(self):
        next_val = self.peek()
        self.index += 1
        return next_val

    def peek(self):
        return self.tokens[self.index]

    def has_next(self):
        return self.index < len(self.tokens)


class Optional(list):
    pass


class TableStatementBase(MysqlStatement):
    def __init__(self, statement):
        self.statement = statement

        if not self.token_matcher.matches(*self.matchers):
            raise IncompatibleStatementError()


class CreateTableStatement(TableStatementBase):
    matchers = [
        'create',
        Optional(['temporary']),
        'table'
    ]


class AlterTableStatement(TableStatementBase):
    matchers = [
        'alter',
        Optional(['online', 'offline']),
        Optional(['ignore']),
        'table'
    ]


class DropTableStatement(TableStatementBase):
    matchers = [
        'drop',
        Optional(['temporary']),
        'table'
    ]


class DatabaseStatementBase(MysqlStatement):
    pass


class CreateDatabaseStatement(DatabaseStatementBase):
    matchers = [
        'create',
        ['database', 'schema']
    ]


class AlterDatabaseStatement(DatabaseStatementBase):
    matchers = [
        'alter',
        ['database', 'schema']
    ]
    first_keyword = 'alter'


class DropDatabaseStatement(DatabaseStatementBase):
    matchers = [
        'drop',
        ['database', 'schema']
    ]


class IndexStatementBase(MysqlStatement):
    pass


class CreateIndexStatement(IndexStatementBase):
    matchers = [
        'create',
        Optional(['online', 'offline']),
        Optional(['unique', 'fulltext', 'spatial']),
        'index'
    ]


class DropIndexStatement(IndexStatementBase):
    matchers = [
        'drop',
        Optional(['online', 'offline']),
        'index',
        Optional(['online', 'offline'])
    ]


class RenameTableStatement(MysqlStatement):
    matchers = [
        'rename',
        'table'
    ]


class UnsupportedStatement(MysqlStatement):
    pass

# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import sqlparse
from sqlparse import tokens as Token
from sqlparse.sql import Comment


log = logging.getLogger('replication_handler.components.sql_handler')


def mysql_statement_factory(query):
    parsed_query = sqlparse.parse(query, dialect='mysql')
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
    ]

    for statement_type in statement_types:
        try:
            return statement_type(statement)
        except IncompatibleStatementError:
            pass
    return UnsupportedStatement(statement)


class IncompatibleStatementError(ValueError):
    pass


class MysqlStatement(object):
    def __init__(self, statement):
        self.statement = statement
        self.token_matcher = TokenMatcher(self.tokens)

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

    def is_supported(self):
        return True


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
        self._required_match(match_vals)
        return True

    def _required_match(self, match_vals):
        for match_val in match_vals:
            if isinstance(match_val, Compound):
                return self._compound_match(match_val)
            elif self.has_next() and self._has_next_token_match([match_val]):
                self.pop()
                return True
        return False

    def _compound_match(self, compound_matcher):
        if (
            self.has_next(len(compound_matcher)) and
            self._has_compound_token_match(compound_matcher)
        ):
            for _ in xrange(len(compound_matcher)):
                self.pop()
            return True
        else:
            return False

    def _has_next_token_match(self, match_vals):
        return self._has_token_match(self.peek(), match_vals)

    def _has_compound_token_match(self, compound_matcher):
        tokens = self.peek(len(compound_matcher))
        for token, match_vals in zip(tokens, compound_matcher):
            match_vals = self._listify(match_vals)
            if not self._has_token_match(token, match_vals):
                return False
        return True

    def _has_token_match(self, token, match_vals):
        if isinstance(match_vals, Any):
            return True
        normed_value = token.value.upper()
        return any(normed_value == value.upper() for value in match_vals)

    def _listify(self, match):
        if not isinstance(match, list):
            match = [match]
        return match

    def pop(self):
        next_val = self.peek()
        self.index += 1
        return next_val

    def peek(self, length=1):
        if length == 1:
            return self.tokens[self.index]
        else:
            return self.tokens[self.index:(self.index + length)]

    def has_next(self, length=1):
        return (self.index + length - 1) < len(self.tokens)

    def get_remaining_tokens(self):
        return self.tokens[self.index:]


class Optional(list):
    pass


class Compound(list):
    pass


class Any(list):
    pass


class TableStatementBase(MysqlStatement):
    @classmethod
    def extract_table_name(cls, token):
        # useful info: https://dev.mysql.com/doc/refman/5.5/en/identifiers.html
        table_name = token.split('.')[-1]
        if (table_name[0] == '"' and table_name[-1] == '"'):
            table_name = cls._remove_identifier(table_name, '"')
        elif (table_name[0] == '`' and table_name[-1] == '`'):
            table_name = cls._remove_identifier(table_name, '`')
        return table_name

    @classmethod
    def _remove_identifier(cls, table_name, identifier):
        return table_name[1:-1].replace(identifier + identifier, identifier)


class CreateTableStatement(TableStatementBase):
    matchers = [
        'create',
        Optional(['temporary']),
        'table'
    ]

    def __init__(self, statement):
        super(CreateTableStatement, self).__init__(statement)
        if (
            self.token_matcher.matches(
                Optional([Compound(['if', 'not', 'exists'])]),
                Optional([Compound([Any(), '.'])])
            ) and
            self.token_matcher.has_next()
        ):
            table = self.token_matcher.pop().value
            self.table = TableStatementBase.extract_table_name(table)
        else:
            raise IncompatibleStatementError()


class AlterTableStatement(TableStatementBase):
    matchers = [
        'alter',
        Optional(['online', 'offline']),
        Optional(['ignore']),
        'table'
    ]

    def __init__(self, statement):
        super(AlterTableStatement, self).__init__(statement)
        if self.token_matcher.has_next():
            self.table = TableStatementBase.extract_table_name(
                self.token_matcher.pop().value
            )
        else:
            raise IncompatibleStatementError()

    def does_rename_table(self):
        return any(
            token.match(Token.Keyword, 'rename')
            for token in self.token_matcher.get_remaining_tokens()
        )


class DropTableStatement(TableStatementBase):
    matchers = [
        'drop',
        Optional(['temporary']),
        'table'
    ]

    def __init__(self, statement):
        super(DropTableStatement, self).__init__(statement)
        if (
            self.token_matcher.matches(Optional([Compound(['if', 'exists'])])) and
            self.token_matcher.has_next()
        ):
            self.table = TableStatementBase.extract_table_name(
                self.token_matcher.pop().value
            )
        else:
            raise IncompatibleStatementError()


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
    matchers = []

    def is_supported(self):
        return False

# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import re

import sqlparse
from sqlparse import tokens as Token
from sqlparse.sql import Comment
from sqlparse.sql import Identifier
from sqlparse.sql import Token as TK


log = logging.getLogger('replication_handler.components.sql_handler')


def mysql_statement_factory(query):
    log.info("Parsing incoming query: {}".format(query))
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


class UnparseableTableNameError(ValueError):
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

    def has_matches(self, *args):
        """Checks if there would be matches, without advancing the index
        of the matcher.
        """
        current_index = self.index
        has_matches = self.matches(*args)
        self.index = current_index
        return has_matches

    def _match(self, match_vals):
        if isinstance(match_vals, Optional):
            return self._optional_match(match_vals)
        else:
            return self._required_match(match_vals)

    def _optional_match(self, match_vals):
        self._required_match(match_vals)
        return True

    def _required_match(self, match_vals):
        if isinstance(match_vals, Compound):
            return self._compound_match(match_vals)

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
        # We need to handle three cases here where the next_val could be:
        # 1. <table_name> ('business')
        # 2. <database_name>.<table_name> ('yelp.business')
        # 3. <database_name>.<table_name> <extended_query>
        # ('yelp.business change col_one col_two')
        # In all the cases we should return a token consisting of only the table
        # name or if the database name is present then the database name and the
        # table name. Case #3 occurs because SQLParse incorrectly parses certain
        # queries.
        if isinstance(next_val, Identifier):
            tokens = next_val.tokens
            if len(tokens) > 1 and tokens[1].value == '.':
                str_token = "{db_name}{punctuation}{table_name}".format(
                    db_name=tokens[0].value,
                    punctuation=tokens[1].value,
                    table_name=tokens[2].value
                )
                return TK(Token.Name, str_token)
            else:
                return next_val.token_first()
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


class ParseError(Exception):
    pass


class MysqlQualifiedIdentifierParser(object):
    def __init__(self, identifier, identifier_qualified=True):
        self.index = 0
        self.identifier = self._clean_identifier(identifier)
        self.identifier_qualified = identifier_qualified

    def _clean_identifier(self, identifier):
        identifier = identifier.strip()

        # This is a workaround for DATAPIPE-588
        # TODO(DATAPIPE-490|justinc): We'll probably need to replace SQLParse
        # to get rid of this.
        # https://regex101.com/r/qC3tB7/6
        match = re.match('^(.+?)(\s+engine$|\s+ROW_FORMAT|\s+AUTO_INCREMENT)+', identifier, re.I)
        if match:
            identifier = match.group(1)

        return identifier

    def parse(self):
        if self.identifier_qualified:
            identifiers = self._handle_qualified_identifier()
        else:
            identifiers = self._handle_identifier()

        if self.index != len(self.identifier):
            log.error(
                "ParseError: {} failed to parse.  Qualified: {}. "
                "Identifiers: {}".format(
                    self.identifier,
                    self.identifier_qualified,
                    identifiers,
                )
            )
            raise ParseError()

        return identifiers

    def _handle_qualified_identifier(self):
        identifiers = []
        identifiers.append(self._handle_identifier())
        while self._peek() == '.':
            self._pop()
            identifiers.append(self._handle_identifier())
        return identifiers

    def _handle_identifier(self):
        if self._peek() in ['`', '"']:
            return self._handle_quoted_identifier(self._pop())

        return self._handle_unquoted_identifier()

    def _handle_quoted_identifier(self, quote_char):
        start_index = self.index

        while not (self._peek() == quote_char and self._peek(2) != (quote_char * 2)):
            if self._peek(2) == (quote_char * 2):
                self._pop()
                self._pop()
            else:
                self._pop()

        identifier = self.identifier[start_index:self.index]
        identifier = identifier.replace(quote_char * 2, quote_char)
        self._pop()

        return identifier

    def _handle_unquoted_identifier(self):
        start_index = self.index
        # This matches all the allowed unquoted identifier chars - see
        # https://dev.mysql.com/doc/refman/5.6/en/identifiers.html
        while re.match("[0-9a-zA-Z\$_\u0080-\uFFFF]", self._peek(), re.UNICODE) is not None:
            self._pop()

        return self.identifier[start_index:self.index]

    def _pop(self):
        next_char = self._peek()
        self.index += 1
        return next_char

    def _peek(self, length=1):
        return self.identifier[self.index:(self.index + length)]


class TableStatementBase(MysqlStatement):
    @classmethod
    def extract_db_and_table_name(cls, token):
        identifiers = MysqlQualifiedIdentifierParser(token).parse()
        if len(identifiers) == 2:
            database_name = identifiers[0]
            table_name = identifiers[1]
        elif len(identifiers) == 1:
            database_name = None
            table_name = identifiers[0]
        else:
            raise UnparseableTableNameError()

        return database_name, table_name

    def set_db_and_table_name(self):
        log.info("Extract db and table (token: {}) from statement: {}".format(
            self.token_matcher.peek().value,
            str(self.statement)
        ))
        self.database_name, self.table = TableStatementBase.extract_db_and_table_name(
            self.token_matcher.pop().value
        )


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
            ) and
            self.token_matcher.has_next()
        ):
            self.database_name = None
            if self.token_matcher.has_matches(Compound([Any(), '.', Any()])):
                db = self.token_matcher.pop().value
                self.token_matcher.pop()
                self.database_name = MysqlQualifiedIdentifierParser(
                    db,
                    identifier_qualified=False
                ).parse()

            self.table = MysqlQualifiedIdentifierParser(
                self.token_matcher.pop().value,
                identifier_qualified=False
            ).parse()
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
            self.set_db_and_table_name()
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
            self.set_db_and_table_name()
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

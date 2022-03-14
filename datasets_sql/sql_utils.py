# File copied from: https://github.com/andialbrecht/sqlparse/blob/master/examples/extract_table_names.py

# !/usr/bin/env python
#
# Copyright (C) 2009-2020 the sqlparse authors and contributors
# <see AUTHORS file>
#
# This example is part of python-sqlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause
#
# This example illustrates how to extract table names from nested
# SELECT statements.
#
# See:
# https://groups.google.com/forum/#!forum/sqlparse/browse_thread/thread/b0bd9a022e9d4895

import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import DML, Keyword


def _is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == "SELECT":
            return True
    return False


def _extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if _is_subselect(item):
                yield from _extract_from_part(item)
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == "FROM":
            from_seen = True


def _extract_table_identifiers(token_stream):
    for item in token_stream:
        print(item)
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value


def extract_tables_from_sql(sql):
    stream = _extract_from_part(sqlparse.parse(sql)[0])
    return list(_extract_table_identifiers(stream))


# Additional utilities


def is_select_sql(sql):
    item = sqlparse.parse(sql)[0].token_first()
    return item.ttype is DML and item.value.upper() == "SELECT"

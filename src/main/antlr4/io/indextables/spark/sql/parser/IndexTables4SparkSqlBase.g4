/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar IndexTables4SparkSqlBase;

@members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

tokens {
    DELIMITER
}

singleStatement
    : statement ';'* EOF
    ;

statement
    : MERGE SPLITS (path=STRING | table=qualifiedName)?
        (WHERE whereClause=predicateToken)?
        (TARGET SIZE targetSize=alphanumericValue)?
        (MAX GROUPS maxGroups=alphanumericValue)?
        PRECOMMIT?                                              #mergeSplitsTable
    | PURGE INDEXTABLE (path=STRING | table=qualifiedName)
        (OLDER THAN retentionNumber=INTEGER_VALUE retentionUnit=(DAYS | HOURS))?
        (TRANSACTION LOG RETENTION txLogRetentionNumber=INTEGER_VALUE txLogRetentionUnit=(DAYS | HOURS))?
        (DRY RUN)?                                              #purgeIndexTable
    | REPAIR INDEXFILES TRANSACTION LOG sourcePath=STRING
        AT LOCATION targetPath=STRING                           #repairIndexFilesTransactionLog
    | FLUSH indexTablesKeyword SEARCHER CACHE                       #flushIndexTablesCache
    | INVALIDATE indexTablesKeyword TRANSACTION LOG CACHE
        (FOR (path=STRING | table=qualifiedName))?             #invalidateIndexTablesTransactionLogCache
    | .*?                                                       #passThrough
    ;

indexTablesKeyword
    : TANTIVY4SPARK | INDEXTABLES
    ;

alphanumericValue
    : IDENTIFIER | INTEGER_VALUE | STRING
    ;

predicateToken
    : .*?
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

nonReserved
    : CACHE | SEARCHER | TANTIVY4SPARK | INDEXTABLES | INDEXTABLE | FOR | TRANSACTION | LOG | MAX | GROUPS
    | REPAIR | INDEXFILES | AT | LOCATION | PURGE | OLDER | THAN | DAYS | HOURS | DRY | RUN
    | RETENTION
    ;

// Keywords (case-insensitive)
MERGE: [Mm][Ee][Rr][Gg][Ee];
SPLITS: [Ss][Pp][Ll][Ii][Tt][Ss];
WHERE: [Ww][Hh][Ee][Rr][Ee];
TARGET: [Tt][Aa][Rr][Gg][Ee][Tt];
SIZE: [Ss][Ii][Zz][Ee];
PRECOMMIT: [Pp][Rr][Ee][Cc][Oo][Mm][Mm][Ii][Tt];
FLUSH: [Ff][Ll][Uu][Ss][Hh];
INVALIDATE: [Ii][Nn][Vv][Aa][Ll][Ii][Dd][Aa][Tt][Ee];
TRANSACTION: [Tt][Rr][Aa][Nn][Ss][Aa][Cc][Tt][Ii][Oo][Nn];
LOG: [Ll][Oo][Gg];
FOR: [Ff][Oo][Rr];
TANTIVY4SPARK: [Tt][Aa][Nn][Tt][Ii][Vv][Yy]'4'[Ss][Pp][Aa][Rr][Kk];
INDEXTABLES: [Ii][Nn][Dd][Ee][Xx][Tt][Aa][Bb][Ll][Ee][Ss];
INDEXTABLE: [Ii][Nn][Dd][Ee][Xx][Tt][Aa][Bb][Ll][Ee];
SEARCHER: [Ss][Ee][Aa][Rr][Cc][Hh][Ee][Rr];
CACHE: [Cc][Aa][Cc][Hh][Ee];
MAX: [Mm][Aa][Xx];
GROUPS: [Gg][Rr][Oo][Uu][Pp][Ss];
REPAIR: [Rr][Ee][Pp][Aa][Ii][Rr];
INDEXFILES: [Ii][Nn][Dd][Ee][Xx][Ff][Ii][Ll][Ee][Ss];
AT: [Aa][Tt];
LOCATION: [Ll][Oo][Cc][Aa][Tt][Ii][Oo][Nn];
PURGE: [Pp][Uu][Rr][Gg][Ee];
OLDER: [Oo][Ll][Dd][Ee][Rr];
THAN: [Tt][Hh][Aa][Nn];
DAYS: [Dd][Aa][Yy][Ss];
HOURS: [Hh][Oo][Uu][Rr][Ss];
DRY: [Dd][Rr][Yy];
RUN: [Rr][Uu][Nn];
RETENTION: [Rr][Ee][Tt][Ee][Nn][Tt][Ii][Oo][Nn];

// Literals
STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

SIMPLE_COMMENT
    : '--' .*? '\r'? '\n' -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all
UNRECOGNIZED
    : .
    ;
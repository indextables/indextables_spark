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

grammar Tantivy4SparkSqlBase;

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
    : MERGE SPLITS (path=STRING | table=qualifiedName)
        (WHERE whereClause=predicateToken)?
        (TARGET SIZE targetSize=sizeValue)?
        PRECOMMIT?                                              #mergeSplitsTable
    | FLUSH TANTIVY4SPARK SEARCHER CACHE                        #flushTantivyCache
    | .*?                                                       #passThrough
    ;

sizeValue
    : '-'? INTEGER_VALUE sizeSuffix?
    ;

sizeSuffix
    : 'M' | 'G'
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
    : CACHE | SEARCHER | TANTIVY4SPARK
    ;

// Keywords (case-insensitive)
MERGE: [Mm][Ee][Rr][Gg][Ee];
SPLITS: [Ss][Pp][Ll][Ii][Tt][Ss];
WHERE: [Ww][Hh][Ee][Rr][Ee];
TARGET: [Tt][Aa][Rr][Gg][Ee][Tt];
SIZE: [Ss][Ii][Zz][Ee];
PRECOMMIT: [Pp][Rr][Ee][Cc][Oo][Mm][Mm][Ii][Tt];
FLUSH: [Ff][Ll][Uu][Ss][Hh];
TANTIVY4SPARK: [Tt][Aa][Nn][Tt][Ii][Vv][Yy]'4'[Ss][Pp][Aa][Rr][Kk];
SEARCHER: [Ss][Ee][Aa][Rr][Cc][Hh][Ee][Rr];
CACHE: [Cc][Aa][Cc][Hh][Ee];

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
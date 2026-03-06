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
        (MAX DEST SPLITS maxDestSplits=alphanumericValue)?
        (MAX SOURCE SPLITS PER MERGE maxSourceSplitsPerMerge=alphanumericValue)?
        PRECOMMIT?                                              #mergeSplitsTable
    | PURGE INDEXTABLE (path=STRING | table=qualifiedName)
        (OLDER THAN retentionNumber=INTEGER_VALUE retentionUnit=(DAYS | HOURS))?
        (TRANSACTION LOG RETENTION txLogRetentionNumber=INTEGER_VALUE txLogRetentionUnit=(DAYS | HOURS))?
        (DRY RUN)?                                              #purgeIndexTable
    | DROP indexTablesKeyword PARTITIONS FROM (path=STRING | table=qualifiedName)
        WHERE whereClause=predicateToken                        #dropPartitions
    | REPAIR INDEXFILES TRANSACTION LOG sourcePath=STRING
        AT LOCATION targetPath=STRING                           #repairIndexFilesTransactionLog
    | FLUSH indexTablesKeyword SEARCHER CACHE                       #flushIndexTablesCache
    | FLUSH indexTablesKeyword DISK CACHE                          #flushDiskCache
    | INVALIDATE indexTablesKeyword TRANSACTION LOG CACHE
        (FOR (path=STRING | table=qualifiedName))?             #invalidateIndexTablesTransactionLogCache
    | DESCRIBE indexTablesKeyword TRANSACTION LOG (path=STRING | table=qualifiedName)
        (INCLUDE ALL)?                                          #describeTransactionLog
    | DESCRIBE indexTablesKeyword DISK CACHE                    #describeDiskCache
    | DESCRIBE indexTablesKeyword STORAGE STATS                 #describeStorageStats
    | DESCRIBE indexTablesKeyword ENVIRONMENT                   #describeEnvironment
    | DESCRIBE indexTablesKeyword STATE (path=STRING | table=qualifiedName)   #describeState
    | DESCRIBE indexTablesKeyword COMPONENT SIZES (path=STRING | table=qualifiedName)
        (WHERE whereClause=predicateToken)?                                 #describeComponentSizes
    | PREWARM indexTablesKeyword CACHE (path=STRING | table=qualifiedName)
        (FOR SEGMENTS '(' segmentList=identifierList ')')?
        (ON FIELDS '(' fieldList=identifierList ')')?
        (WITH PERWORKER PARALLELISM OF parallelism=INTEGER_VALUE)?
        (WHERE whereClause=predicateToken)?
        (ASYNC MODE)?                                           #prewarmCache
    | DESCRIBE indexTablesKeyword PREWARM JOBS                  #describePrewarmJobs
    | DESCRIBE indexTablesKeyword MERGE JOBS                    #describeMergeJobs
    | WAIT FOR indexTablesKeyword PREWARM JOBS
        (path=STRING | table=qualifiedName)?
        (JOB jobId=STRING)?
        (TIMEOUT timeoutSeconds=INTEGER_VALUE)?                 #waitForPrewarmJobs
    | (CHECKPOINT | COMPACT) indexTablesKeyword (path=STRING | table=qualifiedName)   #checkpointIndexTable
    | TRUNCATE indexTablesKeyword TIME TRAVEL
        (path=STRING | table=qualifiedName)
        (DRY RUN)?                                              #truncateTimeTravel
    | BUILD indexTablesKeyword COMPANION FOR
        sourceFormat=(DELTA | PARQUET | ICEBERG) sourcePath=STRING
        (SCHEMA SOURCE schemaSourcePath=STRING)?
        (CATALOG catalogName=STRING (TYPE catalogType=STRING)?)?
        (WAREHOUSE warehouse=STRING)?
        (INDEXING MODES '(' indexingModeList ')')?
        (FASTFIELDS MODE fastFieldMode=(HYBRID | DISABLED | PARQUET_ONLY))?
        (HASHED FASTFIELDS hashedFastfieldsMode=(INCLUDE | EXCLUDE) '(' hashedFastfieldsList=stringList ')')?
        (TARGET INPUT SIZE targetInputSize=alphanumericValue)?
        (WRITER HEAP SIZE writerHeapSize=alphanumericValue)?
        (FROM VERSION fromVersion=INTEGER_VALUE)?
        (FROM SNAPSHOT fromSnapshot=INTEGER_VALUE)?
        (WHERE whereClause=predicateToken)?
        (INVALIDATE ALL PARTITIONS)?
        AT LOCATION destPath=STRING
        (DRY RUN)?                                            #syncToExternal
    | .*?                                                       #passThrough
    ;

indexTablesKeyword
    : TANTIVY4SPARK | INDEXTABLES
    ;

alphanumericValue
    : IDENTIFIER | INTEGER_VALUE | STRING
    ;

identifierList
    : identifier (',' identifier)*
    ;

indexingModeList
    : indexingModeEntry (',' indexingModeEntry)*
    ;

indexingModeEntry
    : fieldName=STRING ':' fieldMode=STRING
    ;

stringList
    : STRING (',' STRING)*
    ;

// Non-greedy match for WHERE predicate text. ANTLR's .*? stops at the first
// token that allows the enclosing rule to continue (e.g., AT LOCATION, DRY RUN).
// This is safe because the subsequent keywords are multi-token sequences that
// won't appear in normal SQL predicate expressions.
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
    | RETENTION | DESCRIBE | INCLUDE | ALL | DROP | PARTITIONS | FROM | DISK | WITH
    | PREWARM | SEGMENTS | FIELDS | PERWORKER | PARALLELISM | OF | ON | STORAGE | STATS
    | DEST | SOURCE | PER | ENVIRONMENT | CHECKPOINT | COMPACT | TRUNCATE | TIME | TRAVEL | STATE
    | ASYNC | MODE | JOBS | JOB | WAIT | TIMEOUT | COMPONENT | SIZES
    | BUILD | COMPANION | DELTA | PARQUET | ICEBERG | INDEXING | MODES | FASTFIELDS | HYBRID | DISABLED | PARQUET_ONLY | INPUT | VERSION | WRITER | HEAP
    | SCHEMA | CATALOG | SNAPSHOT | TYPE | WAREHOUSE | HASHED | EXCLUDE | INVALIDATE
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
DESCRIBE: [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee];
INCLUDE: [Ii][Nn][Cc][Ll][Uu][Dd][Ee];
ALL: [Aa][Ll][Ll];
DROP: [Dd][Rr][Oo][Pp];
PARTITIONS: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn][Ss];
FROM: [Ff][Rr][Oo][Mm];
DISK: [Dd][Ii][Ss][Kk];
WITH: [Ww][Ii][Tt][Hh];
PREWARM: [Pp][Rr][Ee][Ww][Aa][Rr][Mm];
SEGMENTS: [Ss][Ee][Gg][Mm][Ee][Nn][Tt][Ss];
FIELDS: [Ff][Ii][Ee][Ll][Dd][Ss];
PERWORKER: [Pp][Ee][Rr][Ww][Oo][Rr][Kk][Ee][Rr];
PARALLELISM: [Pp][Aa][Rr][Aa][Ll][Ll][Ee][Ll][Ii][Ss][Mm];
OF: [Oo][Ff];
ON: [Oo][Nn];
STORAGE: [Ss][Tt][Oo][Rr][Aa][Gg][Ee];
STATS: [Ss][Tt][Aa][Tt][Ss];
DEST: [Dd][Ee][Ss][Tt];
SOURCE: [Ss][Oo][Uu][Rr][Cc][Ee];
PER: [Pp][Ee][Rr];
ENVIRONMENT: [Ee][Nn][Vv][Ii][Rr][Oo][Nn][Mm][Ee][Nn][Tt];
CHECKPOINT: [Cc][Hh][Ee][Cc][Kk][Pp][Oo][Ii][Nn][Tt];
COMPACT: [Cc][Oo][Mm][Pp][Aa][Cc][Tt];
TRUNCATE: [Tt][Rr][Uu][Nn][Cc][Aa][Tt][Ee];
TIME: [Tt][Ii][Mm][Ee];
TRAVEL: [Tt][Rr][Aa][Vv][Ee][Ll];
STATE: [Ss][Tt][Aa][Tt][Ee];
ASYNC: [Aa][Ss][Yy][Nn][Cc];
MODE: [Mm][Oo][Dd][Ee];
JOBS: [Jj][Oo][Bb][Ss];
JOB: [Jj][Oo][Bb];
WAIT: [Ww][Aa][Ii][Tt];
TIMEOUT: [Tt][Ii][Mm][Ee][Oo][Uu][Tt];
COMPONENT: [Cc][Oo][Mm][Pp][Oo][Nn][Ee][Nn][Tt];
SIZES: [Ss][Ii][Zz][Ee][Ss];
BUILD: [Bb][Uu][Ii][Ll][Dd];
COMPANION: [Cc][Oo][Mm][Pp][Aa][Nn][Ii][Oo][Nn];
DELTA: [Dd][Ee][Ll][Tt][Aa];
INDEXING: [Ii][Nn][Dd][Ee][Xx][Ii][Nn][Gg];
MODES: [Mm][Oo][Dd][Ee][Ss];
FASTFIELDS: [Ff][Aa][Ss][Tt][Ff][Ii][Ee][Ll][Dd][Ss];
HYBRID: [Hh][Yy][Bb][Rr][Ii][Dd];
DISABLED: [Dd][Ii][Ss][Aa][Bb][Ll][Ee][Dd];
PARQUET_ONLY: [Pp][Aa][Rr][Qq][Uu][Ee][Tt]'_'[Oo][Nn][Ll][Yy];
INPUT: [Ii][Nn][Pp][Uu][Tt];
VERSION: [Vv][Ee][Rr][Ss][Ii][Oo][Nn];
WRITER: [Ww][Rr][Ii][Tt][Ee][Rr];
HEAP: [Hh][Ee][Aa][Pp];
PARQUET: [Pp][Aa][Rr][Qq][Uu][Ee][Tt];
ICEBERG: [Ii][Cc][Ee][Bb][Ee][Rr][Gg];
SCHEMA: [Ss][Cc][Hh][Ee][Mm][Aa];
CATALOG: [Cc][Aa][Tt][Aa][Ll][Oo][Gg];
SNAPSHOT: [Ss][Nn][Aa][Pp][Ss][Hh][Oo][Tt];
TYPE: [Tt][Yy][Pp][Ee];
WAREHOUSE: [Ww][Aa][Rr][Ee][Hh][Oo][Uu][Ss][Ee];
HASHED: [Hh][Aa][Ss][Hh][Ee][Dd];
EXCLUDE: [Ee][Xx][Cc][Ll][Uu][Dd][Ee];

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
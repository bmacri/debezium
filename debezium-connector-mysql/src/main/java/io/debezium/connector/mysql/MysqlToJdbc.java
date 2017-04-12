/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Types;

public class MysqlToJdbc {

    public static int mysqlToJdbcType(int mysqlType) {
        // See https://github.com/percona/percona-server/blob/1e2f003a5bd48763c27e37542d97cd8f59d98eaa/libbinlogevents/export/binary_log_types.h#L38
        // See https://github.com/debezium/debezium/blob/v0.3.6/debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/MySqlDdlParser.java#L75
        /*
        MYSQL_TYPE_DECIMAL, // 0
        MYSQL_TYPE_TINY, // 1
        MYSQL_TYPE_SHORT, // 2
        MYSQL_TYPE_LONG, // 3
        MYSQL_TYPE_FLOAT, // 4
        MYSQL_TYPE_DOUBLE, // 5
        MYSQL_TYPE_NULL, // 6
        MYSQL_TYPE_TIMESTAMP, // 7
        MYSQL_TYPE_LONGLONG, // 8
        MYSQL_TYPE_INT24, // 9
        MYSQL_TYPE_DATE, // 10
        MYSQL_TYPE_TIME, // 11
        MYSQL_TYPE_DATETIME, // 12
        MYSQL_TYPE_YEAR, // 13
        MYSQL_TYPE_NEWDATE, // 14
        MYSQL_TYPE_VARCHAR, // 15
        MYSQL_TYPE_BIT, // 16
        MYSQL_TYPE_TIMESTAMP2, // 17
        MYSQL_TYPE_DATETIME2, // 18
        MYSQL_TYPE_TIME2, // 19
        MYSQL_TYPE_JSON=245,
        MYSQL_TYPE_NEWDECIMAL=246,
        MYSQL_TYPE_ENUM=247,
        MYSQL_TYPE_SET=248,
        MYSQL_TYPE_TINY_BLOB=249,
        MYSQL_TYPE_MEDIUM_BLOB=250,
        MYSQL_TYPE_LONG_BLOB=251,
        MYSQL_TYPE_BLOB=252,
        MYSQL_TYPE_VAR_STRING=253,
        MYSQL_TYPE_STRING=254,
        MYSQL_TYPE_GEOMETRY=255
        */
        switch (mysqlType) {
            case 0:   return Types.DECIMAL;
            case 1:   return Types.SMALLINT;
            case 2:   return Types.SMALLINT;
            case 3:   return Types.BIGINT;
            case 4:   return Types.FLOAT;
            case 5:   return Types.DOUBLE;
            case 6:   return Types.NULL;
            case 7:   return Types.TIMESTAMP;
            case 8:   return Types.BIGINT;
            case 9:   return Types.INTEGER;
            case 10:  return Types.DATE;
            case 11:  return Types.TIME;
            case 12:  return Types.TIMESTAMP;
            case 13:  return Types.INTEGER;
            case 14:  return Types.DATE;
            case 15:  return Types.VARCHAR;
            case 16:  return Types.BIT;
            case 17:  return Types.TIMESTAMP;
            case 18:  return Types.TIMESTAMP;
            case 19:  return Types.TIME;
            case 245: return Types.OTHER;
            case 246: return Types.DECIMAL;
            case 247: return Types.CHAR;
            case 248: return Types.CHAR;
            case 249: return Types.BLOB;
            case 250: return Types.BLOB;
            case 251: return Types.BLOB;
            case 252: return Types.BLOB;
            case 253: return Types.VARCHAR;
            case 254: return Types.CHAR;
            case 255: return Types.OTHER;
            default: return Types.OTHER;
        }
    }
}

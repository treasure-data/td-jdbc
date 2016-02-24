/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.jdbc;

import java.sql.SQLException;
import java.sql.Types;

public class Utils
{

    /**
     * Convert hive types to sql types.
     *
     * @param t
     * @return Integer java.sql.Types values
     * @throws SQLException
     */
    public static int TDTypeToSqlType(String t)
            throws SQLException
    {
        String type = t.toLowerCase();
        if ("string".equals(type)) {
            return Types.VARCHAR;
        }
        else if ("varchar".equals(type) || type.startsWith("varchar(")) {
            return Types.VARCHAR;
        }
        else if ("float".equals(type)) {
            return Types.FLOAT;
        }
        else if ("double".equals(type)) {
            return Types.DOUBLE;
        }
        else if ("boolean".equals(type)) {
            return Types.BOOLEAN;
        }
        else if ("tinyint".equals(type)) {
            return Types.TINYINT;
        }
        else if ("smallint".equals(type)) {
            return Types.SMALLINT;
        }
        else if ("int".equals(type)) {
            return Types.INTEGER;
        }
        else if ("long".equals(type)) {
            return Types.BIGINT;
        }
        else if ("bigint".equals(type)) {
            return Types.BIGINT;
        }
        else if ("date".equals(type)) {
            return Types.DATE;
        }
        else if ("timestamp".equals(type)) {
            return Types.TIMESTAMP;
        }
        else if (type.startsWith("map<")) {
            return Types.VARCHAR;
        }
        else if (type.startsWith("array<")) {
            return Types.VARCHAR;
        }
        else if (type.startsWith("struct<")) {
            return Types.VARCHAR;
        }
        throw new SQLException("Unrecognized column type: " + type);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flink.sql.sink.postgres;


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.ExtendOutputFormat;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Reason: postgreSQL Sink
 * Date: 2018/12/11
 * Company: www.infinivision.io
 *
 * @author Hongtao
 */


// TODO: only support 9.5+ which support ON CONFLICT Clause
    /*
    insert into pg_sink ("id", "name", "email", "phone_num", "update_time", "create_time")
    values (5,'Uutiixq','13758960326@infinivision.com','13758960326',1483200000,1483200000)
    ON CONFLICT(id, name)
    DO UPDATE SET phone_num='123456';
     */

public class PostgresSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    private static final String POSTGRES_DRIVER = "org.postgresql.Driver";

    public PostgresSink() {
    }

    @Override
    public RetractJDBCOutputFormat getOutputFormat() {
        return new ExtendOutputFormat();
    }

    @Override
    public void buildSql(String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        return null;
    }

    private void buildInsertSql(String tableName, List<String> fields) {
        String sqlTmp = "insert into " + tableName + " (${fields}) values (${placeholder})";
        String fieldsStr = StringUtils.join(fields, ",");
        String placeholder = "";

        for (String fieldName : fields) {
            placeholder += ",?";
        }
        placeholder = placeholder.replaceFirst(",", "");
        sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);
        this.sql = sqlTmp;
    }


    @Override
    public String getDriverName() {
        return POSTGRES_DRIVER;
    }


}

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

import com.treasure_data.model.Job;

import java.sql.SQLException;

public class ConfigBuilder
{
    private String url;
    private String database = "default";
    private String user;
    private String password;
    private Job.Type type = Job.Type.PRESTO; // Use presto by default
    private boolean useApiKey;
    private ApiConfig apiConfig;
    private int resultRetryCountThreshold = Config.TD_JDBC_RESULT_RETRYCOUNT_THRESHOLD_DEFAULTVALUE;
    private long resultRetryWaitTimeMs = Config.TD_JDBC_RESULT_RETRY_WAITTIME_DEFAULTVALUE;

    public ConfigBuilder() {}

    public ConfigBuilder(Config config) {
        this.url = config.url;
        this.database = config.database;
        this.user = config.user;
        this.password = config.password;
        this.useApiKey = config.useApiKey;
        this.type = config.type;
        this.apiConfig = config.apiConfig;
        this.resultRetryCountThreshold = config.resultRetryCountThreshold;
        this.resultRetryWaitTimeMs = config.resultRetryWaitTimeMs;
    }

    public ConfigBuilder setUrl(String url)
    {
        this.url = url;
         return this;
    }

    public ConfigBuilder setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    public ConfigBuilder setUser(String user)
    {
        this.user = user;
        return this;
    }

    public ConfigBuilder setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public ConfigBuilder setType(Job.Type type)
            throws SQLException
    {
        if (type == null || !(type.equals(Job.Type.HIVE) || type.equals(Job.Type.PRESTO))) {
            throw new SQLException("invalid job type within URL: " + type);
        }
        this.type = type;
        return this;
    }

    public void setUseApiKey(boolean useApiKey) {
        this.useApiKey = useApiKey;
    }

    public ConfigBuilder setApiConfig(ApiConfig apiConfig) {
        this.apiConfig = apiConfig;
        return this;
    }

    public ConfigBuilder setResultRetryCountThreshold(int resultRetryCountThreshold) {
        this.resultRetryCountThreshold = resultRetryCountThreshold;
        return this;
    }

    public ConfigBuilder setResultRetryWaitTimeMs(long resultRetryWaitTimeMs) {
        this.resultRetryWaitTimeMs = resultRetryWaitTimeMs;
        return this;
    }

    public Config createConnectionConfig() throws SQLException
    {
        return new Config(url,
                database,
                user,
                password,
                type,
                useApiKey,
                apiConfig != null ? apiConfig : new ApiConfig.ApiConfigBuilder().createApiConfig(),
                resultRetryCountThreshold,
                resultRetryWaitTimeMs
                );
    }
}

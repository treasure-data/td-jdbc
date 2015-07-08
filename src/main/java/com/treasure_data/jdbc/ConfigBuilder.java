package com.treasure_data.jdbc;

import com.treasure_data.model.Job;

import java.sql.SQLException;

public class ConfigBuilder
{
    private String url;
    private String database = "default";
    private String user;
    private String password;
    private Job.Type type = Job.Type.PRESTO; // Use presto by default
    private ApiConfig apiConfig;
    private int resultRetryCountThreshold = Config.TD_JDBC_RESULT_RETRYCOUNT_THRESHOLD_DEFAULTVALUE;
    private long resultRetryWaitTimeMs = Config.TD_JDBC_RESULT_RETRY_WAITTIME_DEFAULTVALUE;

    public ConfigBuilder() {}

    public ConfigBuilder(Config config) {
        this.url = config.url;
        this.database = config.database;
        this.user = config.user;
        this.password = config.password;
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

    public ConfigBuilder setApiConfig(ApiConfig apiConfig) {
        this.apiConfig =apiConfig;
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
                apiConfig != null ? apiConfig : new ApiConfig.ApiConfigBuilder().createApiConfig(),
                resultRetryCountThreshold,
                resultRetryWaitTimeMs
                );
    }
}
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

import java.util.Properties;

/**
 * TD API configuration
 */
public class ApiConfig
{
    public final String scheme; // http or https
    public final String endpoint;
    public final int port;
    public final boolean useSSL;
    public final Option<String> apiKey;
    public final Option<ProxyConfig> proxy;

    public ApiConfig(String endpoint, Option<Integer> port, boolean useSSL, Option<String> apiKey, Option<ProxyConfig> proxy)
    {
        this.scheme = useSSL ? "https://" : "http://";
        this.endpoint = endpoint == null? "api.treasuredata.com" : endpoint;
        this.port = port.getOrElse(useSSL ? 443 : 80);
        this.useSSL = useSSL;
        this.apiKey = apiKey;
        this.proxy = proxy;
    }

    public Properties toProperties() {
        Properties prop = new Properties();
        prop.setProperty(Config.TD_CK_API_SERVER_SCHEME, scheme);
        prop.setProperty(Config.TD_JDBC_USESSL, Boolean.toString(useSSL));
        prop.setProperty(Config.TD_API_SERVER_HOST, endpoint);
        prop.setProperty(Config.TD_API_SERVER_PORT, Integer.toString(port));

        if(apiKey.isDefined()) {
            prop.setProperty(Config.TD_API_KEY, apiKey.get());
        }

        if(proxy.isDefined()) {
            Properties proxyProp = proxy.get().toProperties();
            prop.putAll(proxyProp);
        }
        return prop;
    }

    public static class ApiConfigBuilder {
        public String endpoint;
        public Option<Integer> port = Option.empty();
        public boolean useSSL = false;
        public Option<String> apiKey = Option.empty();
        public Option<ProxyConfig> proxy = Option.empty();

        public ApiConfigBuilder() {}

        public ApiConfigBuilder(ApiConfig config) {
            this.endpoint = config.endpoint;
            this.port = Option.of(config.port);
            this.useSSL = config.useSSL;
            this.proxy = config.proxy;
        }

        public ApiConfigBuilder setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public ApiConfigBuilder setPort(int port) {
            this.port = Option.of(port);
            return this;
        }

        public ApiConfigBuilder setUseSSL(boolean useSSL) {
            this.useSSL = useSSL;
            return this;
        }

        public ApiConfigBuilder setApiKey(String apiKey) {
            this.apiKey = Option.of(apiKey);
            return this;
        }

        public ApiConfigBuilder unsetApiKey() {
            this.apiKey = Option.empty();
            return this;
        }

        public ApiConfigBuilder setProxyConfig(ProxyConfig proxyConfig) {
            this.proxy = Option.of(proxyConfig);
            return this;
        }

        public ApiConfig createApiConfig() {
            return new ApiConfig(endpoint, port, useSSL, apiKey, proxy);
        }
    }
}

package com.treasure_data.jdbc;

import java.util.Properties;

/**
 * Proxy configuration to access TD API
 */
public class ProxyConfig
{
    public final String host;
    public final int port;
    public final String user;
    public final String password;

    public ProxyConfig(String host, int port, String user, String password)
    {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public void apply() {
        System.setProperty("http.proxyHost", host);
        System.setProperty("https.proxyHost", host);
        String portStr = Integer.toString(port);
        System.setProperty("http.proxyPort", portStr);
        System.setProperty("https.proxyPort", portStr);
        System.setProperty("http.proxyUser", user);
        System.setProperty("http.proxyPassword", password);
    }

    public Properties toProperties()
    {
        Properties prop = new Properties();
        prop.setProperty("http.proxyHost", host);
        prop.setProperty("https.proxyHost", host);

        String portStr = Integer.toString(port);
        prop.setProperty("http.proxyPort", portStr);
        prop.setProperty("https.proxyPort", portStr);

        prop.setProperty("http.proxyUser", user);
        prop.setProperty("http.proxyPassword", password);
        return prop;
    }

    public static class ProxyConfigBuilder
    {
        private String host;
        private int port;
        private String user;
        private String password;

        public ProxyConfigBuilder() {}

        public ProxyConfigBuilder(ProxyConfig config) {
            this.host = config.host;
            this.port = config.port;
            this.user = config.user;
            this.password = config.password;
        }

        public ProxyConfigBuilder setHost(String host)
        {
            this.host = host;
            return this;
        }

        public ProxyConfigBuilder setPort(int port)
        {
            this.port = port;
            return this;
        }

        public ProxyConfigBuilder setUser(String user)
        {
            this.user = user;
            return this;
        }

        public ProxyConfigBuilder setPassword(String password)
        {
            this.password = password;
            return this;
        }

        public ProxyConfig createProxyConfig()
        {
            return new ProxyConfig(host, port, user, password);
        }
    }
}

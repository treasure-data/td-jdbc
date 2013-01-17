package com.treasure_data.jdbc;

import java.sql.SQLException;

//
// jdbc:td://api.treasure-data.com:80/testdb;k1=v1;k2=v2
// +-------+ +-------------------+ ++ +----+ +---------+
// type      host               port  path   parameters
//           +----------------------+
//           endpoint
//
public class JDBCURLParser {

    static class Desc {
        String url = null;
        String host = Constants.TD_JDBC_HOST_DEFAULT;
        String port = Constants.TD_JDBC_PORT_DEFAULT;
        String database = null;
        String user = null;
        String password = null;

        public Desc() {
        }
    }

    public static Desc parse(String url) throws SQLException {
        Utils.validateJDBCType(url);

        Desc d = new Desc();

        d.url = url;

        int postUrlPos = url.length();
        // postUrlPos is the END position in url String,                                                                                                                                                                                                        
        // wrt what remains to be processed.                                                                                                                                                                                                                    
        // i.e., if postUrlPos is 100, url no longer needs to examined at                                                                                                                                                                                       
        // index 100 or later.                                                                                                                                                                                                                                  
        int semiPos = url.indexOf(';', Constants.URL_PREFIX.length());
        if (semiPos > -1) {
            String params = url.substring(semiPos + 1, postUrlPos);
            String[] kvs = params.split(";");
            for (String param : kvs) {
                String[] kv = param.split("=");
                if (kv == null || kv.length != 2) {
                    throw new SQLException("invalid parameters within URL: " + url);
                }

                String k = kv[0];
                if (k.equals(Config.TD_JDBC_USER)) {
                    d.user = kv[1];
                } else if (k.equals(Config.TD_JDBC_PASSWORD)) {
                    d.password = kv[1];
                }
            }
        } else {
            semiPos = postUrlPos;
        }

        int slashPos = url.indexOf('/', Constants.URL_PREFIX.length());
        if (slashPos >= 0) {
            try {
                String rawDatabase = url.substring(slashPos + 1, semiPos);
                if (rawDatabase != null && !rawDatabase.isEmpty()) {
                    d.database = rawDatabase;
                }
            } catch (Throwable t) {
                throw new SQLException("invalid database within URL: " + url);
            }
        }
        if (d.database == null) {
            throw new SQLException("Not found database within URL: " + url);
        }

        String endpoint = null;
        try {
            endpoint = url.substring(Constants.URL_PREFIX.length(), slashPos);
        } catch (Throwable t) {
            throw new SQLException("invalid endpoint within URL: " + url);
        }
        if (endpoint != null && !endpoint.isEmpty()) {
            int i = endpoint.indexOf(':');
            if (i >= 0) {
                if (i != 0) {
                    d.host = endpoint.substring(0, i);
                }
                d.port = endpoint.substring(i + 1, endpoint.length());
                try {
                    Integer.parseInt(d.port);
                } catch (Throwable t) {
                    throw new SQLException("invalid port within URL: " + url);
                }
            } else { // i < 0
                d.host = endpoint;
            }
        }
        
        return d;
    }

}

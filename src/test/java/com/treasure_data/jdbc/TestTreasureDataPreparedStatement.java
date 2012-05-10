package com.treasure_data.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.packer.Packer;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.jdbc.TDConnection;

public class TestTreasureDataPreparedStatement {

    @Test @Ignore
    public void testSimple() throws Exception {
        Properties props = new Properties();
        TDConnection conn =
            new TDConnection("jdbc:td://localhost:9999/mugadb", props);
        PreparedStatement ps = conn.prepareStatement("select v['uid'] as uid from mugatbl;");
        ResultSet rs = ps.executeQuery();
        System.out.println("rs: " + rs);
        ResultSetMetaData metaData = rs.getMetaData();
        int count = metaData.getColumnCount();
        System.out.println("count: " + count);
        for (int i = 0; i < count; i++) {
            System.out.println(metaData.getColumnName(i + 1));
            System.out.println(metaData.getColumnTypeName(i + 1));
        }
    }

    @Test @Ignore
    public void testSimple02() throws Exception {
        Properties props = new Properties();
        TDConnection conn =
            new TDConnection("jdbc:td://localhost:9999/mugadb", props);
        PreparedStatement ps = conn.prepareStatement(
                "select v['id'] as id, v['name'] as name, v['score'] as score from score order by score desc;");

        ResultSet rs = ps.executeQuery();

        System.out.println("## metadata ##");
        ResultSetMetaData metaData = rs.getMetaData();
        int count = metaData.getColumnCount();
        for (int i = 0; i < count; i++) {
            System.out.println(String.format("%d: %s: %s",
                    i,
                    metaData.getColumnName(i + 1),
                    metaData.getColumnTypeName(i + 1)));
        }

        System.out.println("## data ##");
        while (rs.next()) {
            System.out.println(String.format("%s %s %s",
                    rs.getString(1), rs.getString(2), rs.getString(3))); 
        }
    }
}

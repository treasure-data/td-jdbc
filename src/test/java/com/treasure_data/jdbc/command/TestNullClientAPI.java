package com.treasure_data.jdbc.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestNullClientAPI {

    @Test
    public void testDrop() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        assertTrue(clientApi.drop("foo"));
    }

    @Test
    public void testCreate() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        assertTrue(clientApi.create("foo"));
    }

    @Test
    public void testInsert() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        Map<String, Object> record = new HashMap<String, Object>();
        assertTrue(clientApi.insert("foo", record));
    }

    @Test
    public void testFlush() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        assertTrue(clientApi.flush());
    }

    @Test
    public void testSelect() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        assertEquals(null, clientApi.select("sql"));
    }

}

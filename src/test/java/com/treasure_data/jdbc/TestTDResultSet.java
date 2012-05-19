package com.treasure_data.jdbc;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.JobSummary.Status;

public class TestTDResultSet {

    @Test
    public void testSample() throws Exception {
        ClientAPI clientApi = new ClientAPI() {
            public boolean drop(String tableName) throws ClientException {
                return false;
            }

            public boolean create(String table) throws ClientException {
                return false;
            }

            public boolean insert(String tableName, Map<String, Object> record) throws ClientException {
                return false;
            }

            public ResultSet select(String sql) throws ClientException {
                return null;
            }

            public boolean flush() {
                return false;
            }

            public JobSummary waitJobResult(Job job) throws ClientException {
                // hive_result_schema to json
                //1.9.2-p290 :005 > names.zip(types)
                //=> [["age", "int"], ["name", "string"]]
                List<List<String>> resultSchema0 = new ArrayList<List<String>>();
                List<String> col1 = new ArrayList<String>();
                col1.add("age");
                col1.add("int");
                resultSchema0.add(col1);
                List<String> col2 = new ArrayList<String>();
                col2.add("name");
                col2.add("string");
                resultSchema0.add(col2);
                String resultSchema = JSONValue.toJSONString(resultSchema0);
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job) throws ClientException {
                List<List<Object>> result = new ArrayList<List<Object>>();
                List<Object> ret0 = new ArrayList<Object>();
                ret0.add(10);
                ret0.add("muga");
                result.add(ret0);
                List<Object> ret1 = new ArrayList<Object>();
                ret1.add(20);
                ret1.add("nishizawa");
                result.add(ret1);

                try {
                    MessagePack msgpack = new MessagePack();
                    BufferPacker packer = msgpack.createBufferPacker();
                    packer.write(result);
                    byte[] bytes = packer.toByteArray();
                    return msgpack.createBufferUnpacker(bytes);
                } catch (java.io.IOException e) {
                    throw new ClientException("mock");
                }
            }
        };
        Job job = new Job("12345");
        ResultSet rs = new TDResultSet(clientApi, 50, job);
        while (rs.next()) {
            System.out.println(rs.getObject(1));
            System.out.println(rs.getObject(2));
        }
    }
}

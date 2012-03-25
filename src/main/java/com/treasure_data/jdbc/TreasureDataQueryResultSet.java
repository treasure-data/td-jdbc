package com.treasure_data.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.jdbc.HiveResultSetMetaData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.BytesWritable;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;

public class TreasureDataQueryResultSet extends TreasureDataBaseResultSet {
    private static Logger LOG = Logger.getLogger(
            TreasureDataQueryResultSet.class.getName());

    private TreasureDataClient client;

    private SerDe serde;

    private int maxRows = 0;

    private int rowsFetched = 0;

    private int fetchSize = 50;

    private List<String> fetchedRows;

    private Iterator<String> fetchedRowsItr;

    private Job job;

    public TreasureDataQueryResultSet(TreasureDataClient client, int maxRows, Job job)
            throws SQLException {
        this.client = client;
        this.maxRows = maxRows;
        this.job = job;
        initSerDe();
        row = Arrays.asList(new Object[columnNames.size()]);
    }

    public TreasureDataQueryResultSet(TreasureDataClient client, Job job)
            throws SQLException {
        this(client, 0, job);
    }

    /**
     * Instantiate the serde used to deserialize the result rows.
     */
    private void initSerDe() throws SQLException {
        // TODO #MN
        try {
            Schema fullSchema = null;
            //Schema fullSchema = client.getSchema(); // TODO
            List<FieldSchema> schema = fullSchema.getFieldSchemas();
            columnNames = new ArrayList<String>();
            columnTypes = new ArrayList<String>();
            StringBuilder namesSb = new StringBuilder();
            StringBuilder typesSb = new StringBuilder();

            if ((schema != null) && (!schema.isEmpty())) {
                for (int pos = 0; pos < schema.size(); pos++) {
                    if (pos != 0) {
                        namesSb.append(",");
                        typesSb.append(",");
                    }
                    columnNames.add(schema.get(pos).getName());
                    columnTypes.add(schema.get(pos).getType());
                    namesSb.append(schema.get(pos).getName());
                    typesSb.append(schema.get(pos).getType());
                }
            }
            String names = namesSb.toString();
            String types = typesSb.toString();

            serde = new LazySimpleSerDe();
            Properties props = new Properties();
            if (names.length() > 0) {
                LOG.fine("Column names: " + names);
                props.setProperty(Constants.LIST_COLUMNS, names);
            }
            if (types.length() > 0) {
                LOG.fine("Column types: " + types);
                props.setProperty(Constants.LIST_COLUMN_TYPES, types);
            }
            serde.initialize(new Configuration(), props);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Could not create ResultSet: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        client = null;
    }

    /**
     * Moves the cursor down one row from its current position.
     *
     * @see java.sql.ResultSet#next()
     * @throws SQLException     if a database access error occurs.
     */
    public boolean next() throws SQLException {
        if (maxRows > 0 && rowsFetched >= maxRows) {
            return false;
        }

        try {
            if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
                fetchedRows = getJobResult(fetchSize);
                fetchedRowsItr = fetchedRows.iterator();
            }

            String rowStr = "";
            if (fetchedRowsItr.hasNext()) {
                rowStr = fetchedRowsItr.next();
            } else {
                return false;
            }

            rowsFetched++;
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Fetched row string: " + rowStr);
            }
            /* TODO
            StructObjectInspector soi =
                (StructObjectInspector) serde.getObjectInspector();
            List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
            Object data = serde.deserialize(new BytesWritable(rowStr.getBytes()));

            assert row.size() == fieldRefs.size() :
                String.format("%d, %d", row.size(), fieldRefs.size());

            for (int i = 0; i < fieldRefs.size(); i++) {
                StructField fieldRef = fieldRefs.get(i);
                ObjectInspector oi = fieldRef.getFieldObjectInspector();
                Object obj = soi.getStructFieldData(data, fieldRef);
                row.set(i, convertLazyToJava(obj, oi));
            }

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("Deserialized row: %s", row.toString()));
            }
            */
        } catch (ClientException e) {
            throw new SQLException("Error retriving next row", e);
        } catch (Exception e) {
            throw new SQLException("Error retrieving next row", e);
        }
        // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
        return true;
    }

    private List<String> getJobResult(int fetchSize) throws ClientException {
        {
            while (true) {
                ShowJobRequest request = new ShowJobRequest(job);
                ShowJobResult result = client.showJob(request);
                System.out.println(result.getJob().getStatus());
                if (result.getJob().getStatus() == Job.Status.SUCCESS) {
                    break;
                }

                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        org.msgpack.type.ArrayValue arrayValue = null; 
        {
            GetJobResultRequest request = new GetJobResultRequest(new JobResult(job));
            GetJobResultResult result = client.getJobResult(request);
            arrayValue = (org.msgpack.type.ArrayValue) result.getJobResult().getResult();
        }

        List<String> ret = new ArrayList<String>();
        Iterator<org.msgpack.type.Value> iter = arrayValue.iterator();

        // TODO #MN
        while (iter.hasNext()) {
            org.msgpack.type.Value v = iter.next();
            ret.add("" + v.asIntegerValue().intValue());
        }

        return ret;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    /**
     * Convert a LazyObject to a standard Java object in compliance with JDBC 3.0 (see JDBC 3.0
     * Specification, Table B-3: Mapping from JDBC Types to Java Object Types).
     *
     * This method is kept consistent with {@link HiveResultSetMetaData#hiveTypeToSqlType}.
     */
    private static Object convertLazyToJava(Object o, ObjectInspector oi) {
        Object obj = ObjectInspectorUtils.copyToStandardObject(o, oi, ObjectInspectorCopyOption.JAVA);

        // for now, expose non-primitive as a string
        // TODO: expose non-primitive as a structured object while maintaining JDBC compliance
        if (obj != null && oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            obj = obj.toString();
        }

        return obj;
    }

}

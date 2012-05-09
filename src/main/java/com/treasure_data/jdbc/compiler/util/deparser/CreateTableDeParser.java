package com.treasure_data.jdbc.compiler.util.deparser;

import java.util.Iterator;

import com.treasure_data.jdbc.compiler.stat.ColumnDefinition;
import com.treasure_data.jdbc.compiler.stat.CreateTable;
import com.treasure_data.jdbc.compiler.stat.Index;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) a {@link com.treasure_data.jdbc.compiler.stat.CreateTable}
 */
public class CreateTableDeParser {
    protected StringBuilder buffer;

    /**
     * @param buffer
     *            the buffer that will be filled with the select
     */
    public CreateTableDeParser(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public void deParse(CreateTable createTable) {
        buffer.append("CREATE TABLE "
                + createTable.getTable().getWholeTableName());
        if (createTable.getColumnDefinitions() != null) {
            buffer.append(" { ");
            for (Iterator<ColumnDefinition> iter = createTable
                    .getColumnDefinitions().iterator(); iter.hasNext();) {
                ColumnDefinition columnDefinition = (ColumnDefinition) iter
                        .next();
                buffer.append(columnDefinition.getColumnName());
                buffer.append(" ");
                buffer.append(columnDefinition.getColDataType().getDataType());
                if (columnDefinition.getColDataType().getArgumentsStringList() != null) {
                    for (Iterator<String> iterator = columnDefinition
                            .getColDataType().getArgumentsStringList()
                            .iterator(); iterator.hasNext();) {
                        buffer.append(" ");
                        buffer.append((String) iterator.next());
                    }
                }
                if (columnDefinition.getColumnSpecStrings() != null) {
                    for (Iterator<String> iterator = columnDefinition
                            .getColumnSpecStrings().iterator(); iterator
                            .hasNext();) {
                        buffer.append(" ");
                        buffer.append((String) iterator.next());
                    }
                }

                if (iter.hasNext())
                    buffer.append(",\n");

            }

            for (Iterator<Index> iter = createTable.getIndexes().iterator(); iter
                    .hasNext();) {
                buffer.append(",\n");
                Index index = (Index) iter.next();
                buffer.append(index.getType() + " " + index.getName());
                buffer.append("(");
                for (Iterator<String> iterator = index.getColumnsNames()
                        .iterator(); iterator.hasNext();) {
                    buffer.append((String) iterator.next());
                    if (iterator.hasNext()) {
                        buffer.append(", ");
                    }
                }
                buffer.append(")");

                if (iter.hasNext())
                    buffer.append(",\n");
            }

            buffer.append(" \n} ");
        }
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

}

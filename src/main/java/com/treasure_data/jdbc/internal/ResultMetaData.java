package com.treasure_data.jdbc.internal;

public class ResultMetaData {
    public static final int RESULT_METADATA          = 1;

    public static final int SIMPLE_RESULT_METADATA   = 2;

    public static final int UPDATE_RESULT_METADATA   = 3;

    public static final int PARAM_METADATA           = 4;

    public static final int GENERATED_INDEX_METADATA = 5;

    public static final int GENERATED_NAME_METADATA  = 6;

    public static ResultMetaData newGeneratedColumnsMetaData(
            int[] columnIndexes, String[] columnNames) {

        if (columnIndexes != null) {
            ResultMetaData md = new ResultMetaData(GENERATED_INDEX_METADATA);

            md.columnCount         = columnIndexes.length;
            md.extendedColumnCount = columnIndexes.length;
            md.colIndexes          = new int[columnIndexes.length];

            for (int i = 0; i < columnIndexes.length; i++) {
                md.colIndexes[i] = columnIndexes[i] - 1;
            }

            return md;
        } else if (columnNames != null) {
            ResultMetaData md = new ResultMetaData(GENERATED_NAME_METADATA);

            md.columnLabels        = new String[columnNames.length];
            md.columnCount         = columnNames.length;
            md.extendedColumnCount = columnNames.length;
            md.columnLabels        = columnNames;

            return md;
        } else {
            return null;
        }
    }

    private int type;

    private int columnCount;

    private int extendedColumnCount;

    private int[] colIndexes;

    private String[] columnLabels;

    private ResultMetaData(int type) {
        this.type = type;
    }
}

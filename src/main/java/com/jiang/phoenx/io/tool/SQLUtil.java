package com.jiang.phoenx.io.tool;

import java.util.Map;

/**
 * @Author : jt
 * @Description :
 * @Date : Create in 10:39 2019/10/23
 */
public class SQLUtil {

    private static final String TABLE_NAME = "${tableName}";
    private static final String COLUMNS = "${columns}";
    private static final String PREPARE = "${prepare}";
    private static final String METADATA = "${metaData}";
    private static final String PRIMARY_KEY = "${primaryKey}";

    public static final String COUNT_COLUMN_NAME = "num";

    public static final String PHOENIX_TYPE_VARCHAR = "VARCHAR";

    public static String phoenixUpsert(String tableName, Map<String, String> metaData){
        String sqlTmp = "UPSERT INTO ${tableName} (${columns}) VALUES (${prepare})";
        StringBuilder columnBuilder = new StringBuilder();
        StringBuilder prepareBuilder = new StringBuilder();
        metaData.forEach((k, v) ->{
            columnBuilder.append(k).append(",");
            prepareBuilder.append("?").append(",");
        });
        columnBuilder.deleteCharAt(columnBuilder.length() - 1);
        prepareBuilder.deleteCharAt(prepareBuilder.length() - 1);
        sqlTmp = sqlTmp.replace(COLUMNS, columnBuilder)
                .replace(PREPARE, prepareBuilder)
                .replace(TABLE_NAME, tableName);
        return sqlTmp;
    }


    public static String phoenixCreate(String tableName, Map<String, String> metaData, String primaryKey){
        String sqlTmp = "CREATE TABLE IF NOT EXISTS ${tableName} (${metaData} CONSTRAINT PK PRIMARY KEY (${primaryKey}))";
        StringBuilder metaDataBuilder = new StringBuilder();
        metaData.forEach((k, v) ->{
            metaDataBuilder.append(k).append(" ").append(v).append(",");
        });
        if(!metaData.containsKey(primaryKey)){
            metaDataBuilder.append(primaryKey).append(" ").append(PHOENIX_TYPE_VARCHAR).append(",");
            metaData.put(primaryKey, PHOENIX_TYPE_VARCHAR);
        }
        metaDataBuilder.deleteCharAt(metaDataBuilder.length() - 1);
        sqlTmp = sqlTmp.replace(TABLE_NAME, tableName)
                .replace(METADATA, metaDataBuilder)
                .replace(PRIMARY_KEY, primaryKey);
        return sqlTmp;
    }


    public static String phoenixCount(String sql){
        return "select count(1) num from ( " + sql + ")";
    }

    /**
     * 最好不要用分页,phoenix分页查询极慢
     * @param sql
     * @return
     */
    public static String phoenixLimit(String sql){
        return "select * from ( " + sql + ") LIMIT ? OFFSET ?";
    }

    private SQLUtil(){}

}

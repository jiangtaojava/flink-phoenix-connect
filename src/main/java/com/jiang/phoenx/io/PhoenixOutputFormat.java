package com.jiang.phoenx.io;


import com.jiang.phoenx.io.domain.PhoenixParamObj;
import com.jiang.phoenx.io.framework.ConnectionUtil;
import com.jiang.phoenx.io.tool.SQLUtil;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

/**
 * @Author : jt
 * @Description : phoenix jdbc持久化,以后可以改成关系型数据库的持久化类
 *          分为需要创建表和不创建表
 *          不创建表按一般操作
 *          创建表分指定主键列半酣与输出数据内和不包含输出数据内
 *          包含在输出数据内只需建表
 *          不包含在数据数据内需要新增列,并且在写操作的时候为主键列填充值
 * @Date : Create in 16:38 2019/10/18
 */
public class PhoenixOutputFormat implements OutputFormat<Row> {
    private String url;
    private String query;
    private Connection connection;
    private PreparedStatement statement;

    private Boolean createPKValue;

    private Integer num = 0;

    public void configure(Configuration configuration) {

    }

    public void open(int i, int i1) throws IOException {

        try {
            connection = ConnectionUtil.getPhoenixConnection(url);
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(query);
        } catch (SQLException e) {
            throw new IOException(e);
        }

    }

    public void writeRecord(Row row) throws IOException {
        int arity = row.getArity();
        //如果需要创建主键值,则在原有数据长度上+1
        if(createPKValue){
            arity++;
        }
        try {
            for (int i = 1; i <= arity; i++) {
                //如果需要填充主键值
                if(createPKValue && i == arity){
                    statement.setObject(i, UUID.randomUUID().toString().replace("-", ""));
                } else {
                    statement.setObject(i, row.getField(i - 1));
                }
            }
            statement.addBatch();
            num++;
        } catch (SQLException e) {
            throw new IOException(e);
        }
        if (num > 1000) {
            flush();
        }
    }

    private void flush() {
        try {
            statement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Execution of JDBC statement failed.", e);
        } finally {
            num = 0;
        }

    }

    public void close() throws IOException {
        try {
            flush();
            if (!statement.isClosed()) {
                statement.close();
            }
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            try {
                if (!statement.isClosed()) {
                    statement.close();
                }
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }


    public static PhoenixOutputFormat buildPhoenixOutputFormat(PhoenixParamObj phoenixParamObj, Map<String, String> outMetaData) {
        boolean createValue = false;
        String url = phoenixParamObj.getObjectiveURL();
        if(phoenixParamObj.isCreateTable()){
            createValue = !outMetaData.containsKey(phoenixParamObj.getPrimaryKey());
            //这里如果设置的主键不包含在结果列中会创建新列,并且把新列put到metadata最后一个
            String createSQL = SQLUtil.phoenixCreate(phoenixParamObj.getObjectiveTable(), outMetaData, phoenixParamObj.getPrimaryKey());
            try (Connection connection = ConnectionUtil.getPhoenixConnection(url);
                 PreparedStatement statement = connection.prepareStatement(createSQL)) {
                statement.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        String upsertSQL = SQLUtil.phoenixUpsert(phoenixParamObj.getObjectiveTable(), outMetaData);
        return new PhoenixOutputFormat(url, upsertSQL, createValue);
    }


    private PhoenixOutputFormat(String url, String query, Boolean createPrimaryKeyValue) {
        this.url = url;
        this.query = query;
        this.createPKValue = createPrimaryKeyValue;
    }
}

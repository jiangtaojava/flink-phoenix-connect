package com.jiang.phoenx.io;


import com.jiang.phoenx.io.domain.SourceParamObj;
import com.jiang.phoenx.io.framework.ConnectionUtil;
import com.jiang.phoenx.io.splits.PaginationSplits;
import com.jiang.phoenx.io.tool.SQLUtil;
import com.jiang.phoenx.io.type.TypeInfoFormat;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author : jt
 * @Description : phoenix jdbc读取方法,以后可以改成关系型数据库的持久化类,对JDBCInputFormat的本地化优化
 *          这个类获取数据会开启N+1次连接,N为工作节点数
 *          类初始化的时候会调用getProducedType开启一次连接,获取查询结果的metadata
 *      这次是在本地开启,为了解决flink自带的jdbc查询每次使用都要传入结果数据的metadata的麻烦事,所以不用在本地缓存连接
 *          以后N次为方法open在工作节点上开启,需要缓存在本地,多次读取数据
 * @Date : Create in 8:48 2019/10/22
 */
public class PhoenixInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

    private String url;

    private String query;

    private Connection connection;

    private PreparedStatement statement;

    private ResultSet resultSet;

    private Boolean hasNext;

    private Map<String, String> metaDataMap = new LinkedHashMap<>();

    private Integer dataNum;

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public Row nextRecord(Row row) throws IOException {
        try {
            if (this.hasNext) {
                for (int i = 0; i < row.getArity(); ++i) {
                    row.setField(i, this.resultSet.getObject(i + 1));
                }
                return row;
            }
            return null;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            try {
                if (!resultSet.isClosed()) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            try {
                if (statement != null && !statement.isClosed()) {
                    statement.close();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    /**
     * 初始化就会调用该方法,先把连接建起拿到MetaData再说
     * 按原有设计查询器要手动传入查询元数据内容
     * 查询前拿查询的结果元数据有点麻烦
     * @return
     */
    @Override
    public RowTypeInfo getProducedType() {
        String countSQL = SQLUtil.phoenixCount(query);
        try (Connection connection = ConnectionUtil.getPhoenixConnection(url);
             PreparedStatement statement = connection.prepareStatement(query);
             PreparedStatement count = connection.prepareStatement(countSQL);
             ResultSet countNum = count.executeQuery();
             ResultSet resultSet = statement.executeQuery()){
            //读取数据条数,如果需要并行需要这个数据计算每个并行读取的条数
            countNum.next();
            this.dataNum = countNum.getInt(SQLUtil.COUNT_COLUMN_NAME);

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            String[] columnNames = new String[columnCount];
            BasicTypeInfo[] typeInfos = new BasicTypeInfo[columnCount];
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                columnNames[i] = columnName;
                String columnType = metaData.getColumnTypeName(i + 1);
                BasicTypeInfo basicTypeInfo = TypeInfoFormat.translated(columnType);
                typeInfos[i] = basicTypeInfo;
                //字符类型统一为VARCHAR,主要是CHAR建表的时候必须设置长度不好弄
                if(BasicTypeInfo.STRING_TYPE_INFO.equals(basicTypeInfo)){
                    columnType = SQLUtil.PHOENIX_TYPE_VARCHAR;
                }
                metaDataMap.put(columnName, columnType);
            }

            return new RowTypeInfo(typeInfos, columnNames);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 这一步才是在计算节点上打开连接,每个计算节点执行一次
     * 如果拆分器个数 > 1 将查询数据平均分配到各个节点分页查询
     *         PaginationSplits split = (PaginationSplits)inputSplit;
     *         //拆分器个数
     *         totalNumberOfSplits = split.getTotalNumberOfSplits();
     *         //获取当前拆分器记录的当前页数据量
     *         int pageSize = split.getPageSize();
     *         //获取当前拆分器记录的当前页查询偏移量
     *         int startNumber = split.getStartNumber();
     * @param inputSplit
     * @throws IOException
     */
    @Override
    public void open(InputSplit inputSplit) throws IOException {
        PaginationSplits split = (PaginationSplits)inputSplit;
        try {
            if(split.isPagination()){
                query = SQLUtil.phoenixLimit(query);
            }
            connection = ConnectionUtil.getPhoenixConnection(url);
            statement = connection.prepareStatement(query);
            if(split.isPagination()){
                //替换为分页查询语句,phoenix不支持两层分页,这里有可能出现异常
                statement.setInt(1, split.getPageSize());
                statement.setInt(2, split.getStartNumber());
            }
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public static PhoenixInputFormat buildPhoenixInputFormat(SourceParamObj paramObj) {
        return new PhoenixInputFormat(paramObj);
    }


    private PhoenixInputFormat(SourceParamObj paramObj) {
        this.url = paramObj.getSourceURL();
        this.query = paramObj.getSourceQuery();
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    /**
     *  这里如果需要设置平行度 > 1需要进行分页
     *  splitsNum 平行度
     *  根据平行度创建拆分器
     * @param splitsNum
     * @return
     * @throws IOException
     */
    @Override
    public InputSplit[] createInputSplits(int splitsNum) throws IOException {
        final InputSplit[] splits = new PaginationSplits[splitsNum];
        for(int i = 0 ; i < splitsNum ; i++){
            splits[i] = new PaginationSplits(i, splitsNum, dataNum);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }


    @Override
    public boolean reachedEnd() throws IOException {
        try {
            hasNext = resultSet.next();
        } catch (SQLException e) {
            hasNext = false;
            throw new IOException(e);
        }
        return !this.hasNext;
    }


    public Map<String, String> getQueryMetaData() {
        return metaDataMap;
    }

}

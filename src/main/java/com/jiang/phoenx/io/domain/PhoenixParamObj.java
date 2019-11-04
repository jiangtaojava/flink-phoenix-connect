package com.jiang.phoenx.io.domain;

/**
 * @Author : jt
 * @Description :
 * @Date : Create in 9:10 2019/10/23
 */
public class PhoenixParamObj extends SourceParamObj{


    /**
     * 结果数据写入
     */
    private String objectiveURL;
    /**
     * 写入表
     */
    private String objectiveTable;

    /**
     * 是否建表
     */
    private Boolean isCreateTable;

    /**
     * 建表主键名称,如果结果列里没有会新建此列并填充UUID
     */
    private String primaryKey;


    public String getObjectiveURL() {
        return objectiveURL;
    }

    public void setObjectiveURL(String objectiveURL) {
        this.objectiveURL = objectiveURL;
    }

    public String getObjectiveTable() {
        return objectiveTable;
    }

    public void setObjectiveTable(String objectiveTable) {
        this.objectiveTable = objectiveTable;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey.toUpperCase();
    }

    public Boolean isCreateTable() {
        return isCreateTable;
    }

    public void setIsCreateTable(Boolean isCreateTable) {
        this.isCreateTable = isCreateTable;
    }
}

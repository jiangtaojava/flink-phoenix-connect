package com.jiang.phoenx.io.domain;


/**
 * @Author : jt
 * @Description :
 * @Date : Create in 10:48 2019/10/25
 */
public class SourceParamObj {

    /**
     * 任务名称
     */
    private String jobName;

    /**
     * 数据源
     */
    private String sourceURL;
    /**
     * 数据源查询语句
     */
    private String sourceQuery;

    /**
     * 目标存储平台
     */
    private TargetEnum target;

    /**
     * 平行度
     */
    private Integer parallelism = 1;

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public TargetEnum getTarget() {
        return target;
    }

    public void setTarget(TargetEnum target) {
        this.target = target;
    }

    public String getSourceURL() {
        return sourceURL;
    }

    public void setSourceURL(String sourceURL) {
        this.sourceURL = sourceURL;
    }

    public String getSourceQuery() {
        return sourceQuery;
    }

    public void setSourceQuery(String sourceQuery) {
        this.sourceQuery = sourceQuery;
    }

    public enum TargetEnum {
        ES,PHOENIX
    }
}

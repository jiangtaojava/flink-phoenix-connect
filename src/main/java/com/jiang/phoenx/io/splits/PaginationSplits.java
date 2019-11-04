package com.jiang.phoenx.io.splits;

import org.apache.flink.core.io.InputSplit;

/**
 * @Author : jt
 * @Description : 分页拆分器
 * @Date : Create in 9:40 2019/11/4
 */
public class PaginationSplits implements InputSplit {

    private Integer pageNumber;

    private Integer totalNumberOfPartitions;

    private Integer dataNumber;

    @Override
    public int getSplitNumber() {
        return pageNumber;
    }

    public Integer getPageNumber(){
        return this.pageNumber;
    }

    public Integer getDataNumber(){
        return this.dataNumber;
    }

    public Integer getTotalNumberOfSplits(){
        return totalNumberOfPartitions;
    }

    public Boolean isPagination(){
        return totalNumberOfPartitions > 1;
    }

    /**
     * 获取查询偏移量
     * 起始点为 分页数 * 单页数据量
     * @return
     */
    public Integer getStartNumber(){
        return pageNumber * dataNumber / totalNumberOfPartitions;
    }

    /**
     * 获取当前页的数据量
     * 如果是最后一页,把余下除不尽的数据加上
     * @return
     */
    public Integer getPageSize(){
        Integer pageSize = dataNumber / totalNumberOfPartitions;
        if(pageNumber == totalNumberOfPartitions - 1){
            Integer remainder = this.dataNumber % totalNumberOfPartitions;
            pageSize = pageSize + remainder;
        }
        return pageSize;
    }

    /**
     *
     * @param pageNumber
     *          当前页码,从0开始
     * @param totalNumberOfPartitions
     *          总共多少个拆分器
     * @param dataNumber
     *          拆分数据总数
     */
    public PaginationSplits(Integer pageNumber, Integer totalNumberOfPartitions, Integer dataNumber) {
        this.pageNumber = pageNumber;
        this.totalNumberOfPartitions = totalNumberOfPartitions;
        this.dataNumber = dataNumber;
    }
}

package com.jiang.phoenx.io.splits;

import org.apache.flink.core.io.InputSplit;

/**
 * @Author : jt
 * @Description : 分页拆分器
 *              根据数据总数与页码计算当前查询的数据条数与偏移量
 * @Date : Create in 9:40 2019/11/4
 */
public class PaginationSplits implements InputSplit {

    private Integer pageNumber;

    private Integer totalNumberOfPartitions;

    private Integer dataNumber;

    private Integer step;

    @Override
    public int getSplitNumber() {
        return pageNumber;
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
        return step * pageNumber;
    }

    /**
     * 获取当前页的数据量
     * 如果是最后一页,把余数加上
     * @return
     */
    public Integer getPageSize(){
        Integer pageSize = step;
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
        this.step = dataNumber / totalNumberOfPartitions;
    }
}

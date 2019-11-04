package com.jiang.phoenx.io.type;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

/**
 * @Author : jt
 * @Description :
 * @Date : Create in 9:11 2019/10/22
 */
public class TypeInfoFormat {

    public static BasicTypeInfo translated(String columnTypeName){
        switch (columnTypeName){
            case "UNSIGNED_INT" :
            case "INTEGER" :
                return BasicTypeInfo.INT_TYPE_INFO;
            case "BIGINT" :
            case "UNSIGNED_LONG" :
                return BasicTypeInfo.LONG_TYPE_INFO;
            case "TINYINT" :
            case "UNSIGNED_TINYINT" :
                return BasicTypeInfo.BYTE_TYPE_INFO;
            case "SMALLINT" :
            case "UNSIGNED_SMALLINT" :
                return BasicTypeInfo.SHORT_TYPE_INFO;
            case "FLOAT" :
            case "UNSIGNED_FLOAT" :
                return BasicTypeInfo.FLOAT_TYPE_INFO;
            case "DOUBLE" :
            case "UNSIGNED_DOUBLE" :
                return BasicTypeInfo.DOUBLE_TYPE_INFO;
            case "DECIMAL" :
                return BasicTypeInfo.BIG_DEC_TYPE_INFO;
            case "BOOLEAN" :
                return BasicTypeInfo.BOOLEAN_TYPE_INFO;
            case "TIME" :
            case "DATE" :
            case "TIMESTAMP" :
            case "UNSIGNED_TIME" :
            case "UNSIGNED_DATE" :
            case "UNSIGNED_TIMESTAMP" :
                return BasicTypeInfo.DATE_TYPE_INFO;
            default:
                return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

    private TypeInfoFormat() {
    }
}

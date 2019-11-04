package com.jiang.phoenx.io.framework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author : jt
 * @Description :
 * @Date : Create in 14:05 2019/10/24
 */
public class ConnectionUtil {

    public static Connection getPhoenixConnection(String url) throws SQLException {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            return DriverManager.getConnection(url);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
    }


    private ConnectionUtil() {
    }
}

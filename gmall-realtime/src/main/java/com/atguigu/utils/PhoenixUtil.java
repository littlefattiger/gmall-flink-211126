package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    public static void upsertValues(Connection conn, String sinkTable, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String columnStr = StringUtils.join(columns, ",");
        String valueStr = StringUtils.join(values, "','");
        String insertsql = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(" +
                columnStr + ") values ('" + valueStr + "')";
        PreparedStatement preparedSt = null;
        try {
            preparedSt = conn.prepareStatement(insertsql);
            preparedSt.execute();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            throw new RuntimeException("database execution exception");
        } finally {
            if (preparedSt != null) {
                try {
                    preparedSt.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    throw new RuntimeException("database execution exception");
                }
            }
        }
    }
}

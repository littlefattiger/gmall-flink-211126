package com.atguigu.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<T> result = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()){

            T t = clz.newInstance();

            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(columnName);
                if (underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                BeanUtils.setProperty(t, columnName, value);
            }
            result.add(t);
        }
        resultSet.close();
        preparedStatement.close();
        return result;
    }
}

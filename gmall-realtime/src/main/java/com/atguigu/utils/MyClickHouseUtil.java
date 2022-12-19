package com.atguigu.utils;

import com.atguigu.bean.KeywordBean;
import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {


    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        Class<?> tClz = t.getClass();
//                        Method[] methods = tClz.getMethods();
//                        for (int i = 0; i < methods.length; i++) {
//                            Method method = methods[i];
//                            method.invoke(t);
//                        }
                        Field[] declaredFields = tClz.getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];
                            field.setAccessible(true);
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;

                            }

                            Object value = field.get(t);
                            preparedStatement.setObject(i + 1 - offset, value);
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(100L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}

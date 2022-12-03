package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DruidDSUtil;

import com.atguigu.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private static DruidDataSource duridDataSource = null;
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        DruidPooledConnection connection = duridDataSource.getConnection();

        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        PhoenixUtil.upsertValues(connection, sinkTable, data);
        connection.close();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        duridDataSource = DruidDSUtil.createDataSource();
    }
}

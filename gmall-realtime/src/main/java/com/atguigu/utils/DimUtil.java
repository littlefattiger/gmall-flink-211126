package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.JdbcUtil;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;
        String dimJsonStr = jedis.get(redisKey);
        if (dimJsonStr != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return JSON.parseObject(dimJsonStr);
        }

        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + key + "'";
        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        JSONObject dimInfo = jsonObjects.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();
        return dimInfo;
    }

    public static void delDimInfo(String tableName, String key) {
        Jedis jedis = JedisUtil.getJedis();
        jedis.del("DIM:" + tableName + ":" + key);
        jedis.close();
    }
}

package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserRegisterBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.ClickHouseUtil;

import java.time.Duration;

public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window_211126";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String create_time = jsonObject.getString("create_time");
            return new UserRegisterBean("", "", 1L, DateFormatUtil.toTs(create_time, true));
        });
        SingleOutputStreamOperator<UserRegisterBean> userRegisterBeanSingleOutputStreamOperator = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                        return userRegisterBean.getTs();
                    }
                }
        ));
        SingleOutputStreamOperator<Object> resultDS = userRegisterBeanSingleOutputStreamOperator.windowAll(TumblingEventTimeWindows.of(
                Time.seconds(10L))).reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean v1, UserRegisterBean v2) throws Exception {
                v1.setRegisterCt(v1.getRegisterCt() + v2.getRegisterCt());
                return v1;
            }
        }, new AllWindowFunction<UserRegisterBean, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<UserRegisterBean> iterable, Collector<Object> collector) throws Exception {
                UserRegisterBean next = iterable.iterator().next();
                next.setTs(System.currentTimeMillis());
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                collector.collect(next);
            }
        });

        resultDS.addSink(MyClickHouseUtil.getSinkFunction(
                "insert into dws_user_user_register_window values(?,?,?,?)"
        ));
        resultDS.print(">>>>>>>>>>>>>");
        env.execute();

    }
}

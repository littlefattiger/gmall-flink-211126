package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window_211126";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject e, long l) {
                        String operation_time = e.getString("operation_time");
                        if (operation_time != null) {
                            return DateFormatUtil.toTs(operation_time, true);

                        } else {
                            return DateFormatUtil.toTs(e.getString("create_time"), true);

                        }

                    }
                })
        );
        KeyedStream<JSONObject, String> keyedStream = withWatermarkDS.keyBy(r -> r.getString("user_id"));
        SingleOutputStreamOperator<CartAddUuBean> cartaddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> lastCartaddedState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-cart", String.class);
                stringValueStateDescriptor.enableTimeToLive(ttlConfig);
                lastCartaddedState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public void flatMap(JSONObject va, Collector<CartAddUuBean> collector) throws Exception {
                String lastDt = lastCartaddedState.value();
                String operate_time = va.getString("operate_time");
                String curDt = null;
                if (operate_time != null){
                    curDt =operate_time.split(" ")[0];
                }else {
                    String create_time = va.getString("create_time");
                    curDt = create_time.split(" ")[0];
                }
                 if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartaddedState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", 1L, null));
                }
            }
        });
        SingleOutputStreamOperator<Object> resultDS = cartaddDS.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L))).reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean v1, CartAddUuBean v2) throws Exception {
                v1.setCartAddUuCt(v1.getCartAddUuCt() + v2.getCartAddUuCt());
                return v1;
            }
        }, new AllWindowFunction<CartAddUuBean, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<Object> collector) throws Exception {
                CartAddUuBean next = iterable.iterator().next();
                next.setTs(System.currentTimeMillis());
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                collector.collect(next);
            }
        });
        resultDS.print(">>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)" ));
        env.execute();
    }
}

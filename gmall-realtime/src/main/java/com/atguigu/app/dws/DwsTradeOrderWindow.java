package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.bean.TradeOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

import javax.sound.midi.Soundbank;
import java.time.Duration;

public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window_211126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("value>>>" + e);
                }
            }
        });

        KeyedStream<JSONObject, String> keyedbyDedailIdDS = jsonObjDS.keyBy(js -> js.getString("id"));
        SingleOutputStreamOperator<JSONObject> filterDS = keyedbyDedailIdDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig buildConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("is-exists", String.class);
                stringValueStateDescriptor.enableTimeToLive(buildConfig);
                valueState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String state = valueState.value();
                if (state == null) {
                    valueState.update("1");
                    return true;
                }
                return false;
            }
        });

        SingleOutputStreamOperator<JSONObject> jsonWithWMDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject js, long l) {

                return DateFormatUtil.toTs(js.getString("create_time"), true);
            }
        }));

        KeyedStream<JSONObject, String> keyedStream = jsonWithWMDS.keyBy(js -> js.getString("user_id"));
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDS = keyedStream.map(new RichMapFunction<JSONObject, TradeOrderBean>() {
            private ValueState<String> lastOrderDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order", String.class));
            }

            @Override
            public TradeOrderBean map(JSONObject va) throws Exception {
                String lastOrderDt = lastOrderDtState.value();
                String curDt = va.getString("create_time").split(" ")[0];
                long orderSucUniqueUserCount = 0L;
                long orderSucNewUserCount = 0L;
                if (lastOrderDt == null) {
                    orderSucUniqueUserCount = 1L;
                    orderSucNewUserCount = 1L;
                    lastOrderDtState.update(curDt);
                } else if (!lastOrderDt.equals(curDt)) {
                    orderSucUniqueUserCount = 1L;
                    lastOrderDtState.update(curDt);

                }
                Integer sku_num = va.getInteger("sku_num");
                Double order_price = va.getDouble("order_price");
                Double split_activity_amount = va.getDouble("split_activity_amount");
                if (split_activity_amount == null){
                    split_activity_amount = 0.0D;
                }
                Double split_coupon_amount = va.getDouble("split_coupon_amount");
                if (split_coupon_amount== null){
                    split_coupon_amount =0.0D;
                }
                return new TradeOrderBean("", "", orderSucUniqueUserCount, orderSucNewUserCount,
                        split_activity_amount, split_coupon_amount,
                        sku_num * order_price, null);
            }
        });

        SingleOutputStreamOperator<Object> resultDS = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(
                new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean v1, TradeOrderBean v2) throws Exception {
                        v1.setOrderUniqueUserCount(v1.getOrderUniqueUserCount() + v2.getOrderUniqueUserCount());
                        v1.setOrderNewUserCount(v1.getOrderUniqueUserCount() + v2.getOrderUniqueUserCount());
                        v1.setOrderOriginalTotalAmount(v1.getOrderOriginalTotalAmount() + v2.getOrderOriginalTotalAmount());
                        v1.setOrderActivityReduceAmount(v1.getOrderActivityReduceAmount() + v2.getOrderActivityReduceAmount());
                        v1.setOrderCouponReduceAmount(v1.getOrderCouponReduceAmount() + v2.getOrderCouponReduceAmount());
                        return v1;
                    }
                }, new AllWindowFunction<TradeOrderBean, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradeOrderBean> iterable, Collector<Object> collector) throws Exception {
                        TradeOrderBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);
                    }
                }
        );

        resultDS.print(">>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));

        env.execute("DwsTradeOrderWindow");
    }
}

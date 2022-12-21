package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.bean.TradePaymentWindowBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window_211126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println(">>>>>" + e);
                }

            }
        });
        KeyedStream<JSONObject, String> jsonObjKeyedByDetailIdDS = jsonObjDS.keyBy(r -> r.getString("order_detail_id"));
//        jsonObjKeyedByDetailIdDS.print(">>>>>>>>>>>>>");
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjKeyedByDetailIdDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject state = valueState.value();
                        if (state == null) {
                            valueState.update(value);
                            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                        } else {
                            String stateRt = state.getString("row_op_ts");
                            String curRt = value.getString("row_op_ts");
                            int comp = TimestampLtz3CompareUtil.compare(stateRt, curRt);
                            if (comp != 1) {
                                valueState.update(value);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        JSONObject value = valueState.value();
                        out.collect(value);
                        valueState.clear();
                    }
                }
        );
//        filterDS.print(">!!>>>>>>>");
        SingleOutputStreamOperator<JSONObject> jsonObjectWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                String callback_time = jsonObject.getString("callback_time");

                return DateFormatUtil.toTs(callback_time, true);
            }
        }));

        KeyedStream<JSONObject, String> keyedbyUidDS = jsonObjectWithWmDS.keyBy(js -> js.getString("user_id"));

        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedbyUidDS.flatMap(
                new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
                    private ValueState<String> lastDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                        String lastDt = lastDtState.value();
                        String curDt = jsonObject.getString("callback_time").split(" ")[0];
                        long paymentSucUniqueUserCount = 0L;
                        long paymentSucNewUserCount = 0L;
                        if (lastDt == null) {
                            paymentSucUniqueUserCount = 1L;
                            paymentSucNewUserCount = 1L;
                            lastDtState.update(curDt);
                        } else if (!lastDt.equals(curDt)) {
                            paymentSucUniqueUserCount = 1L;
                            lastDtState.update(curDt);
                        }
                        if (paymentSucUniqueUserCount == 1) {
                            collector.collect(new TradePaymentWindowBean("", "", paymentSucUniqueUserCount, paymentSucNewUserCount, null));
                        }
                    }
                }

        );


        // TODO 10. 开窗
        SingleOutputStreamOperator<Object> resultDS = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(
                Time.seconds(10L))).reduce(new ReduceFunction<TradePaymentWindowBean>() {
            @Override
            public TradePaymentWindowBean reduce(TradePaymentWindowBean v1, TradePaymentWindowBean v2) throws Exception {

                v1.setPaymentSucUniqueUserCount(v1.getPaymentSucUniqueUserCount() + v2.getPaymentSucUniqueUserCount());
                v1.setPaymentSucNewUserCount(v1.getPaymentSucNewUserCount() + v2.getPaymentSucNewUserCount());
                return v1;
            }
        }, new AllWindowFunction<TradePaymentWindowBean, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TradePaymentWindowBean> iterable, Collector<Object> collector) throws Exception {
                TradePaymentWindowBean next = iterable.iterator().next();
                next.setTs(System.currentTimeMillis());
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                collector.collect(next);
            }
        });

        // TODO 12. 写出到 OLAP 数据库
        resultDS.print(">>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction(
                "insert into dws_trade_payment_suc_window values(?,?,?,?,?)"
        ));


        env.execute();

    }
}

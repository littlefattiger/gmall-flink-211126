package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserLoginBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window_211126";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        // TODO 4. ??????????????????
        SingleOutputStreamOperator<JSONObject> mappedStream = pageLogSource.map(JSON::parseObject);

        // TODO 5. ?????????????????????????????? id ?????? null ??? last_page_id ??? null ?????? login ?????????
        SingleOutputStreamOperator<JSONObject> filteredStream = mappedStream.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        return jsonObj.getJSONObject("common")
                                .getString("uid") != null
                                && (jsonObj.getJSONObject("page")
                                .getString("last_page_id") == null
                                || jsonObj.getJSONObject("page")
                                .getString("last_page_id").equals("login"));
                    }
                }
        );

        // TODO 6. ???????????????
        SingleOutputStreamOperator<JSONObject> streamOperator = filteredStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 7. ?????? uid ??????
        KeyedStream<JSONObject, String> keyedStream
                = streamOperator.keyBy(r -> r.getJSONObject("common").getString("uid"));

        // TODO 8. ????????????????????????????????????????????????????????????????????????
        SingleOutputStreamOperator<UserLoginBean> backUniqueUserStream = keyedStream
                .process(
                        new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                            private ValueState<String> lastLoginDtState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                lastLoginDtState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<String>("last_login_dt", String.class)
                                );
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<UserLoginBean> out) throws Exception {
                                String lastLoginDt = lastLoginDtState.value();

                                // ??????????????????????????????????????????????????????
                                long backCt = 0L;
                                long uuCt = 0L;

                                // ????????????????????????
                                Long ts = jsonObj.getLong("ts");
                                String loginDt = DateFormatUtil.toDate(ts);

                                if (lastLoginDt != null) {
                                    // ???????????????????????????????????????
                                    if (!loginDt.equals(lastLoginDt)) {
                                        uuCt = 1L;
                                        // ???????????????????????????
                                        // ??????????????????????????????????????????
                                        Long lastLoginTs = DateFormatUtil.toTs(lastLoginDt);
                                        long days = (ts - lastLoginTs) / 1000 / 3600 / 24;

                                        if (days >= 8) {
                                            backCt = 1L;
                                        }
                                        lastLoginDtState.update(loginDt);
                                    }
                                } else {
                                    uuCt = 1L;
                                    lastLoginDtState.update(loginDt);
                                }

                                // ?????????????????????????????????????????? 0??????????????????????????????????????????
                                if (backCt != 0 || uuCt != 0) {
                                    out.collect(new UserLoginBean(
                                            "",
                                            "",
                                            backCt,
                                            uuCt,
                                            ts
                                    ));
                                }
                            }
                        }
                );

        // TODO 9. ??????
        AllWindowedStream<UserLoginBean, TimeWindow> windowStream = backUniqueUserStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 10. ??????
        SingleOutputStreamOperator<UserLoginBean> reducedStream = windowStream.reduce(
                new ReduceFunction<UserLoginBean>() {

                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {

                    @Override
                    public void process(Context context, Iterable<UserLoginBean> elements, Collector<UserLoginBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (UserLoginBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 11. ?????? OLAP ?????????
        SinkFunction<UserLoginBean> jdbcSink = MyClickHouseUtil.getSinkFunction(
                "insert into dws_user_user_login_window values(?,?,?,?,?)"
        );
        reducedStream.print(">>>>>>>>>>>>>");
        reducedStream.addSink(jdbcSink);

        env.execute();


    }
}

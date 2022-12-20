package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.ClickHouseUtil;

import java.time.Duration;

public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    collector.collect(jsonObject);
                }

            }
        });

        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(r -> r.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> homeLastVisitDt;
                    private ValueState<String> detailLastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        super.open(parameters);
                        homeLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("home_last_visit_dt", String.class)
                        );
                        detailLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("detail_last_visit_dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                        String homeLastDt = homeLastVisitDt.value();
                        String detailLastDt = detailLastVisitDt.value();

                        JSONObject page = jsonObj.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        String visitDt = DateFormatUtil.toDate(ts);

                        Long homeUvCt = 0L;
                        Long detailUvCt = 0L;

                        if (pageId.equals("home")) {
                            if (homeLastDt == null || !homeLastDt.equals(visitDt)) {
                                homeUvCt = 1L;
                                homeLastVisitDt.update(visitDt);
                            }
                        }

                        if (pageId.equals("good_detail")) {
                            if (detailLastDt == null || !detailLastDt.equals(visitDt)) {
                                detailUvCt = 1L;
                                detailLastVisitDt.update(visitDt);
                            }
                        }

                        if (homeUvCt != 0 || detailUvCt != 0) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "",
                                    "",
                                    homeUvCt,
                                    detailUvCt,
                                    0L
                            ));
                        }
                    }
                }
        );

        // TODO 9. 开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowStream = uvStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 10. 聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reducedStream = windowStream.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setGoodDetailUvCt(
                                value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt()
                        );
                        value1.setHomeUvCt(
                                value1.getHomeUvCt() + value2.getHomeUvCt()
                        );
                        return value1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(window.getStart());
                        String edt = DateFormatUtil.toYmdHms(window.getEnd());

                        for (TrafficHomeDetailPageViewBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setTs(System.currentTimeMillis());
                            out.collect(value);
                        }
                    }
                }
        );

        reducedStream.print(">>>>>>>>>>>>>");
        SinkFunction<TrafficHomeDetailPageViewBean> jdbcSink = MyClickHouseUtil.getSinkFunction(
                "insert into dws_traffic_page_view_window values(?,?,?,?,?)"
        );
        reducedStream.<TrafficHomeDetailPageViewBean>addSink(jdbcSink);

        env.execute();


    }
}

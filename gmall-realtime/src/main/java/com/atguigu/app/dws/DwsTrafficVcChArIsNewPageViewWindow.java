package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        String ujdTopic = "dwd_traffic_user_jump_detail";
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String topic = "dwd_traffic_page_log";
        String groupId = "vccharisnew_pageview_window_211126";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ujdTopic, groupId));
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")

            );
        });
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts")

            );
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPageDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            if (last_page_id == null) {
                sv = 1L;
            }
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, sv, 1L, page.getLong("during_time"), 0L,
                    jsonObject.getLong("ts")

            );
        });
        DataStream<TrafficPageViewBean> unionDS = trafficPageViewWithUvDS.union(trafficPageViewWithUjDS, trafficPageViewWithPageDS);
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }
                }));
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = trafficPageViewWithWmDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(
                        value.getAr(),
                        value.getCh(),
                        value.getIsNew(),
                        value.getVc()

                );
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean v1, TrafficPageViewBean v2) throws Exception {
                v1.setSvCt(v1.getSvCt() + v2.getSvCt());
                v1.setUvCt(v1.getUvCt() + v2.getUvCt());
                v1.setUjCt(v1.getUjCt() + v2.getUjCt());
                v1.setPvCt(v1.getPvCt() + v2.getPvCt());
                v1.setDurSum(v1.getDurSum() + v2.getDurSum());
                return v1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> out) throws Exception {
                TrafficPageViewBean next = iterable.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        resultDS.print(">>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));
    env.execute();
    }

}

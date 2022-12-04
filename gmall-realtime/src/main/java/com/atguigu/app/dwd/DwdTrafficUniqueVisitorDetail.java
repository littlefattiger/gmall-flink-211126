package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_unique_visitor_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(s);
                }
            }
        });
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastDate = lastVisitState.value();
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);
                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                }
                return false;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig
                        .Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastVisitState = getRuntimeContext().getState(stateDescriptor);
            }
        });
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDS.print("uvDS>>>>>>>>>>>>>>>>>>");
        uvDS.map(json -> json.toJSONString()).addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}

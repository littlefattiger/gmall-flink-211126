package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        KeyedStream<JSONObject, String> keyedStream = jsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));

//        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                return value.getJSONObject("page").getString("last_page_id") == null;
//            }
//        }).next("next").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                return value.getJSONObject("page").getString("last_page_id") == null;
//            }
//        }).next("next").within(Time.seconds(10));
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).times(2)
                .consecutive()
                .within(Time.seconds(10));
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }, new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                });
        DataStream<String> timeoutDS = selectDS.getSideOutput(timeOutTag);
        DataStream<String> unionDS = selectDS.union(timeoutDS);
        selectDS.print("SelectDS>>>>>>>>>>>>>>");
        timeoutDS.print("timeoutDS>>>>>>>>>>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        env.execute("DwdTrafficUserJumpDetail");
    }
}

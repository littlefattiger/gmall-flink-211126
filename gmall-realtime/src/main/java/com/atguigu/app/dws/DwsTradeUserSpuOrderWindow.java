package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeUserSpuOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window_211126";
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
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuDS = filterDS.map(js -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(js.getString("order_id"));
            return TradeUserSpuOrderBean.builder()
                    .skuId(js.getString("sku_id"))
                    .userId(js.getString("user_id"))
                    .orderAmount(js.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(js.getString("create_time"), true))
                    .build();
        });
        tradeUserSpuDS.map(new RichMapFunction<TradeUserSpuOrderBean, Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public Object map(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {
                return null;
            }
        });
    }
}

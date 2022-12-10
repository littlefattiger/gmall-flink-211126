package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table dwd_order_pre(\n" +
                "`id` string,\n" +
                "`order_id` string,\n" +
                "`sku_id` string,\n" +
                "`sku_name` string,\n" +
                "`order_price` string,\n" +
                "`sku_num` string,\n" +
                "`create_time` string,\n" +
                "`source_type_id` string,\n" +
                "`source_type_name` string,\n" +
                "`source_id` string,\n" +
                "`split_total_amount` string,\n" +
                "`split_activity_amount` string,\n" +
                "`split_coupon_amount` string,\n" +
                "`consignee` string,\n" +
                "`consignee_tel` string,\n" +
                "`total_amount` string,\n" +
                "`order_status` string,\n" +
                "`user_id` string,\n" +
                "`payment_way` string,\n" +
                "`delivery_address` string,\n" +
                "`order_comment` string,\n" +
                "`out_trade_no` string,\n" +
                "`trade_body` string,\n" +
                "`operate_time` string,\n" +
                "`expire_time` string,\n" +
                "`process_status` string,\n" +
                "`tracking_no` string,\n" +
                "`parent_order_id` string,\n" +
                "`province_id` string,\n" +
                "`activity_reduce_amount` string,\n" +
                "`coupon_reduce_amount` string,\n" +
                "`original_total_amount` string,\n" +
                "`feight_fee` string,\n" +
                "`feight_fee_reduce` string,\n" +
                "`refundable_time` string,\n" +
                "`order_detail_activity_id` string,\n" +
                "`activity_id` string,\n" +
                "`activity_rule_id` string,\n" +
                "`order_detail_coupon_id` string,\n" +
                "`coupon_id` string,\n" +
                "`coupon_use_id` string,\n" +
                "`type` string,\n" +
                "`old` map<string,string> \n" +

                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process", "order_detail_20211126"));

        Table filteredTable = tableEnv.sqlQuery(
                "select \n" +
                        "id,\n" +
                        "order_id,\n" +
                        "user_id,\n" +
                        "sku_id,\n" +
                        "sku_name,\n" +
                        "sku_num,\n" +
                        "order_price,\n" +
                        "province_id,\n" +
                        "activity_id,\n" +
                        "activity_rule_id,\n" +
                        "coupon_id,\n" +
//                        "date_id,\n" +
                        "create_time,\n" +
                        "source_id,\n" +
                        "source_type_id,\n" +
                        "source_type_name,\n" +
//                        "sku_num,\n" +
//                        "split_original_amount,\n" +
                        "split_activity_amount,\n" +
                        "split_coupon_amount,\n" +
                        "split_total_amount \n" +
//                        "od_ts ts,\n" +
//                        "row_op_ts\n" +
                        "from dwd_order_pre " +
                        "where `type`='insert'"
        );
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "sku_num string,\n" +
                "order_price string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
//                "date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type_id string,\n" +
                "source_type_name string,\n" +
//                "sku_num string,\n" +
//                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string \n" +
//                "ts string,\n" +
//                "row_op_ts timestamp_ltz(3)\n" +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from filtered_table");
        env.execute();
    }
}

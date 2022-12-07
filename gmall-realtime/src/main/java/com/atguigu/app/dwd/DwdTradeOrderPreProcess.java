package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("order_pre_process_211126"));

        Table orderDetailTable = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['order_price'] order_price,\n" +
                "data['sku_num'] sku_num,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_type'] source_type,\n" +
                "data['source_id'] source_id,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "pt \n" +
                "from `topic_db` where `database` = 'gmall-211126' and `table` = 'order_detail' ");
        tableEnv.createTemporaryView("order_detail_table", orderDetailTable);

        Table orderInfoTable = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['consignee'] consignee,\n" +
                "data['consignee_tel'] consignee_tel,\n" +
                "data['total_amount'] total_amount,\n" +
                "data['order_status'] order_status,\n" +
                "data['user_id'] user_id,\n" +
                "data['payment_way'] payment_way,\n" +
                "data['delivery_address'] delivery_address,\n" +
                "data['order_comment'] order_comment,\n" +
                "data['out_trade_no'] out_trade_no,\n" +
                "data['trade_body'] trade_body,\n" +
                "data['create_time'] create_time,\n" +
                "data['operate_time'] operate_time,\n" +
                "data['expire_time'] expire_time,\n" +
                "data['process_status'] process_status,\n" +
                "data['tracking_no'] tracking_no,\n" +
                "data['parent_order_id'] parent_order_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['activity_reduce_amount'] activity_reduce_amount,\n" +
                "data['coupon_reduce_amount'] coupon_reduce_amount,\n" +
                "data['original_total_amount'] original_total_amount,\n" +
                "data['feight_fee'] feight_fee,\n" +
                "data['feight_fee_reduce'] feight_fee_reduce,\n" +
                "data['refundable_time'] refundable_time,\n" +
                "`type`,\n" +
                "`old`,\n" +
                "ts oi_ts\n" +
                "from `topic_db`\n" +
                "where `database` = 'gmall-211126' and `table` = 'order_info'\n" +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info", orderInfoTable);

        tableEnv.toAppendStream(orderInfoTable, Row.class).print(">>>>>")

        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        env.execute();

    }
}

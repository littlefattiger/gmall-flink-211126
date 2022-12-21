package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradePayDetailSuc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("pay_detail_suc_211126"));
        Table paymentInfo = tableEnv.sqlQuery("select\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['payment_type'] payment_type,\n" +
                "data['callback_time'] callback_time,\n" +
                "`pt` \n" +
                "from topic_db\n" +
                "where `table` = 'payment_info'\n"
                +
                "and `type` = 'update'\n" +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);
//        tableEnv.toAppendStream(paymentInfo, Row.class).print();

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
                "split_total_amount string, \n" +
//                "ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "pay_detail_suc_order_211126"));
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "od.id order_detail_id,\n" +
                "od.order_id,\n" +
                "od.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "od.province_id,\n" +
                "od.activity_id,\n" +
                "od.activity_rule_id,\n" +
                "od.coupon_id,\n" +
                "pi.payment_type payment_type_code,\n" +
                "dic.dic_name payment_type_name,\n" +
                "pi.callback_time,\n" +
                "od.source_id,\n" +
                "od.source_type_id,\n" +
                "od.source_type_name,\n" +
                "od.sku_num,\n" +
                "od.order_price,\n" +
//                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount split_payment_amount, \n" +
//                "pi.ts,\n" +
                "od.row_op_ts row_op_ts\n" +
                "from payment_info pi\n" +
                "join dwd_trade_order_detail od\n" +
                "on pi.order_id = od.order_id\n" +
                "join `base_dic` for system_time as of pi.pt as dic\n" +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        tableEnv.executeSql("create table dwd_trade_pay_detail_suc(\n" +
                "order_detail_id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "callback_time string,\n" +
                "source_id string,\n" +
                "source_type_id string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "order_price string,\n" +
//                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_payment_amount string, \n" +
//                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(order_detail_id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        tableEnv.executeSql("" +
                "insert into dwd_trade_pay_detail_suc select * from result_table");

//        env.execute("DwdTradePayDetailSuc");

    }
}

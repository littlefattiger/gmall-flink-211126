package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add_211126"));
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['cart_price'] cart_price,\n" +
                "if(`type` = 'insert',\n" +
                "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['is_checked'] is_checked,\n" +
                "data['create_time'] create_time,\n" +
                "data['operate_time'] operate_time,\n" +
                "data['is_ordered'] is_ordered,\n" +
                "data['order_time'] order_time,\n" +
                "data['source_type'] source_type,\n" +
                "data['source_id'] source_id,\n" +
                "pt \n" +

                "from `topic_db` \n" +
                "where `table` = 'cart_info'\n" +
                "and `database` = 'gmall-211126'\n" +
                "and (`type` = 'insert'\n" +
                "or (`type` = 'update' \n" +
                "and `old`['sku_num'] is not null \n" +
                "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("cart_info_table", cartAddTable);

//        tableEnv.toAppendStream(cartAddTable, Row.class).print(">>>>>>>>>>");

        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        Table cartAddWithDicTable = tableEnv.sqlQuery("select\n" +
                "ci.id,\n" +
                "ci.user_id,\n" +
                "ci.sku_id,\n" +
                "ci.cart_price,\n" +
                "ci.sku_num,\n" +
                "ci.sku_name,\n" +
                "ci.is_checked,\n" +
                "ci.create_time,\n" +
                "ci.operate_time,\n" +
                "ci.is_ordered,\n" +
                "ci.order_time,\n" +

                "ci.source_type source_type_id,\n" +
                "dic.dic_name source_type_name,\n" +
                "ci.source_id \n" +
                "from cart_info_table ci\n" +
                "join base_dic for system_time as of ci.pt as dic\n" +
                "on ci.source_type=dic.dic_code");
        tableEnv.createTemporaryView("cart_add_dic_table", cartAddWithDicTable);
        tableEnv.executeSql("" +
                "create table dwd_cart_add(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "cart_price string,\n" +
                "sku_num string,\n" +
                "sku_name string,\n" +
                "is_checked string,\n" +
                "create_time string,\n" +
                "operate_time string,\n" +
                "is_ordered string,\n" +
                "order_time string,\n" +

                "source_type_id string,\n" +
                "source_type_name string,\n" +
                "source_id string \n" +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));
        tableEnv.executeSql("" +
                "insert into dwd_cart_add select * from cart_add_dic_table").print();

        env.execute("DwdTradeCartAdd");
    }
}

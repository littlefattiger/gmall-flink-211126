package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordBean;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import ru.yandex.clickhouse.ClickHouseUtil;

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window_211126";
        tableEnv.executeSql("create table page_log(\n" +
                "`common` map<string, string>,\n" +
                "`page` map<string, string>,\n" +
                "`ts` bigint,\n" +
                "rt AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" +
                ")" + MyKafkaUtil.getKafkaDDL(topic, groupId));

        Table filterTable = tableEnv.sqlQuery("select\n" +
                "page['item'] item,\n" +
                "rt\n" +
                "from page_log\n" +
                "where page['item'] is not null\n" +
                "and page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("filter_table", filterTable);


        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        Table splitTable = tableEnv.sqlQuery("select\n" +
                "word,\n" +
                "rt \n" +
                "from filter_table,\n" +
                "LATERAL TABLE(SplitFunction(item))  "
        );
        tableEnv.createTemporaryView("split_table", splitTable);

        Table resultTable = tableEnv.sqlQuery("select\n" +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                GmallConstant.KEYWORD_SEARCH + "' source,\n" +
                "word keyword,\n" +
                "count(*) keyword_count,\n" +
                "UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "GROUP BY TUMBLE(rt, INTERVAL '10' SECOND),word");

        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
//        keywordBeanDS.print((">>>>>>>>>>>>>>>"));
//        SinkFunction<KeywordBean> jdbcSink = ClickHouseUtil.<KeywordBean>getJdbcSink(
//                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)");
        keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window " +
                "values(?,?,?,?,?,?)"));

        env.execute();

    }
}

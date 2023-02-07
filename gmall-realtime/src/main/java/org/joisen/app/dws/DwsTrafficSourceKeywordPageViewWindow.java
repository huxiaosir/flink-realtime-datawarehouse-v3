package org.joisen.app.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.joisen.app.func.SplitFunction;
import org.joisen.utils.MyKafkaUtil;

/**
 * @Author Joisen
 * @Date 2023/2/7 17:08
 * @Version 1.0
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) {

        // todo 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 1.1. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "joisen");

        // todo 2. 使用DDL方式读取Kafka page_log 主题的数据创建表并且提取时间戳生产Watermark
        String topic = "dwd_traffic_page_log_0105";
        String groupId = "dws_traffic_source_keyword_page_view_window_0105";
        tableEnv.executeSql("" +
                "create table page_log( " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ")" + MyKafkaUtil.getKafkaDDL(topic, groupId));

        // todo 3. 过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                " select " +
                "    page['item'] item, " +
                "    rt " +
                " from page_log " +
                " where page['last_page_id'] = 'search' " +
                " and page['item_type'] = 'keyword' " +
                " and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);

        // todo 4. 注册UDTF & 切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT\n" +
                "    word,\n" +
                "    rt\n" +
                "FROM filter_table, \n" +
                "LATERAL TABLE(SplitFunction(item))");
        tableEnv.createTemporaryView("split_table", splitTable);

        // todo 5. 分组、开窗、聚合

        // todo 6. 将动态表转换为流

        // todo 7. 将数据写出到ClickHouse

        // todo 8. 启动任务

    }
}

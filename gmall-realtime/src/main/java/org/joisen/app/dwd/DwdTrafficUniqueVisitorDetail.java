package org.joisen.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.flink.util.Collector;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyKafkaUtil;

/**
 * @Author Joisen
 * @Date 2023/2/3 20:01
 * @Version 1.0
 */

// 数据流： web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
// 程  序：      Mock(log_0105.sh) -> Flume(flume1_0105.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
public class DwdTrafficUniqueVisitorDetail {

    public static void main(String[] args) throws Exception {
        // todo 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境中设置为kafka主题的分区数

        // todo 1.1 开启CheckPoint
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        // todo 1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/0105/ck");
//        System.setProperty("HADOOP_USER_NAME", "joisen");

        // todo 2. 读取kafka 页面日志主题创建流
        String topic = "dwd_traffic_page_log_0105";
        String groupId = "unique_visitor_detail_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 过滤掉上一跳页面不为null的数据 并 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 获取上一跳页面ID
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }
            }
        });

        // todo 4. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // todo 5. 使用状态编程时间按照mid去重
        // 需要实现状态过期： 1、定时器： 需要在process中实现
        //                 2、TTL：设置状态存活时间为24h，更改状态时更新过期时间
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                // 设置状态的TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastVisitState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                // 获取状态数据 & 当前数据中的时间戳并转换为日期
                String lastDate = lastVisitState.value();
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // todo 6. 将数据写道kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail_0105";
        uvDS.print(">>>>>");
        uvDS.map(JSONAware::toJSONString)
            .addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));


        // todo 7. 启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");


    }

}

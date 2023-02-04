package org.joisen.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.joisen.utils.MyKafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author Joisen
 * @Date 2023/2/4 14:25
 * @Version 1.0
 */

// 数据流： web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
// 程  序：      Mock(log_0105.sh) -> Flume(flume1_0105.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)
public class DwdTrafficUserJumpDetail {

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

        // todo 2. 读取kafka 页面日志主题数据创建流
        String topic = "dwd_traffic_page_log_0105";
        String groupId = "user_jump_detail_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 将每行数据转换为JSON对象  BaseLogApp中以及过滤了非json格式的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(line -> JSON.parseObject(line));

        // todo 4. 提取事件时间 & 按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));

        // todo 5. 定义CEP的模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception { // 第一个事件 需要上一跳页面id=null
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() { // 第二个事件 也需要上一跳页面id=null
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        /* 循环模式 用times() 简写重复的匹配规则 */
//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                return jsonObject.getJSONObject("common").getString("last_page_id") == null;
//            }
//        }).times(2)             // 默认时宽松近邻 followedBy
//                .consecutive()  // 严格近邻 next
//                .within(Time.seconds(2));

        // todo 6. 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // todo 7. 提取事件（匹配上的事件以及超时事件）
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut"){};
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag,
                // 不管有没有超时 取的数据都是第一个(start)
                new PatternTimeoutFunction<JSONObject, String>() {
                    // 使用Map来封装：key为“start” value为匹配到的数据
                    // Map中key对应的value为集合 的原因是 循环模式只写了一个key而对应的匹配规则有多个
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
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);


        // todo 8. 合并两个事件
        DataStream<String> unionDS = selectDS.union(timeOutDS);

        // todo 9. 将数据写入到Kafka
        selectDS.print("Select>>>>>");
        timeOutDS.print("TimeOut>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail_0105";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        // todo 10. 启动任务
        env.execute("DwdTrafficUserJumpDetail");

    }

}

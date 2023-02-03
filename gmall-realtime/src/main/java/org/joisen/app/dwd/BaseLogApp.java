package org.joisen.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyKafkaUtil;

/**
 * @Author Joisen
 * @Date 2023/2/3 14:54
 * @Version 1.0
 */

// 数据流： web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
// 程  序：      Mock(log_0105.sh) -> Flume(flume1_0105.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {
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


        // todo 2. 消费kafka topic_log_0105 主题的数据创建流
        String topic = "topic_log_0105";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 过滤掉非JSON格式的数据 & 将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        // 获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty Data>>>>");

        // todo 4. 按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        // todo 5. 使用状态编程 做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }
            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 获取is_new标记和ts  并将ts转换为年月日
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                // 获取状态中的日期 (状态的key为mid，value为首次访问的日期)
                String lastDate = lastVisitState.value();

                // 判断is_new标记是否为"1"
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate); // 新访客，更新状态
                    } else if (!lastDate.equals(curDate)) {
                        value.getJSONObject("common").put("is_new", 0); // 不是新访客，修改字段
                    } // 有状态、首次访问日期为当前日期 且 is_new为 1 则 不做任何操作
                } else if (lastDate == null) { // is_new 为 0 且首次访问日期未记录 则需要将状态中首次访问日期更改为curDate的前一天
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                    // 如果已经记录首次访问日期 则 不做任何操作
                }
                return value;
            }
        });

        // todo 6. 使用侧输出流进行分流处理  页面日志放到主流 启动、曝光、动作、错误日志放到侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> errorTag = new OutputTag<String>("error"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    // 将数据写道error侧输出流
                    ctx.output(errorTag, value.toJSONString());
                }
                // 移除错误信息, 保留其中的正常信息
                value.remove("err");

                // 尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    // 将数据写到start侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else { //页面日志信息（页面日志和启动日志是互斥关系）

                    // 获取公共信息 & 页面id & 时间戳
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");

                    // 尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 遍历曝光数据 & 写到display侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    // 尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        // 遍历曝光数据 & 写到display侧输出流
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    // 移除曝光和动作数据 & 写入到页面日志主流
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });

        // todo 7. 提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        // todo 8. 将数据打印并写入对应的主题
        pageDS.print("Page>>>>>>>>>");
        startDS.print("Start>>>>>>>");
        displayDS.print("Display>>>");
        actionDS.print("Action>>>>>");
        errorDS.print("Error>>>>>>>");

        String page_topic = "dwd_traffic_page_log_0105";
        String start_topic = "dwd_traffic_start_log_0105";
        String display_topic = "dwd_traffic_display_log_0105";
        String action_topic = "dwd_traffic_action_log_0105";
        String error_topic = "dwd_traffic_error_log_0105";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        // todo 9. 启动任务
        env.execute("BaseLogApp");
    }
}

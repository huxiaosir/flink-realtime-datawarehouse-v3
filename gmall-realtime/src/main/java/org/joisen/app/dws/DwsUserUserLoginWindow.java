package org.joisen.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.joisen.bean.UserLoginBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/9 17:11
 * @Version 1.0
 */
// 数据流： web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程  序：      Mock(log_0105.sh) -> Flume(flume1_0105.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsUserUserLoginWindow -> ClickHouse(ZK)
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {

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

        // todo 2. 读取Kafka 页面日志主题创建流
        String topic = "dwd_traffic_page_log_0105";
        String groupId = "dws_user_userlogin_window_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 转换数据为JSON对象并过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);

                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (uid != null && (lastPageId == null || lastPageId.equals("login"))) {
                    collector.collect(jsonObject);
                }
            }
        });

        // todo 4. 提取事件时间生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));

        // todo 5. 按照uid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("uid"));

        // todo 6. 使用状态编程获取独立用户以及七日回流用户
        SingleOutputStreamOperator<UserLoginBean> userLoginDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
            private ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {

                lastLoginState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-login", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<UserLoginBean> collector) throws Exception {

                // 获取状态日期以及当前数据日期
                String lastLoginDt = lastLoginState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                // 定义当日独立用户数 & 七日回流用户数
                long uv = 0L;
                long backUv = 0L;

                if (lastLoginDt == null) {
                    uv = 1L;
                    lastLoginState.update(curDt);
                } else if (!lastLoginDt.equals(curDt)) {
                    uv = 1L;
                    lastLoginState.update(curDt);
                    if (DateFormatUtil.toTs(curDt) - DateFormatUtil.toTs(lastLoginDt) / (24 * 60 * 60 * 1000L) >= 8) { // 七天之后为回归用户
                        backUv = 1L;
                    }
                }

                if (uv != 0L) {
                    collector.collect(new UserLoginBean("", "", backUv, uv, ts));
                }

            }
        });

        // todo 7. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userLoginDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean val1, UserLoginBean val2) throws Exception {
                        val1.setBackCt(val1.getBackCt() + val2.getBackCt());
                        val1.setUuCt(val1.getUuCt() + val2.getUuCt());
                        return val1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });

        // todo 8. 将数据写出到ClickHouse
        resultDS.print(">>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_login_window values (?,?,?,?,?)"));

        // todo 9. 启动任务
        env.execute("DwsUserUserLoginWindow");


    }
}

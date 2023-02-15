package org.joisen.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.joisen.bean.UserRegisterBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/9 21:36
 * @Version 1.0
 */
// 数据流： Web/App  -> nginx  -> 业务服务器（MySQL） ->  Maxwell  -> Kafka(ODS)  ->  FlinkApp  ->  Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程序：      Mock -> Mysql  -> Maxwell  ->  Kafka(ZK)  ->  DwdUserRegister  -> Kafka(ZK) -> DwsUserUserRegisterWindow -> ClickHouse(ZK)
public class DwsUserUserRegisterWindow {
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

        // todo 2. 读取Kafka DWD层用户注册主题数据  创建流
        String topic = "dwd_user_register_0105";
        String groupId = "dws_user_user_register_window_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 将每行数据转换为JavaBean对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            // yyyy-MM-dd HH:mm:ss
            String createTime = jsonObject.getString("create_time");
            return new UserRegisterBean("", "", 1L, DateFormatUtil.toTs(createTime, true));
        });

        // todo 4. 提取时间戳生成WaterMark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                        return userRegisterBean.getTs();
                    }
                }));

        // todo 5. 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                        UserRegisterBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });

        // todo 6. 将数据写出到ClickHouse
        resultDS.print(">>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));

        // todo 7. 启动任务
        env.execute("DwsUserUserRegisterWindow");

    }
}

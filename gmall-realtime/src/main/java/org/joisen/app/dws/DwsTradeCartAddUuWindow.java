package org.joisen.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.joisen.bean.CartAddUuBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/10 11:03
 * @Version 1.0
 */
// 数据流： Web/App  -> nginx  -> 业务服务器（MySQL） ->  Maxwell  -> Kafka(ODS)  ->  FlinkApp  ->  Kafka(DWD) -> FlinkApp  ->  ClickHouse(DWS)
// 程序：      Mock -> Mysql  -> Maxwell  ->  Kafka(ZK)  ->  DwdTradeCartAdd  -> Kafka(ZK) -> DwsTradeCartAddUuWindow -> ClickHouse(ZK)
public class DwsTradeCartAddUuWindow {
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

        // todo 2. 读取Kafka DWD层 加购事实表
        String topic = "dwd_trade_cart_add_0105";
        String groupId = "dws_trade_cart_add_uu_window_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // todo 4. 提取事件时间生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        String operateTime = jsonObject.getString("operate_time");
                        if (operateTime != null) {
                            return DateFormatUtil.toTs(operateTime, true);
                        }
                        return DateFormatUtil.toTs(jsonObject.getString("create_time"), true);
                    }
                }));

        // todo 5. 按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(jsonObject -> jsonObject.getString("user_id"));

        // todo 6. 使用状态编程提取独立加购用户
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> lastCartAddState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-cart", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastCartAddState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<CartAddUuBean> collector) throws Exception {

                // 获取状态数据以及当前数据的日期
                String lastDt = lastCartAddState.value();
                String operateTime = jsonObject.getString("operate_time");
                String curDt = null;
                if(operateTime != null){
                    curDt = operateTime.split(" ")[0];
                }else {
                    String createTime = jsonObject.getString("create_time");
                    curDt = createTime.split(" ")[0];
                }

                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", 1L, null));
                }
            }
        });

        // todo 7. 开窗、聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean t, CartAddUuBean t1) throws Exception {
                        t.setCartAddUuCt(t.getCartAddUuCt() + t1.getCartAddUuCt());
                        return t;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean next = values.iterator().next();
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());

                        out.collect(next);
                    }
                });

        // todo 8. 将数据写出到ClickHouse
        resultDS.print(">>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        // todo 9. 启动任务
        env.execute("DwsTradeCartAddUuWindow");


    }
}

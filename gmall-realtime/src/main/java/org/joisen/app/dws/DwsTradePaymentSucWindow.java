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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.joisen.bean.TradePaymentWindowBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;
import org.joisen.utils.TimestampLtz3CompareUtil;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/15 17:14
 * @Version 1.0
 */
// 数据流：Web/App -> nginx -> 业务服务器（MySQL）-> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> kafka(DWD) -> flinkApp -> clickhouse(dws)
// 程序：   Mock -> Mysql -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwdTradePayDetailSuc -> Kafka(ZK) -> DwsTradePaymentSucWindow -> ClickHouse(ZK)
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {

        // todo 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // todo 2. 读取DWD层成功支付主题数据创建流
        String topic = "dwd_trade_pay_detail_suc_0105";
        String groupId = "dws_trade_payment_suc_window_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println(">>>>>" + s);
                }
            }
        });

        // todo 4. 按照订单明细id分组
        KeyedStream<JSONObject, String> jsonObjectKeyedByDetailIdDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("order_detail_id"));

        // todo 5. 使用状态编程保留最新的数据输出
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjectKeyedByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                // 获取状态中的数据
                JSONObject state = valueState.value();
                // 判断state是否为null
                if (state == null) {
                    valueState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                } else {
                    String stateRt = state.getString("row_op_ts");
                    String curRt = value.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(stateRt, curRt);
                    if (compare != 1) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                // 输出并清空状态数据
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();
            }
        });

        // todo 6. 提取时间时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        String callbackTime = jsonObject.getString("callback_time");
                        return DateFormatUtil.toTs(callbackTime, true);
                    }
                }));

        // todo 7. 按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));

        // todo 8. 提取独立支付成功用户数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePamentDS = keyedByUidDS.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                // 取出状态中以及当前数据的日期
                String lastDt = lastDtState.value();
                String curDt = jsonObject.getString("callback_time").split(" ")[0];
                // 定义当日支付人数以及新增支付人数
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;

                // 判断状态日期是否为null
                if (lastDt == null) {
                    paymentSucNewUserCount = 1L;
                    paymentSucUniqueUserCount = 1L;
                    lastDtState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastDtState.update(curDt);
                }
                // 返回数据
                if (paymentSucUniqueUserCount == 1L) {
                    collector.collect(new TradePaymentWindowBean("",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            null));
                }
            }
        });

        // todo 9. 开窗、聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePamentDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean v1, TradePaymentWindowBean v2) throws Exception {
                        v1.setPaymentSucUniqueUserCount(v1.getPaymentSucUniqueUserCount() + v2.getPaymentSucUniqueUserCount());
                        v1.setPaymentSucNewUserCount(v1.getPaymentSucNewUserCount() + v2.getPaymentSucNewUserCount());
                        return v1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                        TradePaymentWindowBean next = values.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        out.collect(next);
                    }
                });

        // todo 10. 将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        // todo 11. 启动任务
        env.execute("DwsTradePaymentSucWindow");

    }
}

package org.joisen.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joisen.app.func.DimAsyncFunction;
import org.joisen.bean.TradeProvinceOrderWindowBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;
import org.joisen.utils.TimestampLtz3CompareUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author Joisen
 * @Date 2023/2/17 16:52
 * @Version 1.0
 */
// 数据流： Web/App -> nginx -> 业务服务器（MySQL）-> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)  -> FlinkApp -> Kafka(DWD) -> flinkApp -> ClickHouse(DWS)
// 程序：    Mock -> Mysql -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwsTradeProvinceOrderWindow(Phoenix(hbase-hdfs-zk)、redis) -> ClickHouse(ZK)
public class DwsTradeProvinceOrderWindow {
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

        // todo 2. 读取kafka DWD层下单主题数据
        String topic = "dwd_trade_order_detail_0105";
        String groupId = "dws_trade_province_order_window_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("Value:" + s);
                }
            }
        });

        // todo 4. 按照订单明细ID分组、去重(取最后一条)
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 取出状态中的数据
                JSONObject lastValue = valueState.value();

                // 判断状态中的数据是否为null
                if (lastValue == null) {
                    valueState.update(value);
                    long processingTime = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(processingTime + 5000L);
                } else {
                    // 取出状态数据 以及 当前数据中的时间
                    String lastTs = lastValue.getString("row_op_ts");
                    String curTs = value.getString("row_op_ts");

                    if (TimestampLtz3CompareUtil.compare(lastTs, curTs) != 1) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                // 输出数据并清空状态
                out.collect(valueState.value());
                valueState.clear();
            }
        });

        // todo 5. 将每行数据转换为JavaBean
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> provinceOrderDS = filterDS.map(line -> {
            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(line.getString("order_id"));
            return new TradeProvinceOrderWindowBean("", "",
                    line.getString("province_id"),
                    "",
                    0L,
                    orderIdSet,
                    line.getDouble("split_total_amount"),
                    DateFormatUtil.toTs(line.getString("create_time"), true));
        });

        // todo 6. 提取时间戳生成 WaterMark
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> tradeProvinceWithWmDS = provinceOrderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindowBean>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindowBean tradeProvinceOrderWindowBean, long l) {
                        return tradeProvinceOrderWindowBean.getTs();
                    }
                }));

        // todo 7. 分组、开窗、聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> reduceDS = tradeProvinceWithWmDS.keyBy(TradeProvinceOrderWindowBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindowBean>() {
                    @Override
                    public TradeProvinceOrderWindowBean reduce(TradeProvinceOrderWindowBean t, TradeProvinceOrderWindowBean t1) throws Exception {
                        t.getOrderIdSet().addAll(t1.getOrderIdSet());
                        t.setOrderAmount(t.getOrderAmount() + t1.getOrderAmount());
                        return t;
                    }
                }, new WindowFunction<TradeProvinceOrderWindowBean, TradeProvinceOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindowBean> input, Collector<TradeProvinceOrderWindowBean> out) throws Exception {
                        TradeProvinceOrderWindowBean next = input.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(next);
                    }
                });

        reduceDS.print("reduceDS>>>>>>>>>>");

        // todo 8. 关联省份维表补充省份名称字段
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> reduceWithProvinceDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeProvinceOrderWindowBean>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(TradeProvinceOrderWindowBean input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindowBean input, JSONObject dimInfo) {
                        input.setProvinceName(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        // todo 9. 将数据写出到ClickHouse
        reduceWithProvinceDS.print("》》》》》》》");
        reduceWithProvinceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        // todo 10. 启动任务
        env.execute("DwsTradeProvinceOrderWindow");
    }
}

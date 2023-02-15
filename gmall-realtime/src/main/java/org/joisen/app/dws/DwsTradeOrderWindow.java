package org.joisen.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.joisen.bean.TradeOrderBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/15 20:51
 * @Version 1.0
 */
// 数据流： Web/App  -> nginx  -> 业务服务器（MySQL） ->  Maxwell  -> Kafka(ODS)  ->  FlinkApp  ->  Kafka(DWD)  -> FlinkApp  -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程序：      Mock -> Mysql  -> Maxwell  ->  Kafka(ZK)  ->  DwdTradeOrderPreProcess  -> Kafka(ZK)  ->  DwdTradeOrderDetail  ->  Kafka(ZK) -> DwsTradeOrderWindow -> ClickHouse(ZK)
public class DwsTradeOrderWindow {
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

        // todo 2. 读取kafka dwd层下单主题数据创建流
        String topic = "dwd_trade_order_detail_0105";
        String groupId = "dws_trade_order_window_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 将每行数据转换为JSON对象
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

        // todo 4. 按照order_detail_id 分组
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));

        // todo 5. 针对order_detail_id 进行去重（保留第一条数据即可）
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("is-exists", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 获取状态数据
                String state = valueState.value();
                // 判断状态是否为null
                if (state == null) {
                    valueState.update("1");
                    return true;
                }
                return false;
            }
        });

        // todo 6. 提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return DateFormatUtil.toTs(jsonObject.getString("create_time"), true);
                    }
                }));

        // todo 7. 按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWmDS.keyBy(json -> json.getString("user_id"));

        // todo 8. 提取独立下单用户
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDS = keyedByUidDS.map(new RichMapFunction<JSONObject, TradeOrderBean>() {
            private ValueState<String> lastOrderDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order", String.class));
            }

            @Override
            public TradeOrderBean map(JSONObject value) throws Exception {
                // 获取状态中以及当前数据中的日期
                String lastOrderDt = lastOrderDtState.value();
                String curDt = value.getString("create_time").split(" ")[0];
                // 定义当天下单人数以及新增下单人数
                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;
                // 判断状态是否为Null
                if (lastOrderDt == null) {
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                    lastOrderDtState.update(curDt);
                } else if (!lastOrderDt.equals(curDt)) {
                    orderUniqueUserCount = 1L;
                    lastOrderDtState.update(curDt);
                }
                // 取出下单件数以及单价
                Integer skuNum = value.getInteger("sku_num");
                Double orderPrice = value.getDouble("order_price");
                Double splitActivityAmount = value.getDouble("split_activity_amount");
                if (splitActivityAmount==null) {
                    splitActivityAmount = 0.0D;
                }
                Double splitCouponAmount = value.getDouble("split_coupon_amount");
                if (splitCouponAmount == null) {
                    splitCouponAmount = 0.0D;
                }

                return new TradeOrderBean("",
                        "",
                        orderUniqueUserCount,
                        orderNewUserCount,
                        splitActivityAmount,
                        splitCouponAmount,
                        skuNum * orderPrice,
                        null);
            }
        });

        // todo 9. 开窗、聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDS = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean t1, TradeOrderBean t2) throws Exception {
                        t1.setOrderUniqueUserCount(t1.getOrderUniqueUserCount() + t2.getOrderUniqueUserCount());
                        t1.setOrderNewUserCount(t1.getOrderNewUserCount() + t2.getOrderNewUserCount());
                        t1.setOrderActivityReduceAmount(t1.getOrderActivityReduceAmount() + t2.getOrderActivityReduceAmount());
                        t1.setOrderOriginalTotalAmount(t1.getOrderOriginalTotalAmount() + t2.getOrderOriginalTotalAmount());
                        t1.setOrderCouponReduceAmount(t1.getOrderCouponReduceAmount() + t2.getOrderCouponReduceAmount());
                        return t1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        TradeOrderBean next = values.iterator().next();
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });

        // todo 10. 写入clickhouse
        resultDS.print(">>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));

        // todo 11. 启动任务
        env.execute("DwsTradeOrderWindow");
    }
}

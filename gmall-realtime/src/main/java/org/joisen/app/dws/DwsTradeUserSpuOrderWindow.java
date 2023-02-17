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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joisen.app.func.DimAsyncFunction;
import org.joisen.bean.TradeUserSpuOrderBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author Joisen
 * @Date 2023/2/16 14:45
 * @Version 1.0
 */

// 数据流： Web/App -> nginx -> 业务服务器（MySQL）-> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)  -> FlinkApp -> Kafka(DWD) -> flinkApp -> ClickHouse(DWS)
// 程序：    Mock -> Mysql -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwsTradeUserSpuOrderWindow(Phoenix(hbase-hdfs-zk)、redis) -> ClickHouse(ZK)
public class DwsTradeUserSpuOrderWindow {
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
        String groupId = "dws_trade_user_spu_order_window_0105";
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

        // todo 4. 按照订单明细id分组
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));

        // todo 5. 去重
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
                } else {
                    return false;
                }
            }
        });

        // todo 6. 将数据转换为JavaBean对象
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuDS = filterDS.map(jsonObject -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));
            return TradeUserSpuOrderBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderAmount(jsonObject.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });

//        tradeUserSpuDS.print("tradeUserSpuDS<<<<<<<<");

        // todo 7. 关联sku_info维表 补充 spu_id,tm_id,category3_id
//        tradeUserSpuDS.map(new RichMapFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean>() {
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                // 创建Phoenix连接池
//            }
//
//            @Override
//            public TradeUserSpuOrderBean map(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {
//                // 查询维表，将查到的信息补充到JavaBean中
//                return null;
//            }
//        });

        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithSkuDS = AsyncDataStream.unorderedWait(tradeUserSpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setSpuId(dimInfo.getString("SPU_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                },
                100, // 要大于访问第三方数据库查询的最大连接时间，此处是Phoenix，访问连接（ZK）时间是60s
                TimeUnit.SECONDS);

//        tradeUserSpuWithSkuDS.print("tradeUserSpuWithSkuDS>>>>>>>>");

        // todo 8. 提取事件时间生成Watermark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithWmDS = tradeUserSpuWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
                    @Override
                    public long extractTimestamp(TradeUserSpuOrderBean tradeUserSpuOrderBean, long l) {
                        return tradeUserSpuOrderBean.getTs();
                    }
                }));

        // todo 9. 分组开窗聚合
        KeyedStream<TradeUserSpuOrderBean, Tuple4<String, String, String, String>> keyedStream = tradeUserSpuWithWmDS.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean value) throws Exception {
                return new Tuple4<>(value.getUserId(),
                        value.getSpuId(),
                        value.getTrademarkId(),
                        value.getCategory3Id());
            }
        });

        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean value1, TradeUserSpuOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TradeUserSpuOrderBean> input, Collector<TradeUserSpuOrderBean> out) throws Exception {

                        TradeUserSpuOrderBean next = input.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(next);
                    }
                });

        // todo 10. 关联spu，tm，category维表 补充相应信息
        // 10.1 关联spu
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithSpuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setSpuName(dimInfo.getString("SPU_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        // 10.2 关联tm
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithTmDS = AsyncDataStream.unorderedWait(reduceWithSpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        // 10.3 关联category3
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory3DS = AsyncDataStream.unorderedWait(reduceWithTmDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        // 10.4 关联category2
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory2DS = AsyncDataStream.unorderedWait(reduceWithCategory3DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        // 10.5 关联category1
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory1DS = AsyncDataStream.unorderedWait(reduceWithCategory2DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        // todo 11. 将数据写出到ClickHouse
        reduceWithCategory1DS.print("》》》》》》");
        reduceWithCategory1DS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // todo 12. 启动任务
        env.execute("DwsTradeUserSpuOrderWindow");


    }
}

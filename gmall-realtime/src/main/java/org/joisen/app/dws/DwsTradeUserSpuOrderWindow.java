package org.joisen.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.joisen.bean.TradeUserSpuOrderBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyKafkaUtil;

import java.util.HashSet;

/**
 * @Author Joisen
 * @Date 2023/2/16 14:45
 * @Version 1.0
 */
public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) {
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

        // todo 7. 关联sku_info维表 补充 spu_id,tm_id,category3_id
        tradeUserSpuDS.map(new RichMapFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建Phoenix连接池
            }

            @Override
            public TradeUserSpuOrderBean map(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {
                // 查询维表，将查到的信息补充到JavaBean中
                return null;
            }
        });

        // todo 8. 提取事件时间生成Watermark

        // todo 9. 分组开窗聚合

        // todo 10. 关联spu，tm，category维表 补充相应信息

        // todo 11. 将数据写出到ClickHouse

        // todo 12. 启动任务



    }
}

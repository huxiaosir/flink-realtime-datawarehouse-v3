package org.joisen.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.joisen.bean.TrafficHomeDetailPageViewBean;
import org.joisen.utils.DateFormatUtil;
import org.joisen.utils.MyClickHouseUtil;
import org.joisen.utils.MyKafkaUtil;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/9 11:40
 * @Version 1.0
 */
// 数据流： web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程  序：      Mock(log_0105.sh) -> Flume(flume1_0105.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsTrafficPageViewWindow -> ClickHouse(ZK)
public class DwsTrafficPageViewWindow {
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

        // todo 2. 读取Kafka页面日志主题数据创建流
        String topic = "dwd_traffic_page_log_0105";
        String groupId = "dws_traffic_page_view_window_0105";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 3. 将每行数据转换为JSON对象并过滤
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                // 获取json对象
                JSONObject jsonObject = JSON.parseObject(s);
                // 获取当前页面id
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                // 过滤出首页与商品详情页的数据
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
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

        // todo 5. 按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        // todo 6. 使用状态编程过滤出首页与商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastState;
            private ValueState<String> detailLastState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> detailStateDes = new ValueStateDescriptor<>("detail-state", String.class);

                // 设置TTL
                homeStateDes.enableTimeToLive(ttlConfig);
                detailStateDes.enableTimeToLive(ttlConfig);

                homeLastState = getRuntimeContext().getState(homeStateDes);
                detailLastState = getRuntimeContext().getState(detailStateDes);

            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {

                // 获取状态数据以及当前数据中的日期
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                String homeLastDt = homeLastState.value();
                String detailLastDt = detailLastState.value();

                // 定义访问首页或者详情页的数据
                long homeCt = 0L;
                long detailCt = 0L;

                // 如果状态为空或者状态时间与当前时间不同，则为需要的数据
                if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                    homeCt = 1L;
                    homeLastState.update(curDt);
                }
                if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                    detailCt = 1L;
                    detailLastState.update(curDt);
                }
                // 满足任何一个数据不等于0，则可以写出
                if (homeCt == 1L || detailCt == 1L) {
                    collector.collect(new TrafficHomeDetailPageViewBean("", "",
                            homeCt, detailCt, ts));
                }

            }
        });

        // todo 7. 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        // 获取数据
                        TrafficHomeDetailPageViewBean next = values.iterator().next();
                        // 补充字段
                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        // 输出数据
                        out.collect(next);

                    }
                });

        // todo 8. 将数据写出到ClickHouse
        resultDS.print(">>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        // todo 9. 启动任务
        env.execute("DwsTrafficPageViewWindow");

    }
}

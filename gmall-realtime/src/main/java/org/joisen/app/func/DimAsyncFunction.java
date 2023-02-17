package org.joisen.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.joisen.utils.DimUtil;
import org.joisen.utils.DruidDSUtil;
import org.joisen.utils.ThreadPoolUtil;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author Joisen
 * @Date 2023/2/17 10:38
 * @Version 1.0
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private DruidDataSource dataSource;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction() {
    }
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {

                try {
                    // 获取连接
                    DruidPooledConnection connection = dataSource.getConnection();

                    // 查询维表 获取维度信息
                    String key = getKey(input);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                    // 将维度信息补充至当前数据
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }

                    // 归还连接
                    connection.close();

                    // 将结果写出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("关联维表失败：" + input + ", Table: " + tableName);
//                    resultFuture.complete(Collections.singletonList(input));
                }
            }
        });

    }




    /**
     * {@link AsyncFunction#asyncInvoke} timeout occurred. By default, the result future is
     * exceptionally completed with a timeout exception.
     *
     * @param input        element coming from an upstream task
     * @param resultFuture to be completed with the result data
     */
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("Timeout: " + input);
    }
}

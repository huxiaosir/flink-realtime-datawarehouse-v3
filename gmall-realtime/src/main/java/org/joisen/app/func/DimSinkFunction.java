package org.joisen.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.joisen.utils.DruidDSUtil;
import org.joisen.utils.PhoenixUtil;

/**
 * @Author Joisen
 * @Date 2023/2/2 17:54
 * @Version 1.0
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    // value： {"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,
    // "commit":true,"data":{"id":13,"tm_name":"atguigu"},"old":{"logo_url":"/aaa/aaa"},"sinkTable":"dim_xxx"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        // 获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        // 写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        // 出现异常不进行捕获，直接抛出异常，程序终止运行
        PhoenixUtil.upsertValues(connection, sinkTable, data);

        // 归还连接  连接池的close操作并不是关闭连接而是归还连接
        connection.close();

    }
}

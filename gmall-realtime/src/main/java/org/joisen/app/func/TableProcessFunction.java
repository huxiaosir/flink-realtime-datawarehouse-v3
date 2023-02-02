package org.joisen.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.joisen.bean.TableProcess;
import org.joisen.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @Author Joisen
 * @Date 2023/2/2 15:04
 * @Version 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    // 处理广播流数据
    /**
     * {"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"dd","sink_extend":"ee"},
     * "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1675322241927,"snapshot":"false",
     * "db":"gmall_config_0105","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
     * "op":"r","ts_ms":1675322241930,"transaction":null}
     */
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 1. 获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // 2. 校验并建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPK(),
                tableProcess.getSinkExtend());

        // 3. 写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);

    }

    /**
     * 校验并建表 ： create table if not exists db.tn(id varchar primary key, bb varchar, cc varchar) xxx;
     * @param sinkTable Phoenix表名
     * @param sinkColumns Phoenix表字段
     * @param sinkPK Phoenix表主键
     * @param sinkExtend Phoenix表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPK, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            // 处理特殊字段
            if (sinkPK == null || "".equals(sinkPK)){
                sinkPK = "id";
            }
            if (sinkExtend == null){
                sinkExtend = "";
            }
            // 拼接SQL： create table if not exists db.tn(id varchar primary key, bb varchar, cc varchar) xxx;
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                // 取出字段
                String column = columns[i];

                // 判断是否为主键
                if(sinkPK.equals(column)){
                    createTableSql.append(column).append(" varchar primary key");
                }else{
                    createTableSql.append(column).append(" varchar");
                }
                // 判断是否为最后一个字段
                if(i < columns.length - 1){
                    createTableSql.append(",");
                }
            }
            createTableSql.append(")").append(sinkExtend);
            // 编译SQL
            System.out.println("建表语句为： " + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            // 执行SQL、建表
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("建表失败： " + sinkTable); // 捕获到异常后直接终止程序运行
        }finally {
            // 释放资源
            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    // 处理主流数据
    // value： {"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,
    // "commit":true,"data":{"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"},"old":{"logo_url":"/aaa/aaa"}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1. 获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table = value.getString("table");
        TableProcess tableProcess = broadcastState.get(table); // 当table为事实表时，tableProcess为null

        if(tableProcess != null){
            // 2. 过滤字段
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            // 3. 补充SinkTable并写出到流中
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);
            
        }else {
            System.out.println("找不到对应的Key： " + table);
        }
    }

    /**
     * 过滤字段
     * @param data ： {"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"}
     * @param sinkColumns ： "id,tm_name"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while(iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if ( !sinkColumns.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        // 切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnsList.contains(next.getKey()));

    }

}

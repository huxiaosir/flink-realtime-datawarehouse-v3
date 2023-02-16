package org.joisen.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.joisen.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author Joisen
 * @Date 2023/2/16 16:41
 * @Version 1.0
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {

        // todo 先查询redis
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM_0105:" + tableName + ":" + key;
        String dimJsonStr = jedis.get(redisKey);
        if (dimJsonStr != null) {
            // 重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            // 归还连接
            jedis.close();
            // 返回维度数据
            return JSON.parseObject(dimJsonStr);
        }

        // 拼接SQL语句
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + key + "'";
        System.out.println("QuerySQL>>>>" + sql);

        // 查询数据
        List<JSONObject> queryList = JdbcUtil.queryList(connection, sql, JSONObject.class, false);

        // todo 将从Phoenix中查询到的数据写入Redis
        JSONObject dimInfo = queryList.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        // 设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);
        // 归还连接
        jedis.close();

        // 返回结果
        return queryList.get(0);
    }

    public static void delDimInfo(String tableName, String key){
        // 获取连接
        Jedis jedis = JedisUtil.getJedis();
        // 删除数据
        jedis.del("DIM_0105:" + tableName + ":" + key); // redis中删除一条不存在的数据是没有问题的
        // 归还连接
        jedis.close();
    }

    public static void main(String[] args) throws Exception {

        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();

        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_TRADEMARK", "15");
        long end = System.currentTimeMillis();
        JSONObject dimInfo1 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "15");
        long end1 = System.currentTimeMillis();

        System.out.println(dimInfo);
        System.out.println(end -start);
        System.out.println(dimInfo1);
        System.out.println(end1 -end);
        connection.close();

    }

}

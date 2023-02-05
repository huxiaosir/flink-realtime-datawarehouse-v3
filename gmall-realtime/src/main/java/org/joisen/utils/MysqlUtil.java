package org.joisen.utils;

/**
 * @Author Joisen
 * @Date 2023/2/5 16:01
 * @Version 1.0
 */
public class MysqlUtil {

    public static String getBaseDicLookUpDDL() {

        return "create table `base_dic`(    " +
                "`dic_code` string,    " +
                "`dic_name` string,    " +
                "`parent_code` string,    " +
                "`create_time` timestamp,    " +
                "`operate_time` timestamp,    " +
                "primary key(`dic_code`) not enforced    " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {

        String ddl = "WITH (    " +
                "'connector' = 'jdbc',    " +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall_0105',    " +
                "'table-name' = '" + tableName + "',    " +
                "'lookup.cache.max-rows' = '10',    " +
                "'lookup.cache.ttl' = '1 hour',    " +
                "'username' = 'root',    " +
                "'password' = '999719',    " +
                "'driver' = 'com.mysql.cj.jdbc.Driver'    " +
                ")";
        return ddl;
    }


}

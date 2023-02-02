package org.joisen.bean;

import lombok.Data;

/**
 * @Author Joisen
 * @Date 2023/2/2 14:51
 * @Version 1.0
 */
@Data
public class TableProcess {
    // 来源表
    String sourceTable;
    // 输出表
    String sinkTable;
    // 输出字段
    String sinkColumns;
    // 主键字段
    String sinkPK;
    // 建表扩展
    String sinkExtend;
}

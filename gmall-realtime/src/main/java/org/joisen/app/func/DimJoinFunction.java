package org.joisen.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @Author Joisen
 * @Date 2023/2/17 15:02
 * @Version 1.0
 */
public interface DimJoinFunction<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo);
}

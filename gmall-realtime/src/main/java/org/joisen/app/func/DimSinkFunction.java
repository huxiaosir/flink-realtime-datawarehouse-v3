package org.joisen.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Author Joisen
 * @Date 2023/2/2 17:54
 * @Version 1.0
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    @Override
    public void open(Configuration parameters) throws Exception {


    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {


    }
}

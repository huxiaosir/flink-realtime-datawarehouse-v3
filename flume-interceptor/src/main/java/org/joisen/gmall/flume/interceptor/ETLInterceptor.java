package org.joisen.gmall.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.joisen.gmall.flume.utils.JSONUtil;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @Author Joisen
 * @Date 2023/1/6 11:27
 * @Version 1.0
 */

/**
 * flume 拦截器 对数据进行简单清洗操作
 * 对数据进行校验，对不完整的json数据进行过滤
 */
public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 1 获取body中的数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        // 2 判断是不是合法的json
        // 3 是： return event 不是：return null
        if (JSONUtil.isJSONValidate(log)){
            return event;
        }else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        // 使用for循环删除list中不合法的数据可能会出现越界异常
//        for (int i = 0; i < list.size(); i++) {
//            Event event = list.get(i);
//            if(intercept(event) == null) {
//                 list.remove(i);
//            }
//        }
        // 使用iterator 没有索引的概念  不会出现越界异常
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()) { // 是否有当前元素
            Event event = iterator.next(); // 取出当前元素， 指针下移
            if(intercept(event) == null){
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}

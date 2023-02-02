package org.joisen.gmall.flume.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;

/**
 * @Author Joisen
 * @Date 2023/1/6 11:36
 * @Version 1.0
 */
public class JSONUtil {
    // 通过异常捕捉， 校验json是不是一个合法的json
    public static boolean isJSONValidate(String log){
        try {
            JSONObject.parseObject(log);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) {
//        System.out.println(JSONObject.parseObject("{id:1}"));
        ArrayList<String> list = new ArrayList<>();
        list.add("a"); // index = 0
        list.add("b"); // index = 1
        list.add("c"); // index = 2
        list.add("d"); // index = 3

        System.out.println(list);

        list.remove(1);
        System.out.println(list);

        list.remove(3);
        System.out.println(list);


    }

}

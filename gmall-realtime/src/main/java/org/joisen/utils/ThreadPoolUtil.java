package org.joisen.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author Joisen
 * @Date 2023/2/17 10:26
 * @Version 1.0
 */
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil(){}

    public static ThreadPoolExecutor getThreadPoolExecutor(){

        // 双层校验 既保证线程安全 又保证效率
        if(threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                if (threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            100,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }

        }

        return threadPoolExecutor;
    }

}

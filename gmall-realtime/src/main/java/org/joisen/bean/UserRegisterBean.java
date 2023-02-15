package org.joisen.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author Joisen
 * @Date 2023/2/9 21:18
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}


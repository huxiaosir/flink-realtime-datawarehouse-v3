package org.joisen.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author Joisen
 * @Date 2023/2/10 11:02
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 加购独立用户数
    Long cartAddUuCt;

    // 时间戳
    Long ts;
}


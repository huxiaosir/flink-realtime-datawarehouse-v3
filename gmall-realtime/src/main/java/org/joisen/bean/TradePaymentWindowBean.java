package org.joisen.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author Joisen
 * @Date 2023/2/15 16:42
 * @Version 1.0
 */
@Data
@AllArgsConstructor
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;

    // 支付成功新用户数
    Long paymentSucNewUserCount;

    // 时间戳
    Long ts;
}


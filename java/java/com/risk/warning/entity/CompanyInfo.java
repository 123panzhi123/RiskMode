package com.risk.warning.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

/**
 * 上市公司基础信息实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompanyInfo {
    private String tsCode;       // 股票代码
    private String symbol;       // 交易代码
    private String name;         // 公司名称
    private String industry;     // 所属行业
    private Date listDate;       // 上市日期
    private Date createdAt;
    private Date updatedAt;
}
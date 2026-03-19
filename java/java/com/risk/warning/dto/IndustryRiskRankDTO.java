package com.risk.warning.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.util.Date;

/**
 * 行业风险排行传输对象 (聚合了公司基本信息与评分)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndustryRiskRankDTO {
    // 公司信息
    private String tsCode;
    private String name;
    private String industry;
    
    // 评分信息
    private Date evalDate;
    private BigDecimal financialScore;
    private BigDecimal marketScore;
    private BigDecimal strategicScore;
    private BigDecimal compositeScore;
}
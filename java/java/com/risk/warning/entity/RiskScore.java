package com.risk.warning.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.util.Date;

/**
 * 风险评分实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RiskScore {
    private Long id;
    private String tsCode;
    private Date evalDate;
    private BigDecimal financialScore;
    private BigDecimal marketScore;
    private BigDecimal strategicScore;
    private BigDecimal compositeScore;
    private Date createdAt;
    private Date updatedAt;
}
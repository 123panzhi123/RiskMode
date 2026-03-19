//package com.risk.warning.dto;
//
//import lombok.Data;
//import java.math.BigDecimal;
//
///**
// * 接收 Python 算法引擎返回的计算结果 DTO
// */
//@Data
//public class RiskEvaluationResultDTO {
//    private String tsCode;
//    private BigDecimal financialScore;
//    private BigDecimal marketScore;
//    private BigDecimal strategicScore;
//    private BigDecimal compositeScore;
//
//    // 如果 Python 侧还会返回错误信息，可以加一个字段
//    // private String errorMessage;
//}

package com.risk.warning.dto;

import java.math.BigDecimal;

public class RiskEvaluationResultDTO {
    private String tsCode;
    private BigDecimal financialScore;
    private BigDecimal marketScore;
    private BigDecimal strategicScore;
    private BigDecimal compositeScore;

    // ======== 下面是手写的 Getter 和 Setter，绝对不会报 406 错误 ========

    public String getTsCode() {
        return tsCode;
    }

    public void setTsCode(String tsCode) {
        this.tsCode = tsCode;
    }

    public BigDecimal getFinancialScore() {
        return financialScore;
    }

    public void setFinancialScore(BigDecimal financialScore) {
        this.financialScore = financialScore;
    }

    public BigDecimal getMarketScore() {
        return marketScore;
    }

    public void setMarketScore(BigDecimal marketScore) {
        this.marketScore = marketScore;
    }

    public BigDecimal getStrategicScore() {
        return strategicScore;
    }

    public void setStrategicScore(BigDecimal strategicScore) {
        this.strategicScore = strategicScore;
    }

    public BigDecimal getCompositeScore() {
        return compositeScore;
    }

    public void setCompositeScore(BigDecimal compositeScore) {
        this.compositeScore = compositeScore;
    }
}
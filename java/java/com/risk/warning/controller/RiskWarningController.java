package com.risk.warning.controller;

import com.risk.warning.dto.IndustryRiskRankDTO;
import com.risk.warning.dto.RiskEvaluationResultDTO;
import com.risk.warning.mapper.RiskScoreMapper;
import com.risk.warning.service.PythonComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * 风险预警系统核心 API 控制器
 * 对外暴露 RESTful 接口供 Vue 前端调用
 */
@RestController
@RequestMapping("/api/risk")
// 极其重要：添加 @CrossOrigin 注解，允许所有本地开发端口（如 Vue 的 5173 或 8080）进行跨域请求
@CrossOrigin(origins = "*", maxAge = 3600)
public class RiskWarningController {

    private static final Logger logger = LoggerFactory.getLogger(RiskWarningController.class);

    private final PythonComputeService pythonComputeService;
    private final RiskScoreMapper riskScoreMapper;

    // 推荐使用构造器注入依赖
    public RiskWarningController(PythonComputeService pythonComputeService, RiskScoreMapper riskScoreMapper) {
        this.pythonComputeService = pythonComputeService;
        this.riskScoreMapper = riskScoreMapper;
    }

    /**
     * 接口 1：触发特定股票的实时风险计算
     * GET /api/risk/evaluate/{ts_code}
     *
     * @param tsCode 股票代码 (例如: 000001.SZ)
     * @return 包含多维风险评分的 JSON 对象
     */
    @GetMapping("/evaluate/{ts_code}")
    public ResponseEntity<RiskEvaluationResultDTO> evaluateRisk(@PathVariable("ts_code") String tsCode) {
        logger.info("接收到前端评估请求，股票代码: {}", tsCode);
        
        // 调用底层跨语言服务执行 Python 脚本
        RiskEvaluationResultDTO result = pythonComputeService.executePythonRiskEvaluation(tsCode);
        
        if (result != null) {
            // HTTP 200 OK，返回计算结果
            return ResponseEntity.ok(result);
        } else {
            // HTTP 500 Internal Server Error，表示底层计算或跨语言通信失败
            logger.error("股票 {} 风险评估失败，无法获取有效结果", tsCode);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * 接口 2：获取特定行业的风险评分排行列表
     * GET /api/risk/industry/{industry}
     *
     * @param industry 行业名称 (例如: 银行, 白酒)
     * @param evalDate 评估日期（可选）。前端不传时，默认采用系统当前日期，或你可以指定一个财报发布的基准日期
     * @return 该行业下所有公司的风险排行数组
     */
    @GetMapping("/industry/{industry}")
    public ResponseEntity<List<IndustryRiskRankDTO>> getIndustryRank(
            @PathVariable("industry") String industry,
            @RequestParam(value = "evalDate", required = false) String evalDate) {
            
        logger.info("接收到获取行业风险排行请求，行业: {}", industry);

        // 容错处理：如果前端没有传入指定的评估日期，则默认使用今天（或根据你的系统设定一个固定的回测日期，如 "2023-12-31"）
        if (evalDate == null || evalDate.trim().isEmpty()) {
            evalDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            // 如果你的数据库里只有特定历史日期的数据，建议在这里把 evalDate 写死，比如：evalDate = "2023-12-31";
        }

        // 调用 MyBatis Mapper 执行我们在步骤 4.1 中写好的极致优化的联表查询
        List<IndustryRiskRankDTO> rankList = riskScoreMapper.selectRiskRankByIndustry(industry, evalDate);
        
        if (rankList != null && !rankList.isEmpty()) {
            return ResponseEntity.ok(rankList);
        } else {
            // HTTP 204 No Content，表示请求成功但该行业在特定日期下没有数据
            return ResponseEntity.noContent().build();
        }
    }
}
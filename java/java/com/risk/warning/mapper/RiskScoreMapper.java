package com.risk.warning.mapper;

import com.risk.warning.dto.IndustryRiskRankDTO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface RiskScoreMapper {

    /**
     * 查询特定日期下，特定行业内所有公司的风险评分，并按综合评分降序排列
     *
     * @param industry 行业名称
     * @param evalDate 评估日期 (格式: yyyy-MM-dd)
     * @return 行业内公司的风险排行列表
     */
    List<IndustryRiskRankDTO> selectRiskRankByIndustry(
            @Param("industry") String industry,
            @Param("evalDate") String evalDate
    );
}
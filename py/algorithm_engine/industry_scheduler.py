import pandas as pd
import numpy as np
import logging
from sqlalchemy import Engine
from entropy_topsis import EntropyTOPSIS

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# 三个维度的正负向指标定义（与数据库 DDL 注释一致）
DIMENSION_CONFIG = {
    'financial': {
        'positive': ['current_ratio', 'quick_ratio', 'roe', 'gross_margin'],
        'negative': ['invturn_days', 'arturn_days']
    },
    'market': {
        'positive': [],
        'negative': ['turnover_rate', 'volume_ratio']
    },
    'strategic': {
        'positive': ['revenue_growth_rate', 'net_profit_growth_rate', 'rd_expense_ratio'],
        'negative': ['goodwill_ratio']
    }
}


def _calculate_single_dimension(group_data: pd.DataFrame, pos_cols: list, neg_cols: list) -> pd.DataFrame:
    """对单个行业子集计算单个维度的 TOPSIS 得分"""
    available_pos = [c for c in pos_cols if c in group_data.columns]
    available_neg = [c for c in neg_cols if c in group_data.columns]

    if not available_pos and not available_neg:
        return pd.DataFrame({'ts_code': group_data['ts_code'], 'score': 0.5})

    model = EntropyTOPSIS(df=group_data, positive_cols=available_pos, negative_cols=available_neg)
    result = model.calculate_comprehensive_score()
    result = result.rename(columns={'composite_score': 'score'})
    return result


def execute_industry_dynamic_scoring(merged_df: pd.DataFrame, company_info_df: pd.DataFrame, db_engine: Engine,
                                     eval_date: str):
    """
    按行业动态分组执行熵权-TOPSIS风险评分，独立计算三个维度得分，并将结果持久化到MySQL

    :param merged_df: 包含所有已清洗对齐的财务、市场、战略指标的 DataFrame
    :param company_info_df: 包含公司基础信息（特别是行业 industry 字段）的 DataFrame
    :param db_engine: SQLAlchemy 数据库连接引擎
    :param eval_date: 评估日期 (用于记录到 risk_scores 表)
    """
    logging.info("开始执行按行业动态分组的三维独立风险评分计算...")

    # 1. 数据关联：通过股票代码将指标数据与行业信息进行关联
    if 'industry' not in merged_df.columns:
        analysis_df = pd.merge(merged_df, company_info_df[['ts_code', 'industry']], on='ts_code', how='inner')
    else:
        analysis_df = merged_df.copy()

    # 填充数值列缺失值为列均值
    numeric_cols = analysis_df.select_dtypes(include=[np.number]).columns
    analysis_df[numeric_cols] = analysis_df[numeric_cols].fillna(analysis_df[numeric_cols].mean())

    all_results = []

    # 2. 按行业分组
    grouped = analysis_df.groupby('industry')

    for industry_name, group_data in grouped:
        if len(group_data) < 3:
            logging.warning(f"行业 [{industry_name}] 样本数不足 ({len(group_data)}家)，跳过。")
            continue

        logging.info(f"正在计算行业 [{industry_name}] 的三维风险评分，样本数: {len(group_data)}")

        try:
            # 3. 分别计算三个维度的得分
            fin_result = _calculate_single_dimension(
                group_data, DIMENSION_CONFIG['financial']['positive'], DIMENSION_CONFIG['financial']['negative']
            )
            mkt_result = _calculate_single_dimension(
                group_data, DIMENSION_CONFIG['market']['positive'], DIMENSION_CONFIG['market']['negative']
            )
            str_result = _calculate_single_dimension(
                group_data, DIMENSION_CONFIG['strategic']['positive'], DIMENSION_CONFIG['strategic']['negative']
            )

            # 4. 合并三个维度的得分
            dim_df = fin_result.rename(columns={'score': 'financial_score'})
            dim_df = dim_df.merge(mkt_result.rename(columns={'score': 'market_score'}), on='ts_code', how='outer')
            dim_df = dim_df.merge(str_result.rename(columns={'score': 'strategic_score'}), on='ts_code', how='outer')

            # 5. 计算综合得分（加权平均：财务 0.4，市场 0.3，战略 0.3）
            dim_df['financial_score'] = dim_df['financial_score'].fillna(0.5)
            dim_df['market_score'] = dim_df['market_score'].fillna(0.5)
            dim_df['strategic_score'] = dim_df['strategic_score'].fillna(0.5)
            dim_df['composite_score'] = (
                dim_df['financial_score'] * 0.4 +
                dim_df['market_score'] * 0.3 +
                dim_df['strategic_score'] * 0.3
            )

            all_results.append(dim_df)

        except Exception as e:
            logging.error(f"计算行业 [{industry_name}] 时发生异常: {str(e)}")
            continue

    # 6. 合并所有行业结果
    if not all_results:
        logging.error("所有行业计算均失败或无有效数据，程序退出。")
        return

    final_score_df = pd.concat(all_results, ignore_index=True)
    final_score_df['eval_date'] = eval_date

    # 7. 持久化到 MySQL
    try:
        # 只保留 risk_scores 表需要的列
        db_columns = ['ts_code', 'eval_date', 'financial_score', 'market_score', 'strategic_score', 'composite_score']
        final_score_df[db_columns].to_sql(
            name='risk_scores',
            con=db_engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        logging.info(f"成功将 {len(final_score_df)} 条三维风险评分数据入库至 risk_scores 表。")
    except Exception as e:
        logging.error(f"数据入库失败: {str(e)}")
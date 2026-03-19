"""
上市公司多维风险画像与预警系统 - Python 算法引擎入口
被 Java 端 PythonComputeService 通过 ProcessBuilder 调用
参数: --ts_code 股票代码
输出: 仅一行纯净 JSON 字符串（供 Java Jackson 反序列化）
"""

import os
import argparse
import json
import sys
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# ============================================================
# 关键：将当前脚本所在目录加入 sys.path
# 解决 Java ProcessBuilder 启动时工作目录不同导致 import 失败的问题
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# ============================================================
# 关键：所有日志输出到 stderr，只有最终 JSON 结果输出到 stdout
# 这样 Java 端 getInputStream() 只会读到纯净的 JSON
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]  # 日志走 stderr
)
logger = logging.getLogger(__name__)

# 数据库配置（与 data_acquisition/config.py 保持一致）
DB_USER = 'root'
DB_PASS = '123'
DB_HOST = '127.0.0.1'
DB_PORT = '3306'
DB_NAME = 'risk_warning_system'
DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"


def get_latest_indicators(ts_code: str, engine) -> dict:
    """
    从数据库获取目标股票的最新一期各维度指标数据
    返回包含财务、市场、战略三个维度原始指标的字典
    """
    # 获取最新一期财务指标
    query_fin = f"""
        SELECT current_ratio, quick_ratio, roe, gross_margin, invturn_days, arturn_days
        FROM financial_indicators
        WHERE ts_code = '{ts_code}'
        ORDER BY end_date DESC
        LIMIT 1
    """
    df_fin = pd.read_sql(query_fin, con=engine)

    # 获取最新一期市场指标
    query_mkt = f"""
        SELECT turnover_rate, volume_ratio, pe
        FROM market_indicators
        WHERE ts_code = '{ts_code}'
        ORDER BY trade_date DESC
        LIMIT 1
    """
    df_mkt = pd.read_sql(query_mkt, con=engine)

    # 获取最新一期战略指标
    query_str = f"""
        SELECT revenue_growth_rate, net_profit_growth_rate, rd_expense_ratio, goodwill_ratio
        FROM strategic_indicators
        WHERE ts_code = '{ts_code}'
        ORDER BY report_period DESC
        LIMIT 1
    """
    df_str = pd.read_sql(query_str, con=engine)

    return {
        'financial': df_fin if not df_fin.empty else None,
        'market': df_mkt if not df_mkt.empty else None,
        'strategic': df_str if not df_str.empty else None
    }


def get_industry_data(ts_code: str, engine) -> pd.DataFrame:
    """
    获取目标股票所在行业的全部公司最新指标数据（用于行业内横向对比计算 TOPSIS）
    """
    # 先查出目标公司所属行业
    query_industry = f"SELECT industry FROM company_info WHERE ts_code = '{ts_code}'"
    df_ind = pd.read_sql(query_industry, con=engine)
    if df_ind.empty:
        return pd.DataFrame()

    industry = df_ind.iloc[0]['industry']
    logger.info(f"目标公司所属行业: {industry}")

    # 获取该行业所有公司代码
    query_codes = f"SELECT ts_code FROM company_info WHERE industry = '{industry}'"
    df_codes = pd.read_sql(query_codes, con=engine)
    if df_codes.empty:
        return pd.DataFrame()

    codes = df_codes['ts_code'].tolist()
    logger.info(f"行业内共 {len(codes)} 家公司")

    all_rows = []
    for code in codes:
        try:
            row = {'ts_code': code}
            has_any_data = False

            # 财务指标（最新一期）
            q_fin = f"""
                SELECT current_ratio, quick_ratio, roe, gross_margin, invturn_days, arturn_days
                FROM financial_indicators WHERE ts_code = '{code}'
                ORDER BY end_date DESC LIMIT 1
            """
            df_f = pd.read_sql(q_fin, con=engine)
            if not df_f.empty:
                row.update(df_f.iloc[0].to_dict())
                has_any_data = True

            # 市场指标（最新一期）
            q_mkt = f"""
                SELECT turnover_rate, volume_ratio, pe
                FROM market_indicators WHERE ts_code = '{code}'
                ORDER BY trade_date DESC LIMIT 1
            """
            df_m = pd.read_sql(q_mkt, con=engine)
            if not df_m.empty:
                row.update(df_m.iloc[0].to_dict())
                has_any_data = True

            # 战略指标（最新一期）
            q_str = f"""
                SELECT revenue_growth_rate, net_profit_growth_rate, rd_expense_ratio, goodwill_ratio
                FROM strategic_indicators WHERE ts_code = '{code}'
                ORDER BY report_period DESC LIMIT 1
            """
            df_s = pd.read_sql(q_str, con=engine)
            if not df_s.empty:
                row.update(df_s.iloc[0].to_dict())
                has_any_data = True

            # 只要有任意一张表的数据就纳入计算（缺失值后续用均值填充）
            if has_any_data:
                all_rows.append(row)
        except Exception as e:
            logger.warning(f"获取 {code} 数据时出错: {e}")
            continue

    if not all_rows:
        return pd.DataFrame()

    return pd.DataFrame(all_rows)


def calculate_dimension_scores(industry_df: pd.DataFrame, ts_code: str) -> dict:
    """
    使用熵权-TOPSIS方法分别计算三个维度的得分和综合得分
    """
    from entropy_topsis import EntropyTOPSIS

    # 定义三个维度的正负向指标
    dimensions = {
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

    scores = {}

    for dim_name, cols_config in dimensions.items():
        pos_cols = [c for c in cols_config['positive'] if c in industry_df.columns]
        neg_cols = [c for c in cols_config['negative'] if c in industry_df.columns]

        if not pos_cols and not neg_cols:
            scores[dim_name] = 0.5  # 无数据时给中间值
            continue

        try:
            # 检查该维度的指标列是否有足够的数据差异（非全 NaN 或全相同值）
            dim_cols = pos_cols + neg_cols
            dim_data = industry_df[dim_cols].dropna(how='all')
            if len(dim_data) < 2:
                logger.warning(f"{dim_name} 维度有效数据不足 2 条，使用默认分 0.5")
                scores[dim_name] = 0.5
                continue

            # 检查是否所有值都相同（方差为 0），此时 TOPSIS 无法区分
            has_variance = False
            for col in dim_cols:
                if col in industry_df.columns and industry_df[col].nunique() > 1:
                    has_variance = True
                    break
            if not has_variance:
                logger.warning(f"{dim_name} 维度所有指标方差为 0，使用默认分 0.5")
                scores[dim_name] = 0.5
                continue

            model = EntropyTOPSIS(
                df=industry_df,
                positive_cols=pos_cols,
                negative_cols=neg_cols
            )
            result_df = model.calculate_comprehensive_score()
            # 找到目标公司的得分
            target_row = result_df[result_df['ts_code'] == ts_code]
            if not target_row.empty:
                score_val = float(target_row.iloc[0]['composite_score'])
                # 防止 NaN 或异常值
                if np.isnan(score_val) or np.isinf(score_val):
                    scores[dim_name] = 0.5
                else:
                    scores[dim_name] = score_val
            else:
                scores[dim_name] = 0.5
        except Exception as e:
            logger.warning(f"计算 {dim_name} 维度得分时出错: {e}")
            scores[dim_name] = 0.5

    # 综合得分：三个维度的加权平均（财务 0.4，市场 0.3，战略 0.3）
    composite = (
        scores.get('financial', 0.5) * 0.4 +
        scores.get('market', 0.5) * 0.3 +
        scores.get('strategic', 0.5) * 0.3
    )
    scores['composite'] = composite

    return scores


def save_to_risk_scores(engine, ts_code: str, result: dict):
    """
    将评估结果写入 risk_scores 表
    """
    from datetime import date
    try:
        eval_date = date.today().strftime('%Y-%m-%d')
        score_df = pd.DataFrame([{
            'ts_code': ts_code,
            'eval_date': eval_date,
            'financial_score': result['financialScore'],
            'market_score': result['marketScore'],
            'strategic_score': result['strategicScore'],
            'composite_score': result['compositeScore']
        }])

        # 使用 INSERT IGNORE 语义：如果当天已有记录则跳过（利用唯一键约束）
        from sqlalchemy import text
        with engine.connect() as conn:
            # 先检查是否已存在
            check = conn.execute(
                text("SELECT id FROM risk_scores WHERE ts_code = :tc AND eval_date = :ed"),
                {"tc": ts_code, "ed": eval_date}
            ).fetchone()
            if check:
                # 更新已有记录
                conn.execute(
                    text("""UPDATE risk_scores SET financial_score=:fs, market_score=:ms,
                            strategic_score=:ss, composite_score=:cs
                            WHERE ts_code=:tc AND eval_date=:ed"""),
                    {"fs": result['financialScore'], "ms": result['marketScore'],
                     "ss": result['strategicScore'], "cs": result['compositeScore'],
                     "tc": ts_code, "ed": eval_date}
                )
            else:
                # 插入新记录
                score_df.to_sql('risk_scores', con=engine, if_exists='append', index=False)
            conn.commit()
        logger.info(f"评分结果已写入 risk_scores 表 (ts_code={ts_code}, eval_date={eval_date})")
    except Exception as e:
        logger.warning(f"写入 risk_scores 表失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="上市公司多维风险画像计算引擎")
    parser.add_argument("--ts_code", type=str, required=True, help="目标股票代码")
    args = parser.parse_args()

    ts_code = args.ts_code
    logger.info(f"开始评估股票: {ts_code}")

    engine = None
    try:
        engine = create_engine(DB_URI, pool_recycle=3600)

        # 先检查该股票是否存在于数据库
        check_query = f"SELECT ts_code FROM company_info WHERE ts_code = '{ts_code}'"
        df_check = pd.read_sql(check_query, con=engine)
        if df_check.empty:
            result = {
                "tsCode": ts_code,
                "financialScore": 0.0,
                "marketScore": 0.0,
                "strategicScore": 0.0,
                "compositeScore": 0.0
            }
            print(json.dumps(result))
            return

        # 获取行业内全部公司数据
        industry_df = get_industry_data(ts_code, engine)

        if industry_df.empty or len(industry_df) < 3:
            logger.warning("行业数据不足，使用单公司简化评估")
            indicators = get_latest_indicators(ts_code, engine)

            fin_score = 0.5 if indicators['financial'] is not None else 0.0
            mkt_score = 0.5 if indicators['market'] is not None else 0.0
            str_score = 0.5 if indicators['strategic'] is not None else 0.0
            comp_score = fin_score * 0.4 + mkt_score * 0.3 + str_score * 0.3

            result = {
                "tsCode": ts_code,
                "financialScore": round(fin_score, 4),
                "marketScore": round(mkt_score, 4),
                "strategicScore": round(str_score, 4),
                "compositeScore": round(comp_score, 4)
            }
        else:
            # 正常流程：行业内横向对比，熵权-TOPSIS 计算
            numeric_cols = industry_df.select_dtypes(include=[np.number]).columns
            industry_df[numeric_cols] = industry_df[numeric_cols].fillna(industry_df[numeric_cols].mean())

            scores = calculate_dimension_scores(industry_df, ts_code)

            result = {
                "tsCode": ts_code,
                "financialScore": round(scores.get('financial', 0.5), 4),
                "marketScore": round(scores.get('market', 0.5), 4),
                "strategicScore": round(scores.get('strategic', 0.5), 4),
                "compositeScore": round(scores.get('composite', 0.5), 4)
            }

        logger.info(f"评估完成: {result}")

        # 将结果写入 risk_scores 表
        save_to_risk_scores(engine, ts_code, result)

        # 极其重要：只有这一行 print 输出到 stdout，供 Java 端 Jackson 解析
        print(json.dumps(result))

    except Exception as e:
        logger.error(f"评估过程发生异常: {e}", exc_info=True)
        fallback = {
            "tsCode": ts_code,
            "financialScore": 0.0,
            "marketScore": 0.0,
            "strategicScore": 0.0,
            "compositeScore": 0.0
        }
        print(json.dumps(fallback))
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()
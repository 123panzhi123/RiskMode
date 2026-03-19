"""
==============================================================================
上市公司多维风险画像与预警系统
阶段二：多源异构数据融合与预处理管道
步骤 2.3：时间序列对齐与未来函数消除

功能描述：
从 MySQL 读取高频市场行情与低频财务数据，使用 merge_asof 严格按时间序列
向后匹配最近的一期财务数据，利用 ffill 消除空值，彻底杜绝“未来函数”。
最终输出无缺失值的扁平化 DataFrame，供下游风险评估模型使用。
==============================================================================
"""

import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from config import TUSHARE_TOKEN, DB_URI

# ==============================================================================
# 1. 全局配置与日志初始化
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# 数据库连接配置 (请保持与前序步骤一致)
# DB_URI = "mysql+pymysql://root:your_password@127.0.0.1:3306/risk_warning_system?charset=utf8mb4"


# ==============================================================================
# 2. 核心数据对齐函数
# ==============================================================================
def align_heterogeneous_data(ts_code: str) -> pd.DataFrame:
    """
    将特定股票的日频市场数据与季/年频财务数据进行时间序列对齐。

    :param ts_code: 股票代码 (如 '000001.SZ')
    :return: 经过频率对齐且无空值的 DataFrame，直接可喂给后续的熵权-TOPSIS模型
    """
    logger.info(f"[{ts_code}] 开始执行多源异构数据频率对齐与未来函数消除...")
    engine = create_engine(DB_URI, pool_recycle=3600)

    try:
        # 1. 从 MySQL 读取高频市场数据 (market_indicators)
        query_market = f"""
            SELECT ts_code, trade_date, turnover_rate, volume_ratio, pe
            FROM market_indicators
            WHERE ts_code = '{ts_code}'
        """
        df_market = pd.read_sql(query_market, con=engine)

        # 2. 从 MySQL 读取低频财务数据 (financial_indicators)
        query_finance = f"""
            SELECT ts_code, end_date, current_ratio, quick_ratio, roe,
                   gross_margin, invturn_days, arturn_days
            FROM financial_indicators
            WHERE ts_code = '{ts_code}'
        """
        df_finance = pd.read_sql(query_finance, con=engine)

        # 3. 从 MySQL 读取低频战略数据 (strategic_indicators)
        query_strategic = f"""
            SELECT ts_code, report_period, revenue_growth_rate, net_profit_growth_rate,
                   rd_expense_ratio, goodwill_ratio
            FROM strategic_indicators
            WHERE ts_code = '{ts_code}'
        """
        df_strategic = pd.read_sql(query_strategic, con=engine)

        # 数据存在性校验
        if df_market.empty or df_finance.empty:
            logger.warning(f"[{ts_code}] 市场数据或财务数据缺失，无法执行对齐。")
            return pd.DataFrame()

        # 4. 时间列类型转换 (merge_asof 严格要求 key 列必须为 datetime 类型)
        df_market['trade_date'] = pd.to_datetime(df_market['trade_date'])
        df_finance['end_date'] = pd.to_datetime(df_finance['end_date'])

        # 5. 升序排序（merge_asof 要求）
        df_market = df_market.sort_values('trade_date').reset_index(drop=True)
        df_finance = df_finance.sort_values('end_date').reset_index(drop=True)

        logger.info(f"[{ts_code}] 正在使用 merge_asof 对齐市场数据与财务数据...")

        # 6. 第一次合并：市场数据 + 财务数据
        merged_df = pd.merge_asof(
            left=df_market,
            right=df_finance,
            left_on='trade_date',
            right_on='end_date',
            by='ts_code',
            direction='backward'
        )

        # 7. 第二次合并：追加战略指标数据
        if not df_strategic.empty:
            df_strategic['report_period'] = pd.to_datetime(df_strategic['report_period'])
            df_strategic = df_strategic.sort_values('report_period').reset_index(drop=True)

            logger.info(f"[{ts_code}] 正在使用 merge_asof 对齐战略指标数据...")
            merged_df = pd.merge_asof(
                left=merged_df,
                right=df_strategic,
                left_on='trade_date',
                right_on='report_period',
                by='ts_code',
                direction='backward'
            )
        else:
            logger.warning(f"[{ts_code}] 战略指标数据缺失，跳过战略维度对齐。")

        # 8. 处理缺失值：向前填充 + 剔除无财报支撑的极早期数据
        merged_df = merged_df.ffill().dropna().reset_index(drop=True)

        logger.info(f"✅ [{ts_code}] 三维异构数据对齐成功！最终有效数据量: {len(merged_df)} 条。")
        return merged_df

    except SQLAlchemyError as db_err:
        logger.error(f"❌ 数据库查询失败: {str(db_err)}", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ 数据对齐过程中发生异常: {str(e)}", exc_info=True)
        return pd.DataFrame()
    finally:
        engine.dispose()


# ==============================================================================
# 3. 简易测试入口
# ==============================================================================
if __name__ == "__main__":
    # 测试单一股票的数据对齐
    test_ts_code = '600519.SH'
    aligned_data = align_heterogeneous_data(test_ts_code)

    if not aligned_data.empty:
        print("\n--- 对齐后的扁平化数据预览 (前 5 条) ---")
        # 打印部分列以验证对齐效果
        preview_columns = ['trade_date', 'end_date', 'turnover_rate', 'current_ratio', 'roe']
        print(aligned_data[preview_columns].head())
"""
==============================================================================
上市公司多维风险画像与预警系统
阶段二：多源异构数据融合与预处理管道
步骤 2.4：战略维度数据独立采集与持久化 (全真实数据版 / 方案B)

功能描述：
跨越三大财务报表接口（财务指标、资产负债表、利润表）提取真实数据：
1. 营收增长率、净利润增长率
2. 真实计算：研发占比 = 研发费用 / 营业总收入
3. 真实计算：商誉占比 = 商誉 / 资产总计
==============================================================================
"""

import time
import logging
import sys
import pandas as pd
# 提前声明适应 Pandas 的未来版本特性，消除 FutureWarning 警告
pd.set_option('future.no_silent_downcasting', True)

import numpy as np
import tushare as ts
from sqlalchemy import create_engine
from functools import wraps

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

# 初始化 Tushare Pro 接口
logger.info("初始化 Tushare Pro 接口 (战略真实数据通道)...")
pro = ts.pro_api(TUSHARE_TOKEN)
pro._DataApi__token = TUSHARE_TOKEN
pro._DataApi__http_url = 'https://jiaoch.site'

# ==============================================================================
# 2. 接口重试机制
# ==============================================================================
def with_exponential_backoff(max_retries=3, base_delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"❌ 达到最大重试次数，执行失败: {str(e)}")
                        raise e
                    sleep_time = base_delay * (2 ** attempt)
                    logger.warning(f"⚠️ 接口调用异常: {str(e)}。{sleep_time} 秒后重试...")
                    time.sleep(sleep_time)
        return wrapper
    return decorator

@with_exponential_backoff(max_retries=3)
def call_tushare_api(api_name, **kwargs):
    api_func = getattr(pro, api_name)
    return api_func(**kwargs)

# ==============================================================================
# 3. 核心抓取与合并逻辑
# ==============================================================================
def fetch_strategic_data(ts_code_list: list):
    """按股票代码列表拉取并计算真实的季度战略指标数据"""
    logger.info(f"开始抓取 {len(ts_code_list)} 家公司的真实战略维度数据...")
    engine = create_engine(DB_URI, pool_recycle=3600)

    try:
        for ts_code in ts_code_list:
            logger.info(f"正在获取 {ts_code} 的跨表财报数据...")

            # 1. 获取成长性指标 (来自 fina_indicator)
            df_fina = call_tushare_api('fina_indicator', ts_code=ts_code, fields='ts_code,end_date,tr_yoy,dt_netprofit_yoy')
            time.sleep(0.3) # 防限流

            # 2. 获取商誉与总资产 (来自 balancesheet)
            df_bs = call_tushare_api('balancesheet', ts_code=ts_code, fields='ts_code,end_date,goodwill,total_assets')
            time.sleep(0.3)

            # 3. 获取研发费用与总营收 (来自 income)
            df_inc = call_tushare_api('income', ts_code=ts_code, fields='ts_code,end_date,rd_exp,total_revenue')
            time.sleep(0.3)

            # 只有当基础的财务指标存在时，才进行后续合并计算
            if df_fina is not None and not df_fina.empty:

                # 清洗各表的报告期格式，并提前按单股单期去重（Tushare有时会返回更正前的多份财报）
                for df in [df_fina, df_bs, df_inc]:
                    if df is not None and not df.empty:
                        df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
                        df.dropna(subset=['end_date'], inplace=True)
                        df.drop_duplicates(subset=['ts_code', 'end_date'], keep='first', inplace=True)

                # ================= 核心：三表归一合并 (Merge) =================
                df_merged = df_fina
                if df_bs is not None and not df_bs.empty:
                    df_merged = pd.merge(df_merged, df_bs, on=['ts_code', 'end_date'], how='left')
                if df_inc is not None and not df_inc.empty:
                    df_merged = pd.merge(df_merged, df_inc, on=['ts_code', 'end_date'], how='left')

                # ================= 计算真实的衍生战略指标 =================
                # 对于商誉和研发费用，如果是空值(NaN)，通常代表企业该季度没有发生该项业务，填充为 0
                df_merged['goodwill'] = df_merged.get('goodwill', pd.Series(dtype=float)).fillna(0)
                df_merged['rd_exp'] = df_merged.get('rd_exp', pd.Series(dtype=float)).fillna(0)

                # 防御性计算：防止分母为空或为 0 导致除零异常
                df_merged['total_assets'] = df_merged.get('total_assets', pd.Series(dtype=float)).replace(0, np.nan)
                df_merged['total_revenue'] = df_merged.get('total_revenue', pd.Series(dtype=float)).replace(0, np.nan)

                # 计算占比 (保留4位小数以防精度溢出)
                df_merged['goodwill_ratio'] = np.round(df_merged['goodwill'] / df_merged['total_assets'], 6).fillna(0)
                df_merged['rd_expense_ratio'] = np.round(df_merged['rd_exp'] / df_merged['total_revenue'], 6).fillna(0)

                # ================= 字段重命名与裁剪 =================
                df_merged = df_merged.rename(columns={
                    'end_date': 'report_period',
                    'tr_yoy': 'revenue_growth_rate',
                    'dt_netprofit_yoy': 'net_profit_growth_rate'
                })

                # 提取最终存入数据库的列
                final_columns = ['ts_code', 'report_period', 'revenue_growth_rate',
                                 'net_profit_growth_rate', 'rd_expense_ratio', 'goodwill_ratio']
                df_to_db = df_merged[final_columns].copy()

                # ================= 数据库增量入库拦截逻辑 =================
                query_existing = f"SELECT report_period FROM strategic_indicators WHERE ts_code = '{ts_code}'"
                existing_df = pd.read_sql(query_existing, con=engine)
                existing_dates = set(pd.to_datetime(existing_df['report_period']).dt.strftime('%Y-%m-%d').tolist())

                df_to_db['date_str'] = df_to_db['report_period'].dt.strftime('%Y-%m-%d')
                df_to_db = df_to_db[~df_to_db['date_str'].isin(existing_dates)].drop(columns=['date_str'])

                if not df_to_db.empty:
                    df_to_db.to_sql('strategic_indicators', con=engine, if_exists='append', index=False)
                    logger.info(f"✅ {ts_code} 真实战略数据计算并入库成功，新增 {len(df_to_db)} 条。")
                else:
                    logger.info(f"➖ {ts_code} 战略数据已是最新，无需更新。")

    except Exception as e:
        logger.error(f"❌ 拉取或计算真实战略数据时发生严重异常: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("战略数据采集完毕，数据库引擎已释放。")

if __name__ == "__main__":
    # 测试拉取真实战略数据
    sample_codes = ['000001.SZ', '600519.SH']
    fetch_strategic_data(sample_codes)
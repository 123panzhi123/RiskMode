"""
==============================================================================
上市公司多维风险画像与预警系统
一键补全行业数据采集脚本

功能：根据指定行业（或全部行业），批量采集该行业所有公司的：
1. 财务指标数据 (financial_indicators)
2. 战略指标数据 (strategic_indicators)
3. 市场行情数据 (market_indicators) — 如果缺失

使用方法：
  python batch_fetch_industry_data.py --industry 白酒
  python batch_fetch_industry_data.py --all   # 采集所有行业
==============================================================================
"""

import argparse
import logging
import sys
import time
import pandas as pd
import numpy as np
from functools import wraps
from sqlalchemy import create_engine

# 确保能导入同目录下的 config
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import TUSHARE_TOKEN, DB_URI

import tushare as ts

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# 初始化 Tushare
pro = ts.pro_api(TUSHARE_TOKEN)
pro._DataApi__token = TUSHARE_TOKEN
pro._DataApi__http_url = 'https://jiaoch.site'


def with_exponential_backoff(max_retries=3, base_delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"达到最大重试次数，执行失败: {str(e)}")
                        raise e
                    sleep_time = base_delay * (2 ** attempt)
                    logger.warning(f"接口异常: {str(e)}，{sleep_time}秒后重试...")
                    time.sleep(sleep_time)
        return wrapper
    return decorator


@with_exponential_backoff(max_retries=3)
def call_tushare_api(api_name, **kwargs):
    api_func = getattr(pro, api_name)
    return api_func(**kwargs)


def fetch_financial_for_codes(ts_code_list, engine):
    """批量采集财务指标数据"""
    logger.info(f"开始采集 {len(ts_code_list)} 家公司的财务数据...")
    
    for ts_code in ts_code_list:
        try:
            # 检查是否已有数据
            existing = pd.read_sql(
                f"SELECT COUNT(*) as cnt FROM financial_indicators WHERE ts_code = '{ts_code}'",
                con=engine
            )
            if existing.iloc[0]['cnt'] > 0:
                logger.info(f"  {ts_code} 财务数据已存在，跳过")
                continue

            fields = 'ts_code,end_date,roe,current_ratio,quick_ratio,gross_margin,invturn_days,arturn_days'
            df = call_tushare_api('fina_indicator', ts_code=ts_code, fields=fields)

            if df is not None and not df.empty:
                df = df.dropna()
                df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
                df = df.dropna(subset=['end_date'])
                df = df.drop_duplicates(subset=['ts_code', 'end_date'], keep='first')

                if not df.empty:
                    df.to_sql('financial_indicators', con=engine, if_exists='append', index=False)
                    logger.info(f"  ✅ {ts_code} 财务数据入库 {len(df)} 条")
            else:
                logger.info(f"  ➖ {ts_code} 无财务数据")

            time.sleep(0.35)
        except Exception as e:
            logger.warning(f"  ❌ {ts_code} 财务数据采集失败: {e}")
            continue


def fetch_market_for_codes(ts_code_list, engine, trade_date=None):
    """批量采集市场行情数据（按股票代码逐个拉取最近交易日数据）"""
    logger.info(f"开始采集 {len(ts_code_list)} 家公司的市场行情数据...")

    for ts_code in ts_code_list:
        try:
            # 检查是否已有数据
            existing = pd.read_sql(
                f"SELECT COUNT(*) as cnt FROM market_indicators WHERE ts_code = '{ts_code}'",
                con=engine
            )
            if existing.iloc[0]['cnt'] > 0:
                logger.info(f"  {ts_code} 市场数据已存在，跳过")
                continue

            # 拉取该股票最近的 daily_basic 数据（不指定日期则返回最新可用数据）
            fields = 'ts_code,trade_date,turnover_rate,volume_ratio,pe'
            if trade_date:
                df = call_tushare_api('daily_basic', ts_code=ts_code, trade_date=trade_date, fields=fields)
            else:
                df = call_tushare_api('daily_basic', ts_code=ts_code, fields=fields)

            if df is not None and not df.empty:
                df = df.dropna()
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                # 只保留需要的列
                db_columns = ['ts_code', 'trade_date', 'turnover_rate', 'volume_ratio', 'pe']
                df_to_db = df[db_columns].drop_duplicates(subset=['ts_code', 'trade_date'], keep='first')

                if not df_to_db.empty:
                    # 防重复：检查已有日期
                    existing_dates_df = pd.read_sql(
                        f"SELECT trade_date FROM market_indicators WHERE ts_code = '{ts_code}'",
                        con=engine
                    )
                    existing_dates = set(pd.to_datetime(existing_dates_df['trade_date']).dt.strftime('%Y-%m-%d').tolist())
                    df_to_db['date_str'] = df_to_db['trade_date'].dt.strftime('%Y-%m-%d')
                    df_to_db = df_to_db[~df_to_db['date_str'].isin(existing_dates)].drop(columns=['date_str'])

                    if not df_to_db.empty:
                        df_to_db.to_sql('market_indicators', con=engine, if_exists='append', index=False)
                        logger.info(f"  ✅ {ts_code} 市场数据入库 {len(df_to_db)} 条")
                    else:
                        logger.info(f"  ➖ {ts_code} 市场数据已是最新")
            else:
                logger.info(f"  ➖ {ts_code} 无市场数据")

            time.sleep(0.35)
        except Exception as e:
            logger.warning(f"  ❌ {ts_code} 市场数据采集失败: {e}")
            continue


def fetch_strategic_for_codes(ts_code_list, engine):
    """批量采集战略指标数据"""
    logger.info(f"开始采集 {len(ts_code_list)} 家公司的战略数据...")
    pd.set_option('future.no_silent_downcasting', True)

    for ts_code in ts_code_list:
        try:
            existing = pd.read_sql(
                f"SELECT COUNT(*) as cnt FROM strategic_indicators WHERE ts_code = '{ts_code}'",
                con=engine
            )
            if existing.iloc[0]['cnt'] > 0:
                logger.info(f"  {ts_code} 战略数据已存在，跳过")
                continue

            # 成长性指标
            df_fina = call_tushare_api('fina_indicator', ts_code=ts_code,
                                       fields='ts_code,end_date,tr_yoy,dt_netprofit_yoy')
            time.sleep(0.3)

            # 商誉与总资产
            df_bs = call_tushare_api('balancesheet', ts_code=ts_code,
                                     fields='ts_code,end_date,goodwill,total_assets')
            time.sleep(0.3)

            # 研发费用与总营收
            df_inc = call_tushare_api('income', ts_code=ts_code,
                                      fields='ts_code,end_date,rd_exp,total_revenue')
            time.sleep(0.3)

            if df_fina is not None and not df_fina.empty:
                for df in [df_fina, df_bs, df_inc]:
                    if df is not None and not df.empty:
                        df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
                        df.dropna(subset=['end_date'], inplace=True)
                        df.drop_duplicates(subset=['ts_code', 'end_date'], keep='first', inplace=True)

                df_merged = df_fina
                if df_bs is not None and not df_bs.empty:
                    df_merged = pd.merge(df_merged, df_bs, on=['ts_code', 'end_date'], how='left')
                if df_inc is not None and not df_inc.empty:
                    df_merged = pd.merge(df_merged, df_inc, on=['ts_code', 'end_date'], how='left')

                df_merged['goodwill'] = df_merged.get('goodwill', pd.Series(dtype=float)).fillna(0)
                df_merged['rd_exp'] = df_merged.get('rd_exp', pd.Series(dtype=float)).fillna(0)
                df_merged['total_assets'] = df_merged.get('total_assets', pd.Series(dtype=float)).replace(0, np.nan)
                df_merged['total_revenue'] = df_merged.get('total_revenue', pd.Series(dtype=float)).replace(0, np.nan)

                df_merged['goodwill_ratio'] = np.round(df_merged['goodwill'] / df_merged['total_assets'], 6).fillna(0)
                df_merged['rd_expense_ratio'] = np.round(df_merged['rd_exp'] / df_merged['total_revenue'], 6).fillna(0)

                df_merged = df_merged.rename(columns={
                    'end_date': 'report_period',
                    'tr_yoy': 'revenue_growth_rate',
                    'dt_netprofit_yoy': 'net_profit_growth_rate'
                })

                final_columns = ['ts_code', 'report_period', 'revenue_growth_rate',
                                 'net_profit_growth_rate', 'rd_expense_ratio', 'goodwill_ratio']
                df_to_db = df_merged[final_columns].copy()
                df_to_db = df_to_db.dropna(subset=['report_period'])

                if not df_to_db.empty:
                    df_to_db.to_sql('strategic_indicators', con=engine, if_exists='append', index=False)
                    logger.info(f"  ✅ {ts_code} 战略数据入库 {len(df_to_db)} 条")
            else:
                logger.info(f"  ➖ {ts_code} 无战略数据")

        except Exception as e:
            logger.warning(f"  ❌ {ts_code} 战略数据采集失败: {e}")
            continue


def main():
    parser = argparse.ArgumentParser(description="一键补全行业数据")
    parser.add_argument("--industry", type=str, default=None, help="指定行业名称，如: 白酒")
    parser.add_argument("--all", action="store_true", help="采集所有行业")
    args = parser.parse_args()

    engine = create_engine(DB_URI, pool_recycle=3600)

    try:
        if args.all:
            df_industries = pd.read_sql("SELECT DISTINCT industry FROM company_info", con=engine)
            industries = df_industries['industry'].tolist()
            logger.info(f"将采集全部 {len(industries)} 个行业的数据")
        elif args.industry:
            industries = [args.industry]
        else:
            logger.error("请指定 --industry 行业名称 或 --all 采集全部行业")
            return

        for industry in industries:
            logger.info(f"\n{'='*60}")
            logger.info(f"开始处理行业: {industry}")
            logger.info(f"{'='*60}")

            df_codes = pd.read_sql(
                f"SELECT ts_code FROM company_info WHERE industry = '{industry}'",
                con=engine
            )
            if df_codes.empty:
                logger.info(f"行业 {industry} 无公司数据，跳过")
                continue

            codes = df_codes['ts_code'].tolist()
            logger.info(f"行业 {industry} 共 {len(codes)} 家公司")

            fetch_financial_for_codes(codes, engine)
            fetch_strategic_for_codes(codes, engine)
            fetch_market_for_codes(codes, engine)

        logger.info("\n全部行业数据采集完毕！")

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
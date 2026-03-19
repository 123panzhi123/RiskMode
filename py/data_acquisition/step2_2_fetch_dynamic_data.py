"""
==============================================================================
上市公司多维风险画像与预警系统
阶段二：多源异构数据融合与预处理管道
步骤 2.2：高频市场行情与低频财务数据采集

功能描述：
1. 从 Tushare 提取每日行情数据（高频）与财务指标数据（低频）。
2. 实现网络异常时的指数退避重试机制（最大重试3次）。
3. 引入休眠控制防反爬/防接口限流，并完成缺失值清洗后持久化至 MySQL。
==============================================================================
"""

import time
import logging
import sys
from functools import wraps
import tushare as ts
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


# 修改01
# TUSHARE_TOKEN = 'YOUR_TUSHARE_TOKEN_HERE'
# DB_URI = "mysql+pymysql://root:your_password@127.0.0.1:3306/risk_warning_system?charset=utf8mb4"
#


# # 初始化 Tushare Pro 接口
# pro = ts.pro_api(TUSHARE_TOKEN)

# 修改01
# 步骤 A: 初始化 Tushare 接口
logger.info("初始化 Tushare Pro 接口 (使用自定义代理节点)...")
pro = ts.pro_api(TUSHARE_TOKEN)

# --- 核心修改：强制修改底层请求的 URL 和 Token ---
pro._DataApi__token = TUSHARE_TOKEN
pro._DataApi__http_url = 'https://jiaoch.site'



# ==============================================================================
# 2. 核心工程组件：指数退避重试装饰器
# ==============================================================================
def with_exponential_backoff(max_retries=3, base_delay=2):
    """
    网络请求重试装饰器，采用指数退避算法缓解服务器压力
    第1次重试等待2秒，第2次等待4秒，第3次等待8秒
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"❌ 达到最大重试次数({max_retries})，函数 {func.__name__} 执行失败: {str(e)}")
                        raise e
                    sleep_time = base_delay * (2 ** attempt)
                    logger.warning(f"⚠️ 接口调用异常: {str(e)}。{sleep_time} 秒后进行第 {attempt + 1} 次重试...")
                    time.sleep(sleep_time)

        return wrapper

    return decorator


# 为 Tushare API 调用封装带有重试机制的函数
@with_exponential_backoff(max_retries=3)
def call_tushare_api(api_name, **kwargs):
    """动态调用 Tushare 接口"""
    api_func = getattr(pro, api_name)
    return api_func(**kwargs)


def fetch_daily_market_data(start_date: str, end_date: str):
    """
    按日循环拉取高频市场行情数据（已加入外键过滤与防重复增量更新机制）
    """
    logger.info(f"开始抓取区间 [{start_date}, {end_date}] 的每日市场行情...")
    engine = create_engine(DB_URI, pool_recycle=3600)

    try:
        # 【新增防御 1】：先从数据库查出所有合法的、存在于 company_info 的股票代码
        valid_codes_df = pd.read_sql("SELECT ts_code FROM company_info", con=engine)
        valid_codes = set(valid_codes_df['ts_code'].tolist())
        logger.info(f"成功加载本地有效股票池，共 {len(valid_codes)} 只股票。")

        dates = pd.date_range(start=start_date, end=end_date)
        for date_obj in dates:
            trade_date = date_obj.strftime('%Y%m%d')
            logger.info(f"正在获取 {trade_date} 的 daily_basic 数据...")

            fields = 'ts_code,trade_date,turnover_rate,volume_ratio,pe,pb'
            df = call_tushare_api('daily_basic', trade_date=trade_date, fields=fields)

            if df is not None and not df.empty:
                df = df.dropna()
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

                # 【过滤网 1】：剔除不在 company_info 表中的股票（解决外键冲突 1452 报错）
                initial_len = len(df)
                df = df[df['ts_code'].isin(valid_codes)]
                if len(df) < initial_len:
                    logger.info(f"已剔除 {initial_len - len(df)} 条不在股票池中的行情记录。")

                db_columns = ['ts_code', 'trade_date', 'turnover_rate', 'volume_ratio', 'pe']
                df_to_db = df[db_columns]

                # 【过滤网 2】：查出当天数据库里已经存了哪些股票，剔除掉（解决重复录入 1062 报错）
                query_existing = f"SELECT ts_code FROM market_indicators WHERE trade_date = '{date_obj.strftime('%Y-%m-%d')}'"
                existing_df = pd.read_sql(query_existing, con=engine)
                existing_codes = set(existing_df['ts_code'].tolist())

                df_to_db = df_to_db[~df_to_db['ts_code'].isin(existing_codes)]

                if not df_to_db.empty:
                    df_to_db.to_sql('market_indicators', con=engine, if_exists='append', index=False)
                    logger.info(f"✅ {trade_date} 增量数据入库成功，共新增 {len(df_to_db)} 条。")
                else:
                    logger.info(f"➖ {trade_date} 数据已存在，跳过入库。")
            else:
                logger.info(f"➖ {trade_date} 无数据（可能为非交易日）。")

            time.sleep(0.5)

    except Exception as e:
        logger.error(f"❌ 拉取每日行情数据时发生严重异常: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("市场数据采集完毕，数据库引擎已释放。")


def fetch_financial_data(ts_code_list: list):
    """
    按股票代码列表拉取低频财务指标数据（已加入防重复增量更新机制）
    """
    logger.info(f"开始抓取 {len(ts_code_list)} 家公司的低频财务数据...")
    engine = create_engine(DB_URI, pool_recycle=3600)

    try:
        for ts_code in ts_code_list:
            logger.info(f"正在获取 {ts_code} 的 fina_indicator 数据...")
            fields = 'ts_code,end_date,roe,current_ratio,quick_ratio,gross_margin,invturn_days,arturn_days'
            df = call_tushare_api('fina_indicator', ts_code=ts_code, fields=fields)

            if df is not None and not df.empty:
                df = df.dropna()
                df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
                df = df.dropna(subset=['end_date'])

                # Tushare本身的数据去重（保留最新一期）
                initial_len = len(df)
                df = df.drop_duplicates(subset=['ts_code', 'end_date'], keep='first')
                if len(df) != initial_len:
                    logger.info(f"🧹 已清洗掉 {initial_len - len(df)} 条 Tushare 返回的重复周期报表。")

                # 【过滤网】：查询数据库中该股票已存在的报表日期，剔除已存在的记录（解决重复录入 1062 报错）
                query_existing = f"SELECT end_date FROM financial_indicators WHERE ts_code = '{ts_code}'"
                existing_df = pd.read_sql(query_existing, con=engine)
                # 将日期转换为 yyyy-mm-dd 字符串集合进行对比
                existing_dates = set(pd.to_datetime(existing_df['end_date']).dt.strftime('%Y-%m-%d').tolist())

                df['date_str'] = df['end_date'].dt.strftime('%Y-%m-%d')
                df = df[~df['date_str'].isin(existing_dates)]
                df = df.drop(columns=['date_str'])  # 用完即丢

                if not df.empty:
                    df.to_sql('financial_indicators', con=engine, if_exists='append', index=False)
                    logger.info(f"✅ {ts_code} 增量财务数据入库成功，新增 {len(df)} 条。")
                else:
                    logger.info(f"➖ {ts_code} 所有历史财务数据已在库中，无需更新。")

            time.sleep(0.3)

    except Exception as e:
        logger.error(f"❌ 拉取财务数据时发生严重异常: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("财务数据采集完毕，数据库引擎已释放。")


# t1 -3-16
# ==============================================================================
# 3. 业务逻辑函数
# ==============================================================================

# def fetch_daily_market_data(start_date: str, end_date: str):
#     """
#     按日循环拉取高频市场行情数据
#     :param start_date: 开始日期 (YYYYMMDD)
#     :param end_date: 结束日期 (YYYYMMDD)
#     """
#     logger.info(f"开始抓取区间 [{start_date}, {end_date}] 的每日市场行情...")
#     engine = create_engine(DB_URI, pool_recycle=3600)
#
#     # 生成交易日历序列
#     dates = pd.date_range(start=start_date, end=end_date)
#
#     try:
#         for date_obj in dates:
#             trade_date = date_obj.strftime('%Y%m%d')
#             logger.info(f"正在获取 {trade_date} 的 daily_basic 数据...")
#
#             # 必须提取的具体字段
#             fields = 'ts_code,trade_date,turnover_rate,volume_ratio,pe,pb'
#             df = call_tushare_api('daily_basic', trade_date=trade_date, fields=fields)
#
#             if df is not None and not df.empty:
#                 # 清洗空值
#                 df = df.dropna()
#
#                 # 转换日期格式适配 MySQL
#                 df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
#
#                 # DDL 适配：数据库里是 volatility，提示词里提取的是 pb
#                 # 我们这里暂时将 df 中的列筛选为 DDL 中存在的列，避免直接报错，也可以选择进一步计算 volatility
#                 db_columns = ['ts_code', 'trade_date', 'turnover_rate', 'volume_ratio', 'pe']
#                 df_to_db = df[db_columns]
#
#                 # 入库
#                 df_to_db.to_sql('market_indicators', con=engine, if_exists='append', index=False)
#                 logger.info(f"✅ {trade_date} 数据入库成功，共 {len(df_to_db)} 条。")
#             else:
#                 logger.info(f"➖ {trade_date} 无数据（可能为非交易日）。")
#
#             # 引入防反爬休眠（Tushare 限制通常为每分钟 200 次或更高，0.5秒较为安全）
#             time.sleep(0.5)
#
#     except Exception as e:
#         logger.error(f"❌ 拉取每日行情数据时发生严重异常: {e}", exc_info=True)
#     finally:
#         engine.dispose()
#         logger.info("市场数据采集完毕，数据库引擎已释放。")


# T1-314
# def fetch_financial_data(ts_code_list: list):
#     """
#     按股票代码列表拉取低频财务指标数据
#     :param ts_code_list: 股票代码列表，例如 ['000001.SZ', '600000.SH']
#     """
#     logger.info(f"开始抓取 {len(ts_code_list)} 家公司的低频财务数据...")
#     engine = create_engine(DB_URI, pool_recycle=3600)
#
#     try:
#         # 考虑到财务接口数据量大，按股票代码循环获取
#         for ts_code in ts_code_list:
#             logger.info(f"正在获取 {ts_code} 的 fina_indicator 数据...")
#
#             # 必须提取的具体字段
#             fields = 'ts_code,end_date,current_ratio,quick_ratio,gross_margin,invturn_days,arturn_days'
#             df = call_tushare_api('fina_indicator', ts_code=ts_code, fields=fields)
#
#             if df is not None and not df.empty:
#                 # 清洗空值
#                 df = df.dropna()
#
#                 # 转换日期格式适配 MySQL
#                 df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
#                 df = df.dropna(subset=['end_date'])
#
#                 # 入库
#                 df.to_sql('financial_indicators', con=engine, if_exists='append', index=False)
#                 logger.info(f"✅ {ts_code} 财务数据入库成功，共 {len(df)} 条（历史各期报表）。")
#
#             # 防限流休眠
#             time.sleep(0.3)
#
#     except Exception as e:
#         logger.error(f"❌ 拉取财务数据时发生严重异常: {e}", exc_info=True)
#     finally:
#         engine.dispose()
#         logger.info("财务数据采集完毕，数据库引擎已释放。")
#


# t1-3-16
# def fetch_financial_data(ts_code_list: list):
#     """
#     按股票代码列表拉取低频财务指标数据
#     """
#     logger.info(f"开始抓取 {len(ts_code_list)} 家公司的低频财务数据...")
#     engine = create_engine(DB_URI, pool_recycle=3600)
#
#     try:
#         # 考虑到财务接口数据量大，按股票代码循环获取
#         for ts_code in ts_code_list:
#             logger.info(f"正在获取 {ts_code} 的 fina_indicator 数据...")
#
#             # 必须提取的具体字段
#             fields = 'ts_code,end_date,current_ratio,quick_ratio,gross_margin,invturn_days,arturn_days'
#             df = call_tushare_api('fina_indicator', ts_code=ts_code, fields=fields)
#
#             if df is not None and not df.empty:
#                 # 1. 清洗空值
#                 df = df.dropna()
#
#                 # 2. 转换日期格式适配 MySQL
#                 df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
#                 df = df.dropna(subset=['end_date'])
#
#                 # ==================== 🌟 核心新增：去重逻辑 ====================
#                 # Tushare经常返回财报更正/合并后的多条记录，导致数据库唯一键冲突报错
#                 initial_len = len(df)
#                 # 根据股票代码和报告期去重，keep='first'表示保留最新发布(或者Tushare默认排第一)的那一份报表
#                 df = df.drop_duplicates(subset=['ts_code', 'end_date'], keep='first')
#
#                 if len(df) != initial_len:
#                     logger.info(f"🧹 已清洗掉 {initial_len - len(df)} 条重复周期的财务报表记录。")
#                 # ===============================================================
#
#                 # 3. 入库
#                 df.to_sql('financial_indicators', con=engine, if_exists='append', index=False)
#                 logger.info(f"✅ {ts_code} 财务数据入库成功，共 {len(df)} 条（历史各期去重后报表）。")
#
#             # 防限流休眠
#             time.sleep(0.3)
#
#
#
#
#     except Exception as e:
#         logger.error(f"❌ 拉取财务数据时发生严重异常: {e}", exc_info=True)
#     finally:
#         engine.dispose()
#         logger.info("财务数据采集完毕，数据库引擎已释放。")


# ==============================================================================
# 4. 简易测试入口
# ==============================================================================
if __name__ == "__main__":
    # 测试拉取一周的市场行情
    fetch_daily_market_data('20231009', '20231013')

    # 测试拉取两家公司的历史财务数据
    sample_codes = ['600519.SH']
    fetch_financial_data(sample_codes)
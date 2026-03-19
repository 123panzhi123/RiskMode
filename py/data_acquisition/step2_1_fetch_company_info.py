"""
==============================================================================
上市公司多维风险画像与预警系统
阶段二：多源异构数据融合与预处理管道
步骤 2.1：基础股票信息拉取与持久化

功能描述：
利用 Tushare Pro 接口拉取 A 股全市场上市公司的基础静态信息，
并通过 SQLAlchemy 配合连接池批量追加至 MySQL `company_info` 表。
==============================================================================
"""

import sys
import logging
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

#
# # --- 接口与数据库配置 ---
# # 注意：实际运行前请务必替换为真实的 Token 和数据库凭证
# TUSHARE_TOKEN = 'YOUR_TUSHARE_TOKEN_HERE'
# DB_USER = 'root'
# DB_PASS = '123'
# DB_HOST = '127.0.0.1'
# DB_PORT = '3306'
# DB_NAME = 'risk_warning_system'
#
# # 构建带字符集声明的数据库 URI，防止生僻字或中文乱码
# DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
#

# ==============================================================================
# 2. 核心业务逻辑函数
# ==============================================================================
# def fetch_and_store_company_info():
#     """核心执行流程：获取数据 -> 清洗 -> 入库"""
#     engine = None
#     try:
#         # # 步骤 A: 初始化 Tushare 接口
#         # logger.info("初始化 Tushare Pro 接口...")
#         # pro = ts.pro_api(TUSHARE_TOKEN)
#
#         # 修改01
#         # 步骤 A: 初始化 Tushare 接口
#         logger.info("初始化 Tushare Pro 接口 (使用自定义代理节点)...")
#         pro = ts.pro_api(TUSHARE_TOKEN)
#
#         # --- 核心修改：强制修改底层请求的 URL 和 Token ---
#         pro._DataApi__token = TUSHARE_TOKEN
#         pro._DataApi__http_url = 'https://jiaoch.site'
#
#
#         # 步骤 B: 构建高可用数据库连接池
#         logger.info("建立 MySQL 数据库连接池...")
#         engine = create_engine(
#             DB_URI,
#             pool_size=10,
#             max_overflow=20,
#             pool_recycle=3600,
#             pool_pre_ping=True
#         )
#
#         # 步骤 C: 调用接口获取基础数据
#         logger.info("正在向 Tushare 发起请求，获取 A 股上市状态公司列表...")
#         target_fields = 'ts_code,symbol,name,area,industry,list_date'
#         df = pro.stock_basic(exchange='', list_status='L', fields=target_fields)
#
#         if df is None or df.empty:
#             logger.warning("未拉取到任何数据，请检查网络状态或 Token 权限。")
#             return
#
#         logger.info(f"成功获取 {len(df)} 条基础数据，开始数据清洗...")
#
#         # 步骤 D: 数据清洗与类型对齐
#         # 1. 将 'YYYYMMDD' 字符串转为标准的 datetime 对象以适配 MySQL DATE 类型
#         df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
#         # 2. 剔除上市日期异常的脏数据
#         df = df.dropna(subset=['list_date'])
#
#         # 3. 字段对齐：剔除 DDL 表结构中不存在的 area 字段，防止入库报错
#         db_columns = ['ts_code', 'symbol', 'name', 'industry', 'list_date']
#         df_to_db = df[db_columns]
#
#         # 步骤 E: 批量持久化入库
#         logger.info("开始将清洗后的数据批量追加至数据库 company_info 表...")
#         df_to_db.to_sql(
#             name='company_info',
#             con=engine,
#             if_exists='append',
#             index=False
#         )
#
#         logger.info("✅ 步骤 2.1 执行完毕：股票基础信息数据入库成功！")
#
#     except SQLAlchemyError as db_err:
#         logger.error(f"❌ 数据库操作引发严重异常: {str(db_err)}", exc_info=True)
#     except Exception as e:
#         logger.error(f"❌ 数据抓取或处理过程发生未知异常: {str(e)}", exc_info=True)
#
#     finally:
#         # 步骤 F: 资源释放防泄漏
#         if engine is not None:
#             engine.dispose()
#             logger.info("已安全释放数据库连接引擎。")


# T1
# ==============================================================================
# 2. 核心业务逻辑函数
# ==============================================================================
def fetch_and_store_company_info():
    """核心执行流程：获取数据 -> 清洗 -> 入库"""
    engine = None
    try:
        # 步骤 A: 初始化 Tushare 接口
        logger.info("初始化 Tushare Pro 接口 (使用自定义代理节点)...")
        pro = ts.pro_api(TUSHARE_TOKEN)

        pro._DataApi__token = TUSHARE_TOKEN
        pro._DataApi__http_url = 'https://jiaoch.site'

        # 步骤 B: 构建高可用数据库连接池
        logger.info("建立 MySQL 数据库连接池...")
        engine = create_engine(
            DB_URI,
            pool_size=10,
            max_overflow=20,
            pool_recycle=3600,
            pool_pre_ping=True
        )

        # 步骤 C: 调用接口获取基础数据
        logger.info("正在向 Tushare 发起请求，获取全市场(包含退市/暂停上市)公司列表...")
        target_fields = 'ts_code,symbol,name,area,industry,list_date'

        # ================== 核心修改 1 ==================
        # 将 list_status='L' 改为 'L,D,P' (上市、退市、暂停上市)，保证基础表覆盖所有历史股票
        df = pro.stock_basic(exchange='', list_status='L,D,P', fields=target_fields)
        # ===============================================

        if df is None or df.empty:
            logger.warning("未拉取到任何数据，请检查网络状态或 Token 权限。")
            return

        logger.info(f"成功获取 {len(df)} 条基础数据，开始数据清洗...")

        # # 步骤 D: 数据清洗与类型对齐
        # df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
        # df = df.dropna(subset=['list_date'])
        #
        # db_columns = ['ts_code', 'symbol', 'name', 'industry', 'list_date']
        # df_to_db = df[db_columns]

        # T1
        # 步骤 D: 数据清洗与类型对齐
        df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
        df = df.dropna(subset=['list_date'])

        # ================= 新增清洗逻辑 =================
        # 如果退市公司的行业为空，则填充为 '未知'
        df['industry'] = df['industry'].fillna('未知')
        # 如果名字等其他可能引发非空约束的字段为空，也可以一并保护
        df['name'] = df['name'].fillna('未知')
        # ===============================================

        db_columns = ['ts_code', 'symbol', 'name', 'industry', 'list_date']
        df_to_db = df[db_columns]


        # ================== 核心修改 2 ==================
        # 增量入库逻辑：避免重复运行脚本时报 "主键冲突 (Duplicate entry)"
        try:
            # 先去数据库里查一下，看看已经存了哪些股票
            existing_df = pd.read_sql("SELECT ts_code FROM company_info", con=engine)
            existing_codes = set(existing_df['ts_code'].tolist())
            logger.info(f"数据库中已存在 {len(existing_codes)} 条股票记录。")
        except Exception:
            existing_codes = set()
            logger.info("未检测到历史记录，将执行全量插入。")

        # 过滤出数据库里还没有的“新股票”
        df_new = df_to_db[~df_to_db['ts_code'].isin(existing_codes)]
        # ===============================================

        # 步骤 E: 批量持久化入库
        if not df_new.empty:
            logger.info(f"开始将 {len(df_new)} 条增量/全新数据追加至数据库 company_info 表...")
            df_new.to_sql(
                name='company_info',
                con=engine,
                if_exists='append',
                index=False
            )
            logger.info("✅ 步骤 2.1 执行完毕：股票基础信息数据入库成功！")
        else:
            logger.info("✅ 数据库中的公司信息已经是最新最全的，无需重复插入。")

    except SQLAlchemyError as db_err:
        logger.error(f"❌ 数据库操作引发严重异常: {str(db_err)}", exc_info=True)
    except Exception as e:
        logger.error(f"❌ 数据抓取或处理过程发生未知异常: {str(e)}", exc_info=True)

    finally:
        # 步骤 F: 资源释放防泄漏
        if engine is not None:
            engine.dispose()
            logger.info("已安全释放数据库连接引擎。")

# ==============================================================================
# 3. 脚本主入口
# ==============================================================================
if __name__ == "__main__":
    logger.info("=== 启动数据管道：基础信息采集模块 ===")
    fetch_and_store_company_info()
    logger.info("=== 基础信息采集模块运行结束 ===")
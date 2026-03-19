# 文件名: data_acquisition/config.py

# Tushare 配置
TUSHARE_TOKEN = '1ef9bc7dd40820fb8a43d620aa2b09629d4fa1396e15862f3ba1f886340a'

# MySQL 数据库配置
DB_USER = 'root'
DB_PASS = '123'
DB_HOST = '127.0.0.1'
DB_PORT = '3306'
DB_NAME = 'risk_warning_system'

# 自动拼接成可用的数据库 URI
DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
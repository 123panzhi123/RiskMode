import numpy as np
import pandas as pd
import warnings


class EntropyTOPSIS:
    """
    面向大语言模型的上市公司多维风险画像与预警系统
    基于熵权法(EWM)与逼近理想解排序法(TOPSIS)的量化评估引擎
    """

    def __init__(self, df: pd.DataFrame, positive_cols: list, negative_cols: list):
        """
        初始化计算类
        :param df: 包含原始指标数据的 Pandas DataFrame
        :param positive_cols: 正向指标列表 (值越大越安全，如 current_ratio)
        :param negative_cols: 负向指标列表 (值越大风险越高，如 volatility)
        """
        self.df = df.copy()
        self.positive_cols = positive_cols
        self.negative_cols = negative_cols
        self.all_cols = positive_cols + negative_cols

        # 校验列是否存在
        missing_cols = [col for col in self.all_cols if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"DataFrame 中缺失以下指标列: {missing_cols}")

    def _calculate_entropy_weights(self) -> tuple:
        """
        私有方法：计算所有特征的客观权重向量 (全向量化运算)
        :return: (权重向量 Series, 标准化后的规范矩阵 DataFrame)
        """
        # 提取需要计算的特征矩阵
        X = self.df[self.all_cols].copy()

        # 1. 极大极小标准化 (Min-Max Normalization)
        X_norm = pd.DataFrame(index=X.index, columns=self.all_cols)

        # 分离正负指标，避免计算时由于极值相同导致的除零警告
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # 处理正向指标: (x - min) / (max - min)
            if self.positive_cols:
                pos_data = X[self.positive_cols]
                pos_min = pos_data.min()
                pos_max = pos_data.max()
                pos_range = pos_max - pos_min
                # 防御性编程：如果某一列全部值相同（方差为0），将 range 设为 1 避免除 0，标准化后该列将全为 0
                pos_range[pos_range == 0] = 1.0
                X_norm[self.positive_cols] = (pos_data - pos_min) / pos_range

            # 处理负向指标: (max - x) / (max - min)
            if self.negative_cols:
                neg_data = X[self.negative_cols]
                neg_min = neg_data.min()
                neg_max = neg_data.max()
                neg_range = neg_max - neg_min
                neg_range[neg_range == 0] = 1.0
                X_norm[self.negative_cols] = (neg_max - neg_data) / neg_range

        # 2. 计算特征比重矩阵 (P)
        # 为防止出现计算 ln(0) 引发数学异常，必须在所有的特征比重数据上加上一个极小的值 1e-9
        X_norm_safe = X_norm + 1e-9

        # 每一列的数据除以该列的总和
        P = X_norm_safe / X_norm_safe.sum(axis=0)

        # 3. 计算每个指标的信息熵 (e)
        n = len(X_norm_safe)
        if n <= 1:
            # 如果样本量只有 1 个，信息熵无意义，直接返回均等权重
            w = pd.Series(1.0 / len(self.all_cols), index=self.all_cols)
            return w, X_norm

        k = 1.0 / np.log(n)

        # 向量化计算信息熵矩阵: -k * sum(P * ln(P))
        entropy = -k * (P * np.log(P)).sum(axis=0)

        # 4. 根据差异系数 (d) 计算并返回客观权重向量 (w)
        d = 1.0 - entropy
        weights = d / d.sum()

        return weights, X_norm





    # 
    def calculate_comprehensive_score(self) -> pd.DataFrame:
        """
        计算 TOPSIS 相对贴近度，得出综合风险安全评分
        :return: 包含股票代码 (ts_code) 和综合评分 (composite_score) 的 DataFrame
        """
        # 1. 获取客观权重向量和标准化矩阵
        weights, X_norm = self._calculate_entropy_weights()

        # 2. 构建加权规范化矩阵 (Z)
        # 利用 Pandas 广播机制，将标准化矩阵的每一列乘以对应特征的客观权重
        Z = X_norm * weights

        # 3. 确定正理想解 (V+) 和负理想解 (V-)
        # ⚠️ 注意：由于在步骤 3.1 中，我们已经对负向指标进行了(max-x)/(max-min)的反转处理
        # 此时 X_norm 中的所有指标实际上都已经转化为了“正向（值越大越安全）”
        # 因此，加权规范化矩阵中，每一列的最大值即为正理想解，最小值即为负理想解
        V_plus = Z.max()
        V_minus = Z.min()

        # 4. 计算欧氏距离 (D+ 和 D-)
        # 利用 NumPy 的向量范数 (np.linalg.norm) 计算每一行代表的公司与极值状态的距离
        # axis=1 表示按行求欧氏距离
        D_plus = np.linalg.norm(Z.values - V_plus.values, axis=1)
        D_minus = np.linalg.norm(Z.values - V_minus.values, axis=1)

        # 5. 计算相对贴近度 (Ci)
        # 相对贴近度 Ci = D- / (D+ + D-)
        # 引入极其微小的平滑参数 1e-9，防止当 D+ 和 D- 同时为 0 时引发除零异常 (ZeroDivisionError)
        denominator = D_plus + D_minus
        denominator = np.where(denominator == 0, 1e-9, denominator)

        C_i = D_minus / denominator

        # 6. 组装结果
        # 提取原始传入的 DataFrame 中的股票代码列，与计算出的综合评分拼接
        if 'ts_code' not in self.df.columns:
            raise KeyError("原始数据中缺失极其关键的标识列: 'ts_code'")

        result_df = pd.DataFrame({
            'ts_code': self.df['ts_code'],
            'composite_score': C_i
        })

        return result_df




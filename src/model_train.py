# src/model_train.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
import logging
import os

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SalaryPredictor:
    """
    薪资预测模型类 (Pro版)
    负责：自动加载清洗后的数据 -> 特征工程适配 -> 随机森林建模 -> 评估与持久化
    """
    def __init__(self):
        # --- 1. 智能路径识别 (与 ETL 保持一致) ---
        base_dir = os.getcwd()
        if 'src' in base_dir: 
            base_dir = os.path.dirname(base_dir)
            
        # 数据库路径
        abs_db_path = os.path.join(base_dir, 'data', 'job_data.db')
        self.db_url = f'sqlite:///{abs_db_path}'
        
        # 模型保存路径
        self.model_path = os.path.join(base_dir, 'data', 'salary_model.pkl')
        
        self.engine = create_engine(self.db_url)
        self.table_name = 'cleaned_jobs_with_features'
        self.model = None
        self.feature_names = None 
        
        logging.info(f"🤖 模型系统初始化 | DB: {self.db_url}")

    def load_and_preprocess(self):
        """
        加载数据并进行特征预处理 (适配新的 ETL 结构)
        """
        logging.info("📥 正在从数据库加载训练数据...")
        
        try:
            df = pd.read_sql(f"SELECT * FROM {self.table_name}", self.engine)
        except Exception as e:
            raise ValueError(f"无法读取数据库表 '{self.table_name}'，请先运行 etl_pipeline.py！错误: {e}")

        if len(df) < 10:
            raise ValueError("数据量太少 (<10条)，无法进行有效训练。请先运行爬虫抓取更多数据！")

        # --- 1.5. 关键修复：过滤掉学历不限(degree_value=0)的样本 ---
        # 学历不限的岗位不应该参与学历相关的薪资预测，因为它们没有明确的学历要求
        original_count = len(df)
        df = df[df['degree_value'] > 0].copy()  # 只保留明确的学历要求（1=大专, 2=本科, 3=硕士, 4=博士）
        filtered_count = original_count - len(df)
        if filtered_count > 0:
            logging.info(f"   ⚠️ 已过滤 {filtered_count} 个'学历不限'样本（不参与学历相关预测）")
        
        # --- 1.6. 额外修复：过滤异常高薪样本（可能是数据解析错误）---
        # 对于数据分析师岗位，月薪超过5万通常异常（可能是OCR错误或单位错误）
        original_count2 = len(df)
        df = df[df['avg_salary'] < 50000].copy()
        filtered_count2 = original_count2 - len(df)
        if filtered_count2 > 0:
            logging.info(f"   ⚠️ 已过滤 {filtered_count2} 个异常高薪样本（>5万/月，可能是数据错误）")
        
        if len(df) < 10:
            raise ValueError(f"过滤后数据量太少 (<10条)，无法进行有效训练。")

        # --- 2. 选定特征列 (Feature Selection) ---
        # 数值型特征 (直接使用)
        numeric_features = ['exp_years']  # 修复：将degree_value从数值特征中移除
        
        # 类别型特征 (需要 One-Hot 编码)
        # 修复：将学历也作为类别特征，避免模型学习错误的数值关系
        categorical_features = ['city_clean', 'degree_value']
        
        # 技能特征 (二进制 0/1, 直接使用)
        skill_features = [col for col in df.columns if col.startswith('has_')]
        
        # 目标变量
        target_col = 'avg_salary'
        
        logging.info(f"   使用的技能特征: {skill_features}")
        logging.info(f"   ⚠️ 重要修复：学历(degree_value)已改为类别特征，避免数值关系错误")
        
        # 准备 X 和 y
        # 组合所有需要的列
        needed_cols = numeric_features + categorical_features + skill_features
        X = df[needed_cols].copy()
        y = df[target_col]
        
        # --- 3. 特征编码 (Encoding) ---
        # 对城市和学历进行 One-Hot 编码
        # 例如: city_clean_南京 = 1, degree_value_2 = 1 (表示本科)
        # 注意：由于已经过滤了degree_value=0，所以不会生成degree_value_0特征
        X = pd.get_dummies(X, columns=categorical_features, drop_first=False)
        
        # 验证：确保没有degree_value_0特征（如果存在说明过滤失败）
        degree_0_cols = [col for col in X.columns if 'degree_value_0' in col]
        if degree_0_cols:
            logging.warning(f"   ⚠️ 警告：发现degree_value_0特征列 {degree_0_cols}，这不应该存在！")
        
        # 记录最终的特征名称列表 (预测时必须保持一致)
        self.feature_names = X.columns.tolist()
        
        logging.info(f"✨ 特征工程完成: 样本数={len(X)}, 特征维度={len(self.feature_names)}")
        return X, y

    def train(self):
        """执行训练流程"""
        try:
            X, y = self.load_and_preprocess()
            
            # 划分数据集 (80% 训练, 20% 验证)
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            logging.info("🔥 开始训练随机森林回归模型 (RandomForestRegressor)...")
            
            # 初始化模型
            # n_estimators=200: 树越多越稳定
            # max_depth=None: 让树自然生长，捕捉复杂关系
            # n_jobs=-1:以此电脑最大核心数并行训练
            self.model = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)
            self.model.fit(X_train, y_train)
            
            # 评估
            score_train = self.model.score(X_train, y_train)
            score_test = self.model.score(X_test, y_test)
            y_pred = self.model.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)
            
            print("\n" + "="*40)
            print("📊 模型评估报告 (Model Evaluation)")
            print("="*40)
            print(f"训练集 R² 得分: {score_train:.4f}")
            print(f"测试集 R² 得分: {score_test:.4f} (核心指标)")
            print(f"平均绝对误差 (MAE): {mae:.2f} 元")
            print("-" * 40)
            print(f"解读: 模型预测薪资的平均误差约为 {int(mae)} 元。")
            print("="*40 + "\n")
            
            self.analyze_feature_importance()
            self.save_model()
            
        except Exception as e:
            logging.error(f"❌ 训练失败: {e}")

    def analyze_feature_importance(self):
        """输出特征重要性排行榜"""
        if self.model is None: return
        
        importances = self.model.feature_importances_
        # 将特征名和重要性组合
        feature_imp = pd.DataFrame({
            'Feature': self.feature_names,
            'Importance': importances
        }).sort_values(by='Importance', ascending=False)
        
        print("🏆 薪资影响因子排行榜 (Top 10):")
        print(feature_imp.head(10).to_string(index=False))
        print("\n")

    def save_model(self):
        """保存模型与特征元数据"""
        if self.model is None: return
        
        payload = {
            'model': self.model,
            'feature_names': self.feature_names # 非常重要：预测时需要对照这个顺序
        }
        
        try:
            joblib.dump(payload, self.model_path)
            logging.info(f"💾 模型已成功保存至: {self.model_path}")
            logging.info("✅ 你现在可以运行 visual_app.py 启动可视化大屏了！")
        except Exception as e:
            logging.error(f"保存模型失败: {e}")

if __name__ == "__main__":
    predictor = SalaryPredictor()
    predictor.train()
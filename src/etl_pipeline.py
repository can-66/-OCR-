# src/etl_pipeline.py

import pandas as pd
import numpy as np
import re
import jieba
from sqlalchemy import create_engine
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataPipeline:
    def __init__(self, csv_path='data/real_job_data.csv', db_path='sqlite:///data/job_data.db'):
        self.csv_path = csv_path
        
        # --- 路径自动修复逻辑 ---
        base_dir = os.getcwd()
        if 'src' in base_dir: 
            base_dir = os.path.dirname(base_dir)
        
        abs_db_path = os.path.join(base_dir, 'data', 'job_data.db')
        self.db_engine_url = f'sqlite:///{abs_db_path}'
        self.engine = create_engine(self.db_engine_url)
        self.clean_table = 'cleaned_jobs_with_features'
        
        logging.info(f"🔧 ETL 初始化 | 目标数据库: {self.db_engine_url}")

    def load_data(self):
        """加载数据，兼容多种编码"""
        # 自动寻找 CSV 文件
        if not os.path.exists(self.csv_path):
            alt_path = os.path.join('data', 'real_job_data.csv')
            if os.path.exists(alt_path):
                self.csv_path = alt_path
            else:
                raise FileNotFoundError(f"找不到 CSV 文件: {self.csv_path}")

        try:
            df = pd.read_csv(self.csv_path, encoding='utf-8-sig')
        except:
            df = pd.read_csv(self.csv_path, encoding='gbk')
        
        logging.info(f"📥 原始数据加载成功: {len(df)} 行")
        return df

    def _extract_city_smart(self, raw_text):
        """
        🔥 核心修复：从原始文本中提取真实城市
        策略：倒序扫描 (Bottom-Up)，避开公司名干扰
        """
        if not isinstance(raw_text, str):
            return "未知"
            
        lines = raw_text.split('\n')
        target_cities = ['南京', '北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '西安', '苏州', '长沙', '重庆', '合肥']
        company_keywords = ['公司', '科技', '集团', '银行', '软件', '服务', '中心', '大学', '厂', '局']

        # 【关键步骤】从最后一行往上看！因为地点通常在卡片底部
        for line in reversed(lines):
            line = line.strip()
            if not line: continue
            
            # 1. 优先匹配带“·”的格式 (如 "南京·江宁区")，这是最准确的地点特征
            if '·' in line:
                # 检查点号前面是不是城市
                part1 = line.split('·')[0]
                if any(city in part1 for city in target_cities):
                    return part1 # 找到真实城市，直接返回！

            # 2. 如果没有点号，检查是否包含城市名，且不是公司名
            matched_city = None
            for city in target_cities:
                if city in line:
                    matched_city = city
                    break
            
            if matched_city:
                # 必须进行二次校验：这行是不是公司名？
                is_company = False
                for kw in company_keywords:
                    if kw in line:
                        is_company = True
                        break
                
                # 特例：如果这行字数很少，且就是城市名 (如 "南京")，那它肯定不是公司
                if len(line) <= 3 and matched_city == line:
                    return matched_city
                
                # 如果包含城市但没有公司关键词，采纳
                if not is_company:
                    return matched_city
                    
        return "未知"

    def _parse_salary(self, salary_str):
        """
        🔥 强力修复版薪资解析
        """
        if pd.isna(salary_str): return np.nan, np.nan, np.nan
        s = str(salary_str).upper().strip()
        
        # === 优先处理标准格式 "15-25K" 或 "15-25" ===
        # 修复：优先匹配带横线的格式，避免"5-6K"被误解析
        if '-' in s:
            match = re.search(r'(\d+)\s*-\s*(\d+)', s)
            if match:
                low = int(match.group(1))
                high = int(match.group(2))
                # 验证合理性：如果两个数字都很小（<50），很可能是K单位
                # 如果数字较大（>50），可能是元单位，需要除以1000
                if low > 50 or high > 50:
                    # 可能是元单位，转换为K
                    if low > 1000:
                        low = low / 1000
                        high = high / 1000
                    else:
                        # 可能是10K-20K格式，已经是K单位
                        pass
                # 安全检查
                if low > 200 or high > 200:
                    return np.nan, np.nan, np.nan
                if low > high:
                    low, high = high, low
                # 转为实际数值 (单位：元)
                return low * 1000, high * 1000, (low + high) * 500
        
        # === 处理粘连数字（如"2140K"）===
        nums = re.findall(r'\d+', s)
        if not nums: return np.nan, np.nan, np.nan
        
        raw_val = int(nums[0])
        
        # 情况A: 如果数字巨大 (比如 > 200)，且看起来像粘连数字
        if raw_val > 200: 
            s_val = str(raw_val)
            if len(s_val) == 4: # 2140 -> 21, 40
                low = int(s_val[:2])
                high = int(s_val[2:])
            elif len(s_val) == 3: # 812 -> 8, 12
                low = int(s_val[:1])
                high = int(s_val[1:])
            else:
                return np.nan, np.nan, np.nan
        else:
            # 情况B: 单个数字 "15K"
            low = raw_val
            high = raw_val

        # === 安全卫士 (Safety Guard) ===
        if low > 200 or high > 200:
            return np.nan, np.nan, np.nan
            
        # 逻辑错误检查 (低 > 高)
        if low > high:
            low, high = high, low
            
        # 转为实际数值 (单位：元)
        return low * 1000, high * 1000, (low + high) * 500

    def _parse_exp_degree(self, text):
        """同时提取经验和学历"""
        exp, deg = 0, 0  # 修改：默认学历为0（未知），而不是1（大专）
        if not isinstance(text, str): return exp, deg
        
        # 经验
        if re.search(r'(\d+)-(\d+)年', text):
            m = re.search(r'(\d+)-(\d+)年', text)
            exp = (float(m.group(1)) + float(m.group(2))) / 2
        elif "应届" in text or "在校" in text:
            exp = 0.5
        elif re.search(r'(\d+)年', text):
            exp = float(re.search(r'(\d+)年', text).group(1))
            
        # 学历 - 修复：优先检查"学历不限"，避免被错误标记
        if "学历不限" in text or "学历要求" in text and "不限" in text:
            deg = 0  # 学历不限标记为0
        elif "博士" in text: 
            deg = 4
        elif "硕士" in text: 
            deg = 3
        elif "本科" in text: 
            deg = 2
        elif "大专" in text: 
            deg = 1
        # 如果没有找到任何学历信息，保持deg=0（未知）
        
        return exp, deg

    def clean_data(self, df):
        logging.info("🧹 开始清洗...")
        
        # --- 1. 修复城市错误 (City Fix) ---
        # 不再信任爬虫原本的 city 列，重新从 raw_text 提取
        df['real_city'] = df['raw_text'].apply(self._extract_city_smart)
        # 覆盖原列
        df['city'] = df['real_city']
        df['city_clean'] = df['real_city'] # 用于后续分析的干净列
        
        # --- 2. 薪资解析 ---
        salary_feats = df['salary'].apply(lambda x: self._parse_salary(x))
        df['min_salary'], df['max_salary'], df['avg_salary'] = zip(*salary_feats)
        df = df.dropna(subset=['avg_salary']) # 剔除无效薪资
        
        # --- 2.5. 异常值过滤（在ETL阶段就过滤，确保数据质量）---
        original_count = len(df)
        # 过滤异常高薪（>5万/月，对于数据分析师岗位通常异常）
        # 过滤异常低薪（<2000/月，可能是日薪或兼职）
        df = df[(df['avg_salary'] < 50000) & (df['avg_salary'] > 2000)].copy()
        filtered_count = original_count - len(df)
        if filtered_count > 0:
            logging.info(f"   ⚠️ 已过滤 {filtered_count} 个异常薪资样本（确保数据质量）")
        
        # --- 3. 经验学历 ---
        exp_deg = df['raw_text'].apply(self._parse_exp_degree)
        df['exp_years'], df['degree_value'] = zip(*exp_deg)
        
        return df

    def feature_engineering(self, df):
        logging.info("🧠 生成 NLP 特征...")
        skills = ['Python', 'SQL', 'Excel', 'Tableau', 'PowerBI', 'Spark', 'Hadoop', 'Machine Learning', 'Java']
        
        text_data = (df['title'] + ' ' + df['raw_text']).fillna('').str.lower()
        
        for skill in skills:
            df[f'has_{skill}'] = text_data.apply(lambda x: 1 if skill.lower() in x else 0)
            
        return df

    def run(self):
        df = self.load_data()
        df = self.clean_data(df)
        df = self.feature_engineering(df)
        
        # 打印修复后的对比，让你放心
        logging.info("📊 城市修复效果抽查:")
        print(df[['city_clean', 'raw_text']].tail(5).to_string())
        
        df.to_sql(self.clean_table, self.engine, if_exists='replace', index=False)
        logging.info(f"✅ 处理完成，数据已存入数据库: {len(df)} 条")

if __name__ == "__main__":
    DataPipeline().run()
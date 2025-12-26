# src/export_analysis.py

import pandas as pd
import numpy as np
import sqlite3
import os
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# 配置绘图风格 (解决中文乱码)
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS'] # 适配 Windows/Mac
plt.rcParams['axes.unicode_minus'] = False
sns.set(style="whitegrid", palette="muted")

class DataExporter:
    def __init__(self):
        # 1. 智能路径识别
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if 'src' not in base_dir and 'DataScience_Pro' not in base_dir: 
            base_dir = os.getcwd()
            
        self.data_dir = os.path.join(base_dir, 'data')
        self.db_path = os.path.join(self.data_dir, 'job_data.db')
        
        # 输出文件路径
        self.output_csv = os.path.join(self.data_dir, 'final_clean_dataset.csv')
        self.output_excel = os.path.join(self.data_dir, 'final_clean_dataset.xlsx')
        self.output_img = os.path.join(self.data_dir, 'salary_verification_plot.png')

    def load_and_verify(self):
        logging.info(f"📥 正在连接数据库: {self.db_path}")
        if not os.path.exists(self.db_path):
            logging.error("❌ 数据库不存在！请先运行 etl_pipeline.py")
            return None

        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql("SELECT * FROM cleaned_jobs_with_features", conn)
        conn.close()
        
        logging.info(f"   初始数据量: {len(df)} 条")
        
        # --- 2. 深度清洗 (Deep Cleaning) ---
        # 再次过滤可能的极值 (月薪 > 10万通常为异常，除非是总监级)
        # 同时也过滤掉过低的薪资 (< 2000)，可能是日薪或兼职
        # 修复：更严格的异常值过滤，对于数据分析师岗位，月薪超过5万通常异常
        df_clean = df[
            (df['avg_salary'] < 50000) &  # 更严格的阈值：5万/月
            (df['avg_salary'] > 2000)
        ].copy()
        
        logging.info(f"   剔除异常极值后: {len(df_clean)} 条 (剔除 {len(df) - len(df_clean)} 条)")
        
        # --- 2.5. 关键修复：过滤掉学历不限的样本（用于学历分析时）---
        # 注意：这里不删除，只是记录，因为export_analysis可能用于查看所有数据
        # 但在模型训练时已经过滤了
        
        return df_clean

    def process_for_export(self, df):
        logging.info("⚙️ 正在标准化数据格式...")
        
        # 1. 还原学历标签
        degree_map = {1: '大专', 2: '本科', 3: '硕士', 4: '博士', 0: '学历不限'}
        df['学历标签'] = df['degree_value'].map(degree_map).fillna('其他')
        
        # 2. 整理技能列
        # 将所有 has_xxx=1 的列合并为一个 "技能标签" 列，方便Excel查看
        skill_cols = [c for c in df.columns if c.startswith('has_')]
        
        def get_skill_tags(row):
            skills = []
            for col in skill_cols:
                if row[col] == 1:
                    skills.append(col.replace('has_', ''))
            return ",".join(skills) if skills else "无"
            
        df['技能标签'] = df.apply(get_skill_tags, axis=1)
        
        # 3. 选取易读的列进行导出
        export_cols = [
            'title', 'city_clean', '学历标签', 'exp_years', 
            'min_salary', 'max_salary', 'avg_salary', 
            '技能标签', 'raw_text'
        ]
        
        # 重命名中文列头，方便阅读
        rename_dict = {
            'title': '职位名称',
            'city_clean': '城市',
            'exp_years': '经验要求(年)',
            'min_salary': '薪资下限',
            'max_salary': '薪资上限',
            'avg_salary': '平均月薪',
            'raw_text': '原始描述'
        }
        
        final_df = df[export_cols].rename(columns=rename_dict)
        return final_df, df  # 返回两个df，final用于导出，df用于画图

    def visualize_health_check(self, df):
        """生成体检报告图：学历 vs 薪资"""
        logging.info("📊 正在生成数据体检报告图...")
        
        plt.figure(figsize=(10, 6))
        
        # 强制指定顺序，确保 大专 在左，博士 在右
        order = ['大专', '本科', '硕士', '博士']
        
        # 画箱线图
        sns.boxplot(x='学历标签', y='avg_salary', data=df, order=order, palette="Set3")
        
        plt.title('【数据质量检查】各学历薪资分布 (清洗后)', fontsize=14)
        plt.ylabel('平均月薪 (元)', fontsize=12)
        plt.xlabel('学历', fontsize=12)
        
        # 保存图片
        plt.savefig(self.output_img, dpi=300, bbox_inches='tight')
        logging.info(f"✅ 体检报告已保存: {self.output_img}")
        logging.info("   (请打开该图片，确认 '硕士' 的箱体位置高于 '大专'，即为正常)")

    def run(self):
        df = self.load_and_verify()
        if df is None or df.empty:
            logging.error("没有数据可处理。")
            return

        final_df, raw_df = self.process_for_export(df)
        
        # 导出 CSV (通用)
        final_df.to_csv(self.output_csv, index=False, encoding='utf-8-sig')
        logging.info(f"💾 CSV 数据集已导出: {self.output_csv}")
        
        # 导出 Excel (适合人工看)
        try:
            final_df.to_excel(self.output_excel, index=False)
            logging.info(f"💾 Excel 数据集已导出: {self.output_excel}")
        except ImportError:
            logging.warning("⚠️ 未安装 openpyxl，跳过 Excel 导出 (pip install openpyxl 即可解决)")
            
        # 统计打印
        print("\n" + "="*30)
        print("📊 数据集最终统计")
        print("="*30)
        print(f"总记录数: {len(final_df)}")
        print(f"平均月薪: ¥{int(final_df['平均月薪'].mean())}")
        print("\n按学历统计均值:")
        print(final_df.groupby('学历标签')['平均月薪'].mean().sort_values().to_string())
        print("="*30 + "\n")
        
        # 画图
        try:
            self.visualize_health_check(raw_df)
        except Exception as e:
            logging.warning(f"绘图失败 (可能是字体原因，不影响数据导出): {e}")

if __name__ == "__main__":
    exporter = DataExporter()
    exporter.run()
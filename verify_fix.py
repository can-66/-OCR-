"""
验证修复效果的脚本
运行此脚本可以检查：
1. 学历不限样本是否被正确过滤
2. 异常高薪样本是否被过滤
3. 各学历的平均薪资是否合理
"""
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///data/job_data.db')
df = pd.read_sql('SELECT * FROM cleaned_jobs_with_features', engine)

print('='*70)
print('📊 数据质量检查报告')
print('='*70)

# 1. 检查学历分布
print('\n1. 学历分布:')
degree_dist = df['degree_value'].value_counts().sort_index()
print(degree_dist)
print(f'   学历不限(0)样本数: {len(df[df["degree_value"]==0])}')

# 2. 检查异常薪资
print('\n2. 异常薪资检查:')
high_salary = df[df['avg_salary'] > 50000]
low_salary = df[df['avg_salary'] < 2000]
print(f'   异常高薪(>5万)样本数: {len(high_salary)}')
print(f'   异常低薪(<2000)样本数: {len(low_salary)}')

# 3. 过滤后的学历薪资统计（模拟模型训练时的过滤）
print('\n3. 过滤后的数据统计（模拟模型训练）:')
df_filtered = df[(df['degree_value'] > 0) & (df['avg_salary'] < 50000) & (df['avg_salary'] > 2000)].copy()
print(f'   过滤后总样本数: {len(df_filtered)} (原始: {len(df)})')

if len(df_filtered) > 0:
    degree_stats = df_filtered.groupby('degree_value')['avg_salary'].agg(['mean', 'count', 'median'])
    degree_map = {1: '大专', 2: '本科', 3: '硕士', 4: '博士'}
    degree_stats.index = degree_stats.index.map(degree_map)
    print('\n   各学历平均薪资（过滤后）:')
    print(degree_stats)
    
    # 验证合理性
    print('\n4. 合理性验证:')
    if 2 in df_filtered['degree_value'].values and 1 in df_filtered['degree_value'].values:
        bachelor_mean = df_filtered[df_filtered['degree_value']==2]['avg_salary'].mean()
        college_mean = df_filtered[df_filtered['degree_value']==1]['avg_salary'].mean()
        if bachelor_mean > college_mean:
            print(f'   ✅ 本科平均薪资({bachelor_mean:.0f}) > 大专平均薪资({college_mean:.0f}) - 合理！')
        else:
            print(f'   ⚠️  本科平均薪资({bachelor_mean:.0f}) < 大专平均薪资({college_mean:.0f}) - 仍需检查！')
    
    if 3 in df_filtered['degree_value'].values and 2 in df_filtered['degree_value'].values:
        master_mean = df_filtered[df_filtered['degree_value']==3]['avg_salary'].mean()
        bachelor_mean = df_filtered[df_filtered['degree_value']==2]['avg_salary'].mean()
        if master_mean > bachelor_mean:
            print(f'   ✅ 硕士平均薪资({master_mean:.0f}) > 本科平均薪资({bachelor_mean:.0f}) - 合理！')
        else:
            print(f'   ⚠️  硕士平均薪资({master_mean:.0f}) < 本科平均薪资({bachelor_mean:.0f}) - 仍需检查！')

print('\n' + '='*70)
print('💡 提示：如果数据仍不合理，请重新运行 etl_pipeline.py 和 model_train.py')
print('='*70)


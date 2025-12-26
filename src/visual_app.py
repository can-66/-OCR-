# src/visual_app.py

import streamlit as st
import pandas as pd
import joblib
import plotly.express as px
import os
from sqlalchemy import create_engine

# ==========================================
# 1. 页面基础配置 (Page Config)
# ==========================================
st.set_page_config(
    page_title="数据分析岗位薪资罗盘",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 注入自定义 CSS 美化界面
st.markdown("""
    <style>
    .main {background-color: #f8f9fa;}
    .stMetric {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    h1, h2, h3 {color: #2c3e50;}
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 核心数据加载 (Data & Model Loading)
# ==========================================

@st.cache_data
def load_data():
    """从 SQLite 数据库加载清洗后的数据"""
    # 智能路径定位
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = f'sqlite:///{os.path.join(base_dir, "data", "job_data.db")}'
    
    try:
        engine = create_engine(db_path)
        query = "SELECT * FROM cleaned_jobs_with_features"
        df = pd.read_sql(query, engine)
        
        # 【关键修复】数据增强：将数值型 degree_value 映射回中文标签，方便画图
        degree_map = {1: '大专', 2: '本科', 3: '硕士', 4: '博士', 0: '学历不限'}
        df['degree_label'] = df['degree_value'].map(degree_map).fillna('其他')
        
        return df
    except Exception as e:
        st.error(f"❌ 无法加载数据: {e}")
        return pd.DataFrame()

@st.cache_resource
def load_model_payload():
    """加载模型文件 (Model + Feature Names)"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    model_path = os.path.join(base_dir, 'data', 'salary_model.pkl')
    
    if not os.path.exists(model_path):
        st.error("❌ 找不到模型文件！请先运行 model_train.py")
        return None, None
        
    try:
        payload = joblib.load(model_path)
        # 注意：这里要用 'feature_names'，与 model_train.py 保持一致
        return payload['model'], payload['feature_names']
    except Exception as e:
        st.error(f"模型加载失败: {e}")
        return None, None

# 加载资源
df = load_data()
model, model_features = load_model_payload()

# ==========================================
# 3. 侧边栏与导航
# ==========================================
st.sidebar.image("https://img.icons8.com/color/96/000000/python.png", width=80)
st.sidebar.title("DataJob Pro 📊")
st.sidebar.caption("基于 Python 全栈的数据科学岗位分析系统")

app_mode = st.sidebar.radio("📌 功能导航", ["全景市场洞察", "AI 薪资预测器"])

st.sidebar.markdown("---")
st.sidebar.info(f"📅 数据版本: {pd.Timestamp.now().strftime('%Y-%m-%d')}")
st.sidebar.info(f"🔢 样本总量: {len(df)} 条")

# ==========================================
# 4. 模块一：全景市场洞察 (Dashboard)
# ==========================================
if app_mode == "全景市场洞察":
    st.title("🏙️ 数据分析师岗位 · 全景市场洞察")
    
    if df.empty:
        st.warning("暂无数据，请检查爬虫和清洗脚本。")
    else:
        # --- 顶栏 KPI ---
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("平均月薪 (Mean)", f"¥{int(df['avg_salary'].mean()):,}", "基础盘")
        col2.metric("薪资中位数 (Median)", f"¥{int(df['avg_salary'].median()):,}", "真实水平")
        col3.metric("最高年薪估算", f"¥{int(df['avg_salary'].max() * 14 / 10000)} 万", "天花板")
        col4.metric("覆盖城市", f"{df['city_clean'].nunique()} 个", "地域广度")
        
        st.markdown("---")

        # --- 图表区 ---
        c1, c2 = st.columns([3, 2])
        
        with c1:
            st.subheader("📍 各城市薪资竞争力排行")
            # 聚合分析
            city_stats = df.groupby('city_clean')['avg_salary'].agg(['mean', 'count']).reset_index()
            city_stats = city_stats[city_stats['count'] > 1].sort_values('mean', ascending=True) # 过滤掉样本太少的孤点
            
            fig_bar = px.bar(city_stats, x='mean', y='city_clean', orientation='h',
                             text_auto='.0f',
                             labels={'mean': '平均月薪', 'city_clean': '城市'},
                             color='mean', color_continuous_scale='Viridis')
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with c2:
            st.subheader("🎓 学历门槛分布")
            degree_dist = df['degree_label'].value_counts().reset_index()
            degree_dist.columns = ['学历', '占比']
            fig_pie = px.pie(degree_dist, values='占比', names='学历', hole=0.4,
                             color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig_pie, use_container_width=True)

        # --- 技能价值分析 (NLP特征) ---
        st.subheader("🛠️ 硬技能 '含金量' 分析")
        st.caption("分析掌握某项技能的岗位比未掌握该技能的岗位平均高出多少薪资")
        
        # 自动扫描所有 has_ 开头的列
        skill_cols = [c for c in df.columns if c.startswith('has_')]
        skill_data = []
        
        for col in skill_cols:
            skill_name = col.replace('has_', '')
            # 计算溢价
            salary_with = df[df[col] == 1]['avg_salary'].mean()
            salary_without = df[df[col] == 0]['avg_salary'].mean()
            
            if pd.notna(salary_with) and pd.notna(salary_without):
                premium = salary_with - salary_without
                skill_data.append({'技能': skill_name, '薪资溢价': premium})
        
        if skill_data:
            df_skill = pd.DataFrame(skill_data).sort_values('薪资溢价', ascending=False)
            fig_skill = px.bar(df_skill, x='技能', y='薪资溢价',
                               color='薪资溢价', color_continuous_scale='Magma',
                               text_auto='.0f')
            st.plotly_chart(fig_skill, use_container_width=True)

# ==========================================
# 5. 模块二：AI 薪资预测器 (Predictor)
# ==========================================
elif app_mode == "AI 薪资预测器":
    st.title("🤖 AI 薪资预测助手")
    st.markdown("输入您的背景，**随机森林模型 (Random Forest)** 将为您评估市场价值。")
    
    if model is None:
        st.error("模型未加载，无法使用预测功能。")
    else:
        # 布局：左侧输入，右侧结果
        col_input, col_pred = st.columns([1, 1])
        
        with col_input:
            st.markdown("### 📝 您的画像")
            # 1. 城市选择 (从数据中动态获取)
            valid_cities = sorted(df['city_clean'].dropna().unique())
            in_city = st.selectbox("意向城市", valid_cities)
            
            # 2. 学历选择 (映射回数值)
            degree_dict = {'大专': 1, '本科': 2, '硕士': 3, '博士': 4}
            in_degree_str = st.selectbox("最高学历", list(degree_dict.keys()), index=1)
            in_degree_val = degree_dict[in_degree_str]
            
            # 3. 经验 (数值)
            in_exp = st.slider("工作经验 (年)", 0.0, 10.0, 3.0, 0.5)
            
            # 4. 技能 (多选)
            # 动态获取模型训练时用到的技能
            model_skill_feats = [f.replace('has_', '') for f in model_features if f.startswith('has_')]
            # 去重并排序
            skill_options = sorted(list(set(model_skill_feats)))
            in_skills = st.multiselect("掌握技能", skill_options, default=['Python', 'SQL'])

        with col_pred:
            st.markdown("### 💰 评估报告")
            
            predict_btn = st.button("🚀 开始 AI 估价", type="primary", use_container_width=True)
            
            if predict_btn:
                # --- 核心预测逻辑 (必须与训练时完全一致) ---
                try:
                    # 1. 创建一个全零向量，列名与模型训练时一致
                    input_df = pd.DataFrame(0, index=[0], columns=model_features)
                    
                    # 2. 填充数值特征
                    input_df['exp_years'] = in_exp        # 对应 model_train 中的 exp_years
                    
                    # 3. 填充学历 (One-Hot) - 修复：学历现在是类别特征
                    # 训练时用的列名是 "degree_value_1"（大专）、"degree_value_2"（本科）等
                    degree_col = f'degree_value_{in_degree_val}'
                    if degree_col in input_df.columns:
                        input_df[degree_col] = 1
                    else:
                        st.warning(f"注意：模型训练数据中缺乏学历值 '{in_degree_val}' 的样本，预测可能不准。")
                    
                    # 4. 填充城市 (One-Hot)
                    # 训练时用的列名是 "city_clean_南京"，所以这里要拼接
                    city_col = f'city_clean_{in_city}'
                    if city_col in input_df.columns:
                        input_df[city_col] = 1
                    else:
                        st.warning(f"注意：模型训练数据中缺乏 '{in_city}' 的样本，预测可能不准。")
                        
                    # 5. 填充技能
                    for skill in in_skills:
                        skill_col = f'has_{skill}'
                        if skill_col in input_df.columns:
                            input_df[skill_col] = 1
                            
                    # 5. 预测
                    pred_salary = model.predict(input_df)[0]
                    
                    # 6. 显示结果
                    st.balloons()
                    st.success("预测完成！")
                    
                    metric_col1, metric_col2 = st.columns(2)
                    metric_col1.metric("预测月薪", f"¥{int(pred_salary):,}")
                    metric_col2.metric("预测年薪 (13薪)", f"¥{int(pred_salary * 13):,}")
                    
                    # 简单的建议逻辑
                    st.info(f"📋 分析建议：在 {in_city} 拥有 {in_exp} 年经验的 {in_degree_str} 数据分析师，"
                            f"当前市场价值约为 {int(pred_salary)} 元。")
                    
                except Exception as e:
                    st.error(f"预测过程出错: {e}")
                    st.code("Debug Info: " + str(e))
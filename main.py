# main.py
"""
DataScience_Pro 主程序入口
执行完整的数据处理流程：ETL清洗 -> 模型训练 -> 数据导出
"""
import os
from src.etl_pipeline import DataPipeline
from src.model_train import SalaryPredictor
from src.export_analysis import DataExporter

def main():
    """
    主流程函数
    1. 数据清洗（ETL）
    2. 模型训练
    3. 数据导出与验证
    """
    # 确保data目录存在（在项目根目录下）
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    print("="*70)
    print("🚀 DataScience_Pro 数据处理流程启动")
    print("="*70)
    
    # ==========================================
    # 阶段一：数据清洗（ETL Pipeline）
    # ==========================================
    print("\n>>> 阶段一：数据工程任务（ETL清洗）<<<")
    print("-" * 70)
    print("功能：薪资解析、城市修复、学历提取、NLP特征工程")
    
    user_input = input("\n是否执行ETL清洗？(yes/no): ").strip().lower()
    if user_input == 'yes':
        pipeline = DataPipeline()
        pipeline.run()  # 执行ETL清洗，包括：薪资解析、城市修复、学历提取、NLP特征工程
        print("\n✅ ETL清洗完成！数据已存入数据库: data/job_data.db")
    else:
        print("⏭️  跳过ETL清洗步骤")
    
    # ==========================================
    # 阶段二：机器学习建模
    # ==========================================
    print("\n>>> 阶段二：机器学习建模任务 <<<")
    print("-" * 70)
    print("功能：随机森林回归模型训练、特征重要性分析")
    
    user_input = input("\n是否执行模型训练？(yes/no): ").strip().lower()
    if user_input == 'yes':
        predictor = SalaryPredictor()
        predictor.train()  # 内部会调用load_and_preprocess()，然后训练模型
        print("\n✅ 模型训练完成！模型文件已保存: data/salary_model.pkl")
    else:
        print("⏭️  跳过模型训练步骤")
    
    # ==========================================
    # 阶段三：数据导出与验证
    # ==========================================
    print("\n>>> 阶段三：数据导出与质量验证 <<<")
    print("-" * 70)
    print("功能：导出清洗后的数据，生成可视化报告")
    
    user_input = input("\n是否执行数据导出？(yes/no): ").strip().lower()
    if user_input == 'yes':
        exporter = DataExporter()
        exporter.run()  # 导出清洗后的数据，生成可视化报告
        print("\n✅ 数据导出完成！")
        print("   - CSV文件: data/final_clean_dataset.csv")
        print("   - Excel文件: data/final_clean_dataset.xlsx")
        print("   - 质量验证图: data/salary_verification_plot.png")
    else:
        print("⏭️  跳过数据导出步骤")
    
    # ==========================================
    # 完成提示
    # ==========================================
    print("\n" + "="*70)
    print("🎉 流程执行完成！")
    print("="*70)
    print("\n📌 下一步操作：")
    print("   运行以下命令启动可视化大屏：")
    print("   streamlit run src/visual_app.py")
    print("\n" + "="*70)

if __name__ == "__main__":
    main()
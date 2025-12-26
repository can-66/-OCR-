import time
import random
import os
import re
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import ddddocr
import logging

# 配置日志输出
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class SpiderFinal:
    def __init__(self):
        # 1. 配置浏览器选项
        options = uc.ChromeOptions()
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-renderer-backgrounding") # 后台保活
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--window-size=1920,1080")
        
        logging.info(">>> 启动【数据分析大师修复版】爬虫...")
        self.driver = uc.Chrome(options=options)
        self.data_list = []
        
        # 2. 路径配置
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # 如果当前脚本直接在根目录运行，调整一下逻辑
        if 'src' not in base_dir and 'DataScience_Pro' not in base_dir: 
            # 兼容单文件运行情况
            base_dir = os.getcwd()
            
        data_dir = os.path.join(base_dir, 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        self.csv_path = os.path.join(data_dir, 'real_job_data.csv')
        
        logging.info(f"📂 数据将保存至: {self.csv_path}")

        # 3. 加载OCR模型
        logging.info("🧠 正在加载 OCR 模型 (ddddocr)...")
        self.ocr = ddddocr.DdddOcr(show_ad=False)

    def clean_salary(self, raw_str):
        """
        核心修复逻辑：清洗OCR结果，解决数字粘连问题
        输入: "2040K", "15-20K", "双休"
        输出: "20-40K", "15-20K", None
        """
        if not raw_str:
            return None
            
        # 1. 基础清洗：去空格，转大写，只留关键字符
        # 允许的字符：数字, K, -, .
        s = "".join([c for c in raw_str if c.isalnum() or c in ['-', '.']]).upper()
        
        # 2. 完美情况：直接匹配到 "15-25K" 或 "15-25K.13薪"
        # 正则含义：数字 + 连字符 + 数字 + K
        if re.search(r'\d+-\d+K', s):
            # 提取标准部分，忽略后面的 ".13薪" 等杂讯，保持纯净
            match = re.search(r'(\d+-\d+K)', s)
            return match.group(1) if match else s

        # 3. 修复 "粘连" 情况 (Feature: Smart Split)
        # OCR 经常把 "20-40K" 识别成 "2040K"
        match_fused = re.search(r'^(\d+)K', s)
        if match_fused:
            num_str = match_fused.group(1)
            val = int(num_str)
            
            # 逻辑：如果数字 > 100 (月薪通常不会是 100K 以上的单数)，且长度为 3 或 4 位，尝试拆分
            # 例如: 2040 -> 20, 40
            if val > 100:
                # 4位数字 (1525 -> 15-25)
                if len(num_str) == 4:
                    part1, part2 = num_str[:2], num_str[2:]
                    if int(part1) < int(part2):
                        return f"{part1}-{part2}K"
                # 3位数字 (812 -> 8-12)
                elif len(num_str) == 3:
                    part1, part2 = num_str[:1], num_str[1:]
                    if int(part1) < int(part2):
                        return f"{part1}-{part2}K"
            
            # 如果是合理的单数 (例如 "30K")，保留
            if val <= 200: 
                return s

        return None # 无法解析的脏数据 (如 "双休绩效")

    def get_salary_by_ocr(self, card_element):
        """定位元素并截图识别"""
        try:
            salary_el = None
            try:
                # 优先找 class="salary"
                salary_el = card_element.find_element(By.CSS_SELECTOR, ".salary")
            except:
                try:
                    # 备选：找文本包含 'K' 的 span
                    salary_el = card_element.find_element(By.XPATH, ".//span[contains(text(), 'K')]")
                except:
                    pass
            
            if salary_el:
                # 截图 - 使用二进制流，不落地文件
                png = salary_el.screenshot_as_png
                res = self.ocr.classification(png)
                return res
        except:
            pass
        return None

    def start(self, target_count=300):
        self.driver.get("https://www.zhipin.com/")
        
        print("\n" + "="*50)
        print("🚨【最后一步指令】")
        print("1. 请在浏览器上手动扫码登录 Boss直聘。")
        print("2. 在搜索框输入 '数据分析师' 并搜索。")
        print("3. 筛选城市（推荐选 '全国' 或特定城市）。")
        print("4. 确保页面已显示职位列表。")
        print("="*50 + "\n")
        
        input(">>> 准备好了吗？按回车键 (Enter) 启动自动抓取 <<<")
        
        while len(self.data_list) < target_count:
            try:
                # 查找所有职位卡片 li 标签
                job_cards = self.driver.find_elements(By.CSS_SELECTOR, ".job-card-wrapper")
                
                # 如果没找到特定的 class，尝试找 li (兼容性)
                if not job_cards:
                     job_cards = [li for li in self.driver.find_elements(By.TAG_NAME, "li") 
                                  if "K" in li.text and "年" in li.text]

                if not job_cards:
                    logging.warning("当前页面未找到职位卡片，尝试滚动刷新...")
                    self.auto_scroll()
                    time.sleep(3)
                    continue

                logging.info(f"📸 本页扫描到 {len(job_cards)} 个职位，开始处理...")
                
                new_count = 0
                for card in job_cards:
                    try:
                        full_text = card.text
                        
                        # 1. 查重
                        if any(d['raw_text'] == full_text for d in self.data_list):
                            continue

                        # 2. 获取并【清洗】薪资
                        raw_ocr_salary = self.get_salary_by_ocr(card)
                        real_salary = self.clean_salary(raw_ocr_salary)

                        # 【关键】如果薪资清洗失败（是脏数据），直接跳过！
                        if not real_salary:
                            # logging.warning(f"丢弃无效数据: OCR原值='{raw_ocr_salary}'")
                            continue

                        # 3. 提取其他信息
                        lines = full_text.split('\n')
                        title = lines[0]
                        
                        # 简单的城市提取逻辑
                        city = "未知"
                        common_cities = ['北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '南京', '西安', '苏州', '长沙', '重庆']
                        for l in lines:
                            for c in common_cities:
                                if c in l and len(l) < 10: # 城市行通常比较短
                                    city = l
                                    break
                            if city != "未知": break
                        
                        # 如果还没找到，尝试取最后一行或倒数第二行（Boss常用布局）
                        if city == "未知" and len(lines) > 2:
                             # 简单的启发式：通常地址在中间或后面
                             pass 

                        item = {
                            'title': title,
                            'salary': real_salary,
                            'city': city,
                            'raw_text': full_text
                        }
                        
                        self.data_list.append(item)
                        new_count += 1
                        print(f"   [✅捕获] {title[:8]}... | 薪资: {real_salary} (原识别: {raw_ocr_salary})")

                        if len(self.data_list) >= target_count:
                            break

                    except Exception as e:
                        continue

                # 批次保存
                if new_count > 0:
                    self.save()
                    logging.info(f"💾 已保存 {len(self.data_list)} 条数据")
                else:
                    logging.info("本页无新数据，继续滚动...")

                if len(self.data_list) >= target_count:
                    logging.info("🎉 任务圆满完成！")
                    break

                self.auto_scroll()
                # 随机等待，模拟人类
                time.sleep(random.uniform(3, 5))

            except Exception as e:
                logging.error(f"主循环错误: {e}")
                time.sleep(3)

    def auto_scroll(self):
        try:
            self.driver.execute_script("window.scrollBy(0, 800);")
        except:
            pass

    def save(self):
        if self.data_list:
            df = pd.DataFrame(self.data_list)
            # 使用 utf-8-sig 防止 Excel 打开中文乱码
            df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    spider = SpiderFinal()
    try:
        # 设定一个合理的目标数量，比如 500 条
        spider.start(target_count=500)
    except KeyboardInterrupt:
        spider.save()
        print("\n🛑 用户强制停止，数据已保存。")
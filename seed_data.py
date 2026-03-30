import sqlite3
import os
from datetime import datetime, timedelta

DB_NAME = "crm_enterprise.db"

def seed_data():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # 0. 如果表不存在则创建 (整合自 crm_app.py)
    tables = {
        "customers": "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, industry TEXT, level TEXT, address TEXT",
        "contacts": "id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id INTEGER, name TEXT, post TEXT, dept TEXT, phone TEXT, email TEXT, birthday TEXT, is_decision_maker INTEGER, FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE",
        "suppliers": "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, category TEXT, contact_person TEXT, phone TEXT, note TEXT",
        "projects": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT UNIQUE, customer_id INTEGER, project_name TEXT, stage TEXT, loss_reason TEXT, next_visit_date TEXT, FOREIGN KEY (customer_id) REFERENCES customers(id)",
        "follow_ups": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, follow_date TEXT, contact_name TEXT, stage TEXT, detail TEXT, next_plan TEXT, FOREIGN KEY(project_no) REFERENCES projects(project_no)",
        "quotations": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, quote_date TEXT, amount REAL, file_path TEXT, version TEXT, FOREIGN KEY (project_no) REFERENCES projects(project_no)",
        "contracts": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, start_date TEXT, end_date TEXT, total_amount REAL, paid_amount REAL, file_path TEXT, FOREIGN KEY (project_no) REFERENCES projects(project_no)"
    }
    for name, schema in tables.items():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {name} ({schema})")

    # 1. 清理现有数据 (实现干净的“重新放置”)
    tables_to_clean = ["contracts", "quotations", "follow_ups", "projects", "contacts", "suppliers", "customers"]
    for t in tables_to_clean:
        cursor.execute(f"DELETE FROM {t}")
    cursor.execute("DELETE FROM sqlite_sequence") # 重置自增 ID

    # 1. 模拟客户
    customers = [
        ("科大讯飞", "人工智能", "战略客户", "合肥市望江西路666号"),
        ("比亚迪汽车", "新能源", "大客户", "深圳市坪山区比亚迪路3009号"),
        ("字节跳动", "互联网", "潜在客户", "北京市海淀区北三环西路43号"),
        ("宁德时代", "锂离子电池", "大客户", "福建省宁德市漳湾镇新港路1号")
    ]
    cursor.executemany("INSERT OR IGNORE INTO customers (name, industry, level, address) VALUES (?,?,?,?)", customers)
    
    # 获取客户ID
    cursor.execute("SELECT id, name FROM customers")
    cust_map = {name: cid for cid, name in cursor.fetchall()}

    # 2. 模拟联系人
    contacts = [
        (cust_map["科大讯飞"], "张三", "副总裁", "智慧教育事业部", "13800138001", "zhangsan@iflytek.com", "1980-05-20", 1),
        (cust_map["比亚迪汽车"], "李四", "采购总监", "电池供应链", "13912345678", "lisi@byd.com", "1985-08-12", 1),
        (cust_map["宁德时代"], "王五", "研发经理", "电芯开发部", "13788889999", "wangwu@catl.com", "1990-03-30", 0)
    ]
    cursor.executemany("INSERT OR IGNORE INTO contacts (customer_id, name, post, dept, phone, email, birthday, is_decision_maker) VALUES (?,?,?,?,?,?,?,?)", contacts)

    # 3. 模拟项目
    projects = [
        ("PRJ20260327-001", cust_map["科大讯飞"], "语音识别模块集采", "方案报价", "2026-04-10"),
        ("PRJ20260327-002", cust_map["比亚迪汽车"], "刀片电池产线视觉监测", "合同阶段", "2026-04-05"),
        ("PRJ20260327-003", cust_map["宁德时代"], "实验室自动化改造", "初步线索", "2026-04-15")
    ]
    cursor.executemany("INSERT OR IGNORE INTO projects (project_no, customer_id, project_name, stage, next_visit_date) VALUES (?,?,?,?,?)", projects)

    # 4. 模拟跟进记录
    follow_ups = [
        ("PRJ20260327-001", "2026-03-25", "张三", "方案报价", "对方对第三版技术方案比较认可，尤其是在降噪处理部分。但对价格仍有疑虑，需要进一步向其推荐高性价比阶梯报价包。", "准备第四版报价单并与其财务部对接。"),
        ("PRJ20260327-002", "2026-03-26", "李四", "合同阶段", "商务谈判已接近尾声。对方已确认法务审核通过，预计下周一可以正式签署。合同总金额 25.8 万。", "准备合同原件及公司公章，安排下周一拜访。"),
        ("PRJ20260327-003", "2026-03-27", "王五", "初步线索", "初步电话沟通。王五表示目前宁德时代的实验室确实有自动化升级的需求，但预算目前正在排期。建议我们先发一份公司简介和成功案例。", "发送公司 PPT 介绍及类似行业的案例集。")
    ]
    cursor.executemany("INSERT OR IGNORE INTO follow_ups (project_no, follow_date, contact_name, stage, detail, next_plan) VALUES (?,?,?,?,?,?)", follow_ups)

    # 5. 模拟报价
    quotations = [
        ("PRJ20260327-001", "2026-03-20", 125000.0, "quotation_v1.pdf", "V1.0"),
        ("PRJ20260327-001", "2026-03-25", 118000.0, "quotation_v2_final.pdf", "V2.1")
    ]
    cursor.executemany("INSERT OR IGNORE INTO quotations (project_no, quote_date, amount, file_path, version) VALUES (?,?,?,?,?)", quotations)

    # 6. 模拟合同
    contracts = [
        ("PRJ20260327-002", "2026-04-01", "2027-03-31", 258000.0, 50000.0, "contract_sample_byd.pdf")
    ]
    cursor.executemany("INSERT OR IGNORE INTO contracts (project_no, start_date, end_date, total_amount, paid_amount, file_path) VALUES (?,?,?,?,?,?)", contracts)

    conn.commit()
    print("Test data seeded successfully!")
    conn.close()

if __name__ == "__main__":
    seed_data()

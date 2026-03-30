import os
os.environ["QT_API"] = "PyQt5"
import sys
import sqlite3
import traceback
from datetime import datetime, date, timedelta

# 拦截全局未捕获异常
def exception_hook(exctype, value, tb):
    err_msg = "".join(traceback.format_exception(exctype, value, tb))
    print(err_msg, flush=True)
    try:
        from PyQt5.QtWidgets import QMessageBox
        QMessageBox.critical(None, "程序崩溃", f"系统遇到致命错误：\n{err_msg[:500]}...")
    except:
        pass
    sys.exit(1)

sys.excepthook = exception_hook

# Import QApplication first to allow setting attributes and creating instance
from PyQt5.QtCore import Qt, QUrl, QDate, QSize, pyqtSignal, QEvent, QPoint, QTimer
from PyQt5.QtGui import QIcon, QDesktopServices, QColor, QPainter
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QTableWidgetItem, QHeaderView, QFileDialog,
                             QScrollArea, QFrame, QDialog, QFormLayout, QTableWidget,
                             QMenu, QAction, QComboBox, QCompleter, QStackedWidget, QMessageBox)
from PyQt5.QtNetwork import QLocalServer, QLocalSocket

# 启用高 DPI 支持
QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)

# 创建应用实例
app = QApplication.instance()
if not app:
    app = QApplication(sys.argv)

try:
    from qfluentwidgets import (FluentWindow, NavigationItemPosition, Theme, setTheme,
                                CardWidget, BodyLabel, SubtitleLabel, TitleLabel, 
                                TableWidget, PrimaryPushButton, MessageBox,
                                LineEdit, SearchLineEdit, ComboBox, EditableComboBox, DateEdit, TextEdit, Pivot, 
                                InfoBar, InfoBarPosition, FluentIcon as FIF, ScrollArea,
                                ToolButton, TransparentToolButton, ProgressBar, CalendarPicker)
except ImportError:
    # 针对旧版本 qfluentwidgets 的降级方案
    from qfluentwidgets import (FluentWindow, NavigationItemPosition, Theme, setTheme,
                                CardWidget, BodyLabel, SubtitleLabel, TitleLabel, 
                                TableWidget, PrimaryPushButton, MessageBox,
                                LineEdit, ComboBox, DateEdit, TextEdit, Pivot, 
                                InfoBar, InfoBarPosition, FluentIcon as FIF, ScrollArea,
                                ToolButton, TransparentToolButton, ProgressBar, CalendarPicker)
    EditableComboBox = ComboBox # Fallback

# ==========================================
# 核心数据库初始化 (7 表 Schema)
# ==========================================
DB_NAME = "crm_enterprise.db"

def init_db():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # 自动补全缺失字段的逻辑 (热更新)
    def patch_db(cursor):
        patches = [
            ("projects", "stage", "TEXT"),
            ("projects", "loss_reason", "TEXT"),
            ("contracts", "paid_amount", "REAL DEFAULT 0.0")
        ]
        for table, column, col_type in patches:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"Patch error on {table}.{column}: {e}")
                pass 
    
    # 建表语句 (使用 TEXT 统一日期)
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
    
    patch_db(cursor)
    conn.commit()
    return conn

# ==========================================
# 辅助组件：KPI 卡片
# ==========================================
class KPICard(CardWidget):
    def __init__(self, title, value, color="#2980b9", parent=None):
        super().__init__(parent)
        self.setFixedSize(240, 120)
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)
        
        self.title_label = BodyLabel(title)
        self.value_label = TitleLabel(value)
        self.value_label.setTextColor(QColor(color), QColor(color))
        
        layout.addWidget(self.title_label, 0, Qt.AlignCenter)
        layout.addWidget(self.value_label, 0, Qt.AlignCenter)

# ==========================================
# 模块界面：经营看板 (Dashboard)
# ==========================================
class DashboardPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DashboardPage")
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        layout.setSpacing(25)
        
        # 顶部标题
        header = QHBoxLayout()
        title = TitleLabel("企业经营驾驶舱")
        header.addWidget(title)
        header.addStretch()
        layout.addLayout(header)
        
        # 1. 业绩墙 KPI
        kpi_layout = QHBoxLayout()
        self.kpi_total = KPICard("年度成交总额", "¥ 0.00", "#27ae60")
        self.kpi_unpaid = KPICard("待收尾款总计", "¥ 0.00", "#e67e22")
        self.kpi_active = KPICard("在跟进项目数", "0 个", "#2980b9")
        self.kpi_week = KPICard("本周拜访数量", "0 次", "#9b59b6")
        self.kpi_month = KPICard("本月拜访数量", "0 次", "#34495e")
        
        kpi_layout.addWidget(self.kpi_total)
        kpi_layout.addWidget(self.kpi_unpaid)
        kpi_layout.addWidget(self.kpi_active)
        kpi_layout.addWidget(self.kpi_week)
        kpi_layout.addWidget(self.kpi_month)
        kpi_layout.addStretch()
        layout.addLayout(kpi_layout)
        
        # 1.5 今日提醒区 (拜访与生日)
        self.remind_label = BodyLabel("今日无特别提醒")
        self.remind_label.setStyleSheet("color: #2980b9; font-weight: bold;")
        layout.addWidget(self.remind_label)

        # 2. 近期拜访预警 (30天内动态行程) - 置顶核心行程
        visit_title = SubtitleLabel("📅 近期拜访预警 (30天内行程)")
        layout.addWidget(visit_title)
        
        self.visit_list = TableWidget()
        self.visit_list.setColumnCount(4)
        self.visit_list.setHorizontalHeaderLabels(["客户名称", "项目名称", "计划拜访日期", "倒计时"])
        self.visit_list.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.visit_list.setFixedHeight(220)
        layout.addWidget(self.visit_list)

        # 3. 合同到期预警核心区
        alert_title = SubtitleLabel("合同到期红绿灯预警 (三级管控)")
        layout.addWidget(alert_title)
        
        alert_layout = QHBoxLayout()
        # 红色：30天内
        vbox_red = QVBoxLayout()
        lbl_red = BodyLabel("紧急: 30天内到期", self)
        lbl_red.setStyleSheet("color: #e74c3c; font-weight: bold;")
        vbox_red.addWidget(lbl_red)
        self.red_list = TableWidget()
        vbox_red.addWidget(self.red_list)
        
        # 橙色：60天内
        vbox_orange = QVBoxLayout()
        lbl_orange = BodyLabel("关注: 60天内到期", self)
        lbl_orange.setStyleSheet("color: #e67e22; font-weight: bold;")
        vbox_orange.addWidget(lbl_orange)
        self.orange_list = TableWidget()
        vbox_orange.addWidget(self.orange_list)

        # 蓝色：90天内
        vbox_blue = QVBoxLayout()
        lbl_blue = BodyLabel("预见: 90天内到期", self)
        lbl_blue.setStyleSheet("color: #2980b9; font-weight: bold;")
        vbox_blue.addWidget(lbl_blue)
        self.blue_list = TableWidget()
        vbox_blue.addWidget(self.blue_list)

        for t in [self.red_list, self.orange_list, self.blue_list]:
            t.setColumnCount(3)
            t.setHorizontalHeaderLabels(["客户", "项目", "天数"])
            t.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            t.setFixedHeight(180)
            t.itemDoubleClicked.connect(self.on_row_double_clicked)

        self.visit_list.itemDoubleClicked.connect(self.on_row_double_clicked)

        alert_layout.addLayout(vbox_red)
        alert_layout.addLayout(vbox_orange)
        alert_layout.addLayout(vbox_blue)
        layout.addLayout(alert_layout)
        
        layout.addStretch()
        self.load_data()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_data()

    def on_row_double_clicked(self, item):
        tw = item.tableWidget()
        first_item = tw.item(item.row(), 0)
        p_no = first_item.data(Qt.UserRole) if first_item else None
        if p_no:
            try:
                dlg = ProjectDetailDialog(p_no, self)
                dlg.exec()
                self.load_data()
            except Exception as e:
                InfoBar.error("加载失败", f"无法打开项目详情: {str(e)}", parent=self.window())

    def load_data(self):
        with sqlite3.connect(DB_NAME) as conn:
            # KPI 计算
            total_rev = conn.execute("SELECT SUM(total_amount) FROM contracts").fetchone()[0] or 0.0
            paid_rev = conn.execute("SELECT SUM(paid_amount) FROM contracts").fetchone()[0] or 0.0
            unpaid = total_rev - paid_rev
            active_p = conn.execute("SELECT COUNT(*) FROM projects WHERE loss_reason IS NULL OR loss_reason=''").fetchone()[0]
            
            self.kpi_total.value_label.setText(f"¥ {total_rev:,.2f}")
            self.kpi_unpaid.value_label.setText(f"¥ {unpaid:,.2f}")
            self.kpi_active.value_label.setText(f"{active_p} 个")
            
            # 拜访频率统计 (周/月)
            today = date.today()
            # 本周开始时间 (周一为准)
            week_start = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
            # 本月开始时间
            month_start = today.replace(day=1).strftime("%Y-%m-%d")
            
            week_v = conn.execute("SELECT COUNT(*) FROM follow_ups WHERE follow_date >= ?", (week_start,)).fetchone()[0]
            month_v = conn.execute("SELECT COUNT(*) FROM follow_ups WHERE follow_date >= ?", (month_start,)).fetchone()[0]
            
            self.kpi_week.value_label.setText(f"{week_v} 次")
            self.kpi_month.value_label.setText(f"{month_v} 次")
            
            # 今日提醒
            today_str = date.today().strftime("%Y-%m-%d")
            today_mmdd = date.today().strftime("-%m-%d")
            try:
                visits = conn.execute("SELECT project_name FROM projects WHERE next_visit_date=?", (today_str,)).fetchall()
                bdays = conn.execute("SELECT name FROM contacts WHERE birthday LIKE ?", (f"%{today_mmdd}",)).fetchall()
                
                reminds = []
                if visits: reminds.append(f"📅 今日拜访: {len(visits)}个项目")
                if bdays: reminds.append(f"🎂 客户生日: {', '.join([b[0] for b in bdays])}")
                if reminds: 
                    self.remind_label.setText(" | ".join(reminds))
                else:
                    self.remind_label.setText("今日无特别提醒")
            except: 
                self.remind_label.setText("提醒模块加载异常 (请检查数据库结构)")
            
            for t in [self.red_list, self.orange_list, self.blue_list, self.visit_list]:
                t.setRowCount(0)
            
            # --- 合同到期提醒 ---
            contracts = conn.execute("""
                SELECT c.name, p.project_name, ct.end_date, p.project_no
                FROM contracts ct 
                JOIN projects p ON ct.project_no = p.project_no
                JOIN customers c ON p.customer_id = c.id
            """).fetchall()
            
            for cust, proj, edate, p_no in contracts:
                try:
                    days = (datetime.strptime(edate, "%Y-%m-%d").date() - date.today()).days
                    target_table = None
                    if 0 <= days <= 30: target_table = self.red_list
                    elif 30 < days <= 60: target_table = self.orange_list
                    elif 60 < days <= 90: target_table = self.blue_list
                    
                    if target_table:
                        idx = target_table.rowCount()
                        target_table.insertRow(idx)
                        
                        item_cust = QTableWidgetItem(cust)
                        item_cust.setData(Qt.UserRole, p_no)
                        
                        target_table.setItem(idx, 0, item_cust)
                        target_table.setItem(idx, 1, QTableWidgetItem(proj))
                        target_table.setItem(idx, 2, QTableWidgetItem(f"{days}天"))
                except: pass

            # --- 近期拜访提醒 (30天内) ---
            visits_30 = conn.execute("""
                SELECT c.name, p.project_name, p.next_visit_date, p.project_no
                FROM projects p 
                JOIN customers c ON p.customer_id = c.id
                WHERE p.next_visit_date IS NOT NULL AND p.next_visit_date != ''
                AND (loss_reason IS NULL OR loss_reason='')
                ORDER BY p.next_visit_date ASC
            """).fetchall()
            
            for cust, proj, vdate, p_no in visits_30:
                try:
                    v_d = datetime.strptime(vdate, "%Y-%m-%d").date()
                    days = (v_d - date.today()).days
                    if 0 <= days <= 30:
                        idx = self.visit_list.rowCount()
                        self.visit_list.insertRow(idx)
                        
                        item_cust = QTableWidgetItem(cust)
                        item_cust.setData(Qt.UserRole, p_no)
                        self.visit_list.setItem(idx, 0, item_cust)
                        
                        self.visit_list.setItem(idx, 1, QTableWidgetItem(proj))
                        self.visit_list.setItem(idx, 2, QTableWidgetItem(vdate))
                        item_days = QTableWidgetItem(f"{days}天")
                        if days <= 7: item_days.setForeground(QColor("#e74c3c")) # 7天内紧急红色
                        self.visit_list.setItem(idx, 3, item_days)
                except: pass
            for t in [self.red_list, self.orange_list, self.blue_list]:
                t.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

# ==========================================
# 模块界面：档案库 (Master Data)
# ==========================================
class MasterDataPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("MasterDataPage")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        
        self.pivot = Pivot(self)
        self.stacked = QStackedWidget(self)
        
        # 子界面
        self.customer_view = QWidget()
        self.supplier_view = QWidget()
        
        self.init_customer_view()
        self.init_supplier_view()
        
        self.stacked.addWidget(self.customer_view)
        self.stacked.addWidget(self.supplier_view)
        
        self.pivot.addItem("customers", "客户档案库", lambda: self.stacked.setCurrentWidget(self.customer_view))
        self.pivot.addItem("suppliers", "供应商库", lambda: self.stacked.setCurrentWidget(self.supplier_view))
        
        layout.addWidget(TitleLabel("基础档案中心"))
        layout.addWidget(self.pivot)
        layout.addWidget(self.stacked)
        
        self.pivot.setCurrentItem("customers")
        
        # 绑定事件
        self.add_cust_btn.clicked.connect(self.show_add_customer)
        self.add_supp_btn.clicked.connect(self.show_add_supplier)
        self.cust_table.itemDoubleClicked.connect(self.on_customer_double_clicked)
        
        self.load_customers()
        self.load_suppliers()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_customers()
        self.load_suppliers()

    def init_customer_view(self):
        layout = QVBoxLayout(self.customer_view)
        btn_bar = QHBoxLayout()
        self.add_cust_btn = PrimaryPushButton(FIF.ADD, "新增客户")
        btn_bar.addWidget(self.add_cust_btn)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        
        self.cust_table = TableWidget()
        self.cust_table.setColumnCount(5)
        self.cust_table.setHorizontalHeaderLabels(["ID", "客户名称", "行业", "级别", "地址"])
        self.cust_table.horizontalHeader().setStretchLastSection(True)
        self.cust_table.setEditTriggers(TableWidget.NoEditTriggers)
        layout.addWidget(self.cust_table)

    def init_supplier_view(self):
        layout = QVBoxLayout(self.supplier_view)
        btn_bar = QHBoxLayout()
        self.add_supp_btn = PrimaryPushButton(FIF.ADD, "新增供应商")
        btn_bar.addWidget(self.add_supp_btn)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        
        self.supp_table = TableWidget()
        self.supp_table.setColumnCount(5)
        self.supp_table.setHorizontalHeaderLabels(["ID", "名称", "类别", "联系人", "电话"])
        self.supp_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.supp_table)

    def load_customers(self):
        self.cust_table.setRowCount(0)
        with sqlite3.connect(DB_NAME) as conn:
            cur = conn.execute("SELECT id, name, industry, level, address FROM customers")
            for r in cur:
                idx = self.cust_table.rowCount()
                self.cust_table.insertRow(idx)
                for i, v in enumerate(r):
                    self.cust_table.setItem(idx, i, QTableWidgetItem(str(v or "")))

    def load_suppliers(self):
        self.supp_table.setRowCount(0)
        with sqlite3.connect(DB_NAME) as conn:
            cur = conn.execute("SELECT id, name, category, contact_person, phone FROM suppliers")
            for r in cur:
                idx = self.supp_table.rowCount()
                self.supp_table.insertRow(idx)
                for i, v in enumerate(r):
                    self.supp_table.setItem(idx, i, QTableWidgetItem(str(v or "")))

    def show_add_customer(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("新增客户档案")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        name = LineEdit(); industry = EditableComboBox(); level = ComboBox(); addr = LineEdit()
        industry.addItems(["政府", "国企", "企业"])
        level.addItems(["A (重点)", "B (普通)", "C (初触)"])
        form.addRow("客户全称*:", name)
        form.addRow("所属行业:", industry)
        form.addRow("客户等级:", level)
        form.addRow("联系地址:", addr)
        layout.addLayout(form)
        btn = PrimaryPushButton("保存")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        
        if dlg.exec():
            if not name.text(): return
            with sqlite3.connect(DB_NAME) as conn:
                try:
                    conn.execute("INSERT INTO customers (name, industry, level, address) VALUES (?,?,?,?)",
                                (name.text(), industry.currentText(), level.currentText()[0], addr.text()))
                except: InfoBar.error("错误", "客户名已存在", parent=self.window())
            self.load_customers()

    def show_add_supplier(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("新增供应商")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        name = LineEdit(); cat = ComboBox(); cp = LineEdit(); ph = LineEdit()
        cat.addItems(["复印机", "耗材", "零件", "软件/服务", "其他"])
        form.addRow("供应商名称*:", name)
        form.addRow("产品类别:", cat)
        form.addRow("主联系人:", cp)
        form.addRow("联系电话:", ph)
        layout.addLayout(form)
        btn = PrimaryPushButton("保存")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        if dlg.exec():
            with sqlite3.connect(DB_NAME) as conn:
                conn.execute("INSERT INTO suppliers (name, category, contact_person, phone) VALUES (?,?,?,?)",
                            (name.text(), cat.currentText(), cp.text(), ph.text()))
            self.load_suppliers()

    def on_customer_double_clicked(self, item):
        cid = int(self.cust_table.item(item.row(), 0).text())
        dlg = CustomerDetailDialog(cid, self)
        dlg.exec()

class CustomerDetailDialog(QDialog):
    def __init__(self, customer_id, parent=None):
        super().__init__(parent)
        self.customer_id = customer_id
        self.setWindowTitle("客户深度档案详情")
        self.resize(800, 600)
        layout = QVBoxLayout(self)
        
        # 头部
        with sqlite3.connect(DB_NAME) as conn:
            res = conn.execute("SELECT name, level FROM customers WHERE id=?", (customer_id,)).fetchone()
            self.name = res[0]
            layout.addWidget(SubtitleLabel(f"档案项目: {res[0]} ({res[1]}级)"))
            
        self.pivot = Pivot(self)
        self.stacked = QStackedWidget(self)
        layout.addWidget(self.pivot)
        layout.addWidget(self.stacked)
        
        # 联系人子页
        self.contacts_page = QWidget()
        self.init_contacts_page()
        self.stacked.addWidget(self.contacts_page)
        self.pivot.addItem("contacts", "联系人矩阵 (1:N)", lambda: self.stacked.setCurrentWidget(self.contacts_page))
        self.pivot.setCurrentItem("contacts")

    def init_contacts_page(self):
        layout = QVBoxLayout(self.contacts_page)
        btn_bar = QHBoxLayout()
        add_btn = PrimaryPushButton(FIF.ADD, "新增联系人")
        add_btn.clicked.connect(self.add_contact)
        btn_bar.addWidget(add_btn)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        
        self.table = TableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["ID", "姓名", "职务", "电话", "决策人", "生日"])
        layout.addWidget(self.table)
        self.load_contacts()

    def load_contacts(self):
        self.table.setRowCount(0)
        # 响应式拉伸
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        with sqlite3.connect(DB_NAME) as conn:
            cur = conn.execute("SELECT id, name, post, phone, is_decision_maker, birthday FROM contacts WHERE customer_id=?", (self.customer_id,))
            for r in cur:
                idx = self.table.rowCount()
                self.table.insertRow(idx)
                for i, v in enumerate(r):
                    val = "是" if i == 4 and v == 1 else ("否" if i == 4 else str(v or ""))
                    self.table.setItem(idx, i, QTableWidgetItem(val))

    def add_contact(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("新增联系人")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        name = LineEdit(); post = LineEdit(); phone = LineEdit(); decision = ComboBox(); bday = DateEdit()
        decision.addItems(["否", "是"])
        form.addRow("姓名*:", name)
        form.addRow("职务/部门:", post)
        form.addRow("手机/电话:", phone)
        form.addRow("决策人?:", decision)
        form.addRow("生日:", bday)
        layout.addLayout(form)
        btn = PrimaryPushButton("确认")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        if dlg.exec():
            with sqlite3.connect(DB_NAME) as conn:
                conn.execute("INSERT INTO contacts (customer_id, name, post, phone, is_decision_maker, birthday) VALUES (?,?,?,?,?,?)",
                            (self.customer_id, name.text(), post.text(), phone.text(), decision.currentIndex(), bday.date().toString("yyyy-MM-dd")))
            self.load_contacts()

# ==========================================
# 占位页面：项目、报价、合同
# ==========================================
def get_next_project_no():
    prefix = datetime.now().strftime("%y%m%d")
    with sqlite3.connect(DB_NAME) as conn:
        res = conn.execute("SELECT project_no FROM projects WHERE project_no LIKE ? ORDER BY project_no DESC LIMIT 1", (f"{prefix}%",)).fetchone()
        if res:
            seq = int(res[0][-2:]) + 1
            return f"{prefix}{seq:02d}"
        else:
            return f"{prefix}01"

class ProjectPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("ProjectPage")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        
        # 顶部工具栏 (优化检索与刷新)
        btn_bar = QHBoxLayout()
        title = TitleLabel("销售项目与漏斗管理")
        
        self.search_box = SearchLineEdit()
        self.search_box.setPlaceholderText("搜客户名称 / 项目名称...")
        self.search_box.setFixedWidth(300)
        self.search_box.textChanged.connect(self.load_projects)
        
        self.refresh_btn = TransparentToolButton(FIF.SYNC, self)
        self.refresh_btn.setToolTip("刷新列表")
        self.refresh_btn.clicked.connect(self.load_projects)
        
        self.add_btn = PrimaryPushButton(FIF.ADD, "发起新项目/立项")
        
        btn_bar.addWidget(title)
        btn_bar.addStretch()
        btn_bar.addWidget(self.search_box)
        btn_bar.addWidget(self.refresh_btn)
        btn_bar.addWidget(self.add_btn)
        layout.addLayout(btn_bar)
        
        # 项目列表
        self.table = TableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(["项目编号", "客户", "项目名称", "当前阶段", "下次拜访", "状态", "操作"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setEditTriggers(TableWidget.NoEditTriggers)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu) # 右键支持
        layout.addWidget(self.table)
        
        self.add_btn.clicked.connect(self.add_project)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        
        # 多重事件监听，解决部分平台信号丢失问题
        self.table.cellDoubleClicked.connect(self.on_cell_double_click)
        self.table.cellClicked.connect(self.on_cell_double_click)
        
        self.load_projects()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_projects()

    def load_projects(self):
        st = self.search_box.text().strip()
        try:
            self.table.setRowCount(0)
            with sqlite3.connect(DB_NAME, timeout=10) as conn:
                query = """
                    SELECT p.project_no, c.name, p.project_name, 
                           COALESCE(p.stage, '初期线索'), 
                           COALESCE(p.next_visit_date, ''), 
                           COALESCE(p.loss_reason, '')
                    FROM projects p JOIN customers c ON p.customer_id = c.id
                    WHERE (c.name LIKE ? OR p.project_name LIKE ?)
                    ORDER BY p.next_visit_date DESC
                """
                cur = conn.execute(query, (f"%{st}%", f"%{st}%"))
                for r in cur:
                    idx = self.table.rowCount()
                    self.table.insertRow(idx)
                    
                    # 状态判定逻辑 (默认跟进中)
                    p_no, c_name, p_name, stage, next_v, loss = r
                    status_text = "跟进中"
                    status_color = "#2980b9"
                    
                    if stage == "已成交":
                        status_text = "已成交"
                        status_color = "#27ae60"
                    elif stage == "已流失" or (loss and str(loss).strip()):
                        status_text = "已流失"
                        status_color = "#e74c3c"
                    
                    # 组装行数据 (确保非空)
                    row_data = [
                        str(p_no or ""), 
                        str(c_name or ""), 
                        str(p_name or ""), 
                        str(stage or "未开始"), 
                        str(next_v or ""), 
                        str(status_text)
                    ]
                    
                    for i, v in enumerate(row_data):
                        item = QTableWidgetItem(v)
                        if i == 5: # 状态列着色
                            item.setForeground(QColor(status_color))
                        self.table.setItem(idx, i, item)
                    
                    # 注入操作按钮
                    view_btn = TransparentToolButton(FIF.SEARCH, self)
                    view_btn.setToolTip("查看详情")
                    view_btn.setProperty("p_no", str(p_no))
                    view_btn.clicked.connect(self.on_view_btn_clicked)
                    self.table.setCellWidget(idx, 6, view_btn)
            self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        except Exception as e:
            traceback.print_exc()
            InfoBar.error("加载失败", f"项目加载出错: {str(e)}", duration=0, parent=self.window())

    def add_project(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("项目立项")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        # 客户选择器
        cust_cb = ComboBox()
        cust_ids = []
        with sqlite3.connect(DB_NAME) as conn:
            for r in conn.execute("SELECT id, name FROM customers"):
                cust_ids.append(r[0]); cust_cb.addItem(r[1])
        
        if not cust_ids:
            InfoBar.warning("提示", "请先在【基础档案】中建立客户库", parent=self.window())
            return

        name = LineEdit(); stage = ComboBox()
        stage.addItems(["初期线索", "有预算", "商机接洽", "报价阶段", "合同阶段"])
        
        form.addRow("关联客户*:", cust_cb)
        form.addRow("项目名称*:", name)
        form.addRow("初始阶段:", stage)
        layout.addLayout(form)
        
        btn = PrimaryPushButton("确认立项")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        
        if dlg.exec():
            if not name.text(): return
            p_no = get_next_project_no()
            cid = cust_ids[cust_cb.currentIndex()]
            with sqlite3.connect(DB_NAME) as conn:
                try:
                    conn.execute("INSERT INTO projects (project_no, customer_id, project_name, stage) VALUES (?,?,?,?)",
                                (p_no, cid, name.text(), stage.currentText()))
                except Exception as e:
                    InfoBar.error("立项失败", f"数据库写入错误: {str(e)}", duration=3000, parent=self.window())
                    return
            self.load_projects()
            InfoBar.success("立项成功", f"项目编号 {p_no} 已创建", duration=2000, parent=self.window())

    def show_context_menu(self, pos):
        from PyQt5.QtWidgets import QMenu
        menu = QMenu(self)
        view_action = menu.addAction("查看项目详情")
        row = self.table.currentRow()
        if row >= 0:
            view_action.triggered.connect(self.view_selected_detail)
            menu.exec_(self.table.viewport().mapToGlobal(pos))
    def on_view_btn_clicked(self):
        btn = self.sender()
        p_no = btn.property("p_no")
        if p_no:
            self.show_detail(p_no)

    def view_selected_detail(self):
        print("DEBUG: 点击工具栏查看详情按钮", flush=True)
        row = self.table.currentRow()
        if row < 0:
            InfoBar.warning("请选择项目", "请先在列表中选中一个项目进行查看", duration=2000, parent=self)
            return
        it = self.table.item(row, 0)
        p_no = it.text() if it else ""
        if p_no: self.show_detail(p_no)

    def on_cell_double_click(self, row, col):
        print(f"DEBUG: 监测到点击事件 [Row: {row}, Col: {col}]", flush=True)
        it = self.table.item(row, 0)
        p_no = it.text() if it else ""
        if p_no:
            self.show_detail(p_no)

    def show_detail(self, p_no):
        print(f"DEBUG: 命中详情加载逻辑: {p_no}", flush=True)
        # 立即给予视觉反馈
        InfoBar.info("正在加载", f"正在打开项目 {p_no} 的全量看板...", duration=1000, parent=self.window())
        
        try:
            dlg = ProjectDetailDialog(p_no, self)
            dlg.exec()
        except Exception as e:
            print(f"DEBUG: 对话框启动失败: {e}", flush=True)
            traceback.print_exc()
            InfoBar.error("加载失败", f"无法初始化详情看板: {str(e)}", duration=3000, parent=self.window())

class ProjectDetailDialog(QDialog):
    def __init__(self, project_no, parent=None):
        super().__init__(parent)
        print(f"DEBUG: 初始化项目详情 [{project_no}]")
        self.project_no = project_no
        self.customer_id = None
        self.current_stage = ""
        self.customer_name = ""
        self.p_name = "" 
        
        try:
            with sqlite3.connect(DB_NAME) as conn:
                row = conn.execute("""
                    SELECT p.customer_id, p.stage, c.name, p.project_name FROM projects p 
                    JOIN customers c ON p.customer_id = c.id 
                    WHERE p.project_no=?""", (project_no,)).fetchone()
                if row:
                    self.customer_id, self.current_stage, self.customer_name, self.p_name = row
            print(f"DEBUG: 数据库读取完成 [{self.customer_name}]")
        except Exception as e:
            print(f"DEBUG: 数据库读取异常: {e}")
            traceback.print_exc()

        self.setWindowTitle(f"项目深度跟进: {project_no}")
        self.resize(1000, 700)
        try:
            self.init_ui()
        except Exception as e:
            QMessageBox.critical(self, "UI 加载失败", f"初次渲染详情页时崩溃: {e}")
            traceback.print_exc()
        print(f"DEBUG: 详情页初始化成功")

    def init_ui(self):
        print("DEBUG: 开始加载 UI 组件")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        
        title = TitleLabel(f"项目名称: {self.p_name} | 客户: {self.customer_name}")
        layout.addWidget(title)
        
        self.pivot = Pivot(self)
        self.stacked = QStackedWidget(self)
        layout.addWidget(self.pivot)
        layout.addWidget(self.stacked)
        
        # 跟进记录页
        self.follow_page = QWidget()
        self.init_follow_page()
        self.stacked.addWidget(self.follow_page)
        self.pivot.addItem("follows", "过程跟进记录", lambda: self.stacked.setCurrentWidget(self.follow_page))
        self.pivot.setCurrentItem("follows")
        print("DEBUG: UI 组件加载完成")

    def init_follow_page(self):
        layout = QVBoxLayout(self.follow_page)
        btn_bar = QHBoxLayout()
        add_btn = PrimaryPushButton(FIF.ADD, "新增跟进日志")
        add_btn.clicked.connect(self.add_follow)
        btn_bar.addWidget(add_btn)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        
        self.table = TableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["日期", "联系人", "跟进阶段", "跟进详情", "下一步"])
        # 核心：设置自适应拉伸与自动换行
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setWordWrap(True) # 开启自动换行
        self.table.verticalHeader().hide()
        layout.addWidget(self.table, 1)
        self.load_follows()

    def load_follows(self):
        self.table.setRowCount(0)
        with sqlite3.connect(DB_NAME) as conn:
            cur = conn.execute("SELECT follow_date, contact_name, stage, detail, next_plan FROM follow_ups WHERE project_no=? ORDER BY follow_date DESC", (self.project_no,))
            for r in cur:
                idx = self.table.rowCount()
                self.table.insertRow(idx)
                row_data = [str(r[0] or ""), str(r[1] or ""), str(r[2] or ""), str(r[3] or ""), str(r[4] or "")]
                for i, v in enumerate(row_data):
                    if i >= 3: # 跟进详情、下一步
                        lbl = QLabel(str(v or ""))
                        lbl.setWordWrap(True)
                        lbl.setContentsMargins(10, 8, 10, 8)
                        lbl.setAlignment(Qt.AlignVCenter | Qt.AlignLeft) # 垂直居中
                        lbl.setStyleSheet("font-size: 13px; color: #333; height: auto;")
                        self.table.setCellWidget(idx, i, lbl)
                    else:
                        item = QTableWidgetItem(str(v or ""))
                        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft) # 垂直居中
                        self.table.setItem(idx, i, item)
            
            # 核心：延迟调用高度修正函数
            QTimer.singleShot(300, self._fix_row_heights)

    def _fix_row_heights(self):
        """对所有行进行物理高度重排"""
        self.table.blockSignals(True)
        for row in range(self.table.rowCount()):
            max_h = 40 # 基础最小高度
            for col in [3, 4]: # 检查有文本的列
                w = self.table.cellWidget(row, col)
                if isinstance(w, QLabel):
                    # 强制标签在当前列宽下换行并计算高度
                    w.setFixedWidth(self.table.columnWidth(col))
                    h = w.sizeHint().height() + 30 
                    max_h = max(max_h, h)
            self.table.setRowHeight(row, max_h)
        self.table.blockSignals(False)

    def add_follow(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("记录商谈跟进")
        dlg.setMinimumWidth(450)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        date_e = CalendarPicker(); date_e.setDate(QDate.currentDate())
        
        # 阶段选择
        stage_cb = ComboBox()
        stages = ["初期线索", "有预算", "商机接洽", "报价阶段", "合同阶段", "已成交", "已流失"]
        stage_cb.addItems(stages)
        if self.current_stage in stages: stage_cb.setCurrentText(self.current_stage)
        
        # 面会人 (下拉选择 + 电话) - 使用 EditableComboBox 以支持编辑
        contact_cb = EditableComboBox()
        with sqlite3.connect(DB_NAME) as conn:
            contacts = conn.execute("SELECT name, phone FROM contacts WHERE customer_id=?", (self.customer_id,)).fetchall()
            for name, phone in contacts:
                contact_cb.addItem(f"{name} ({phone})")
        
        detail = TextEdit()
        plan = LineEdit()
        
        # 下次拜访 (提前定义以防 NameError)
        visit_date = CalendarPicker()
        visit_date.setDate(QDate.currentDate().addDays(7))
        
        loss_reason_input = LineEdit(); loss_reason_input.setPlaceholderText("如果是流失/丢单，请在此输入原因...")
        loss_reason_input.hide()
        
        # 联动显示
        def update_visibility(t):
            is_closed = t in ["已成交", "已流失"]
            loss_reason_input.setVisible(t == "已流失")
            visit_date.setDisabled(is_closed) # 如果成交或流失，禁用下次拜访
            if is_closed:
                visit_date.setDate(QDate.currentDate()) # 重置为今日
        
        stage_cb.currentTextChanged.connect(update_visibility)
        update_visibility(stage_cb.currentText())
        
        form.addRow("跟进日期*:", date_e)
        form.addRow("跟进阶段*:", stage_cb)
        form.addRow("流失原因:", loss_reason_input) # 仅在已流失时可见
        form.addRow("面会人*:", contact_cb)
        form.addRow("内容详情*:", detail)
        form.addRow("下一步计划:", plan)
        form.addRow("下次拜访日提醒*:", visit_date)
        layout.addLayout(form)
        
        btn = PrimaryPushButton("保存跟进记录")
        layout.addWidget(btn)
        
        def on_save():
            current_s = stage_cb.currentText()
            is_closed = current_s in ["已成交", "已流失"]
            
            # 基础校验：面会人始终必填
            if not contact_cb.currentText():
                InfoBar.warning("必填项缺失", "请选择或输入面会人", duration=3000, parent=dlg)
                return
            
            # 核心：根据阶段调整校验逻辑
            if not is_closed:
                v_date = visit_date.date
                d_date = date_e.date
                if v_date <= d_date:
                    InfoBar.warning("日期校验失败", "跟进中项目需指定有效的下次拜访日期", duration=3000, parent=dlg)
                    return
                if not detail.toPlainText().strip():
                    InfoBar.warning("必填项缺失", "请填写详细跟进内容", duration=3000, parent=dlg)
                    return
            
            dlg.accept()

        btn.clicked.connect(on_save)
        
        if dlg.exec():
            new_stage = stage_cb.currentText()
            d_str = date_e.date.toString("yyyy-MM-dd")
            v_str = visit_date.date.toString("yyyy-MM-dd")
            
            with sqlite3.connect(DB_NAME, timeout=10) as conn:
                # 插入跟进
                conn.execute("INSERT INTO follow_ups (project_no, follow_date, contact_name, stage, detail, next_plan) VALUES (?,?,?,?,?,?)",
                            (self.project_no, d_str, contact_cb.currentText(), new_stage, detail.toPlainText(), plan.text()))
                
                # 更新项目状态
                if new_stage in ["已成交", "已流失"]:
                    # 归档项目：清除下次拜访日期
                    conn.execute("UPDATE projects SET stage=?, next_visit_date=NULL, loss_reason=? WHERE project_no=?",
                                (new_stage, loss_reason_input.text() if new_stage=="已流失" else None, self.project_no))
                else:
                    conn.execute("UPDATE projects SET stage=?, next_visit_date=?, loss_reason=NULL WHERE project_no=?",
                                (new_stage, v_str, self.project_no))
                
                self.current_stage = new_stage
                conn.commit()


            self.load_follows()
            InfoBar.success("已更新", f"跟进记录已保存，项目现处于【{new_stage}】阶段", duration=3000)

class QuotationPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("QuotationPage")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        
        btn_bar = QHBoxLayout()
        btn_bar.addWidget(TitleLabel("项目报价与方案库"))
        btn_bar.addStretch()
        
        self.search_box = SearchLineEdit()
        self.search_box.setPlaceholderText("搜客户名称 / 项目名称...")
        self.search_box.setFixedWidth(300)
        self.search_box.textChanged.connect(self.load_quotes)
        
        self.refresh_btn = TransparentToolButton(FIF.SYNC, self)
        self.refresh_btn.setToolTip("刷新列表")
        self.refresh_btn.clicked.connect(self.load_quotes)
        
        self.add_btn = PrimaryPushButton(FIF.ADD, "提交新报价")
        
        btn_bar.addWidget(self.search_box)
        btn_bar.addWidget(self.refresh_btn)
        btn_bar.addWidget(self.add_btn)
        layout.addLayout(btn_bar)
        
        self.table = TableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels(["客户名称", "项目名称", "关联单号", "报价日期", "版本", "金额 (¥)", "附件状态", "操作"])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)
        
        self.add_btn.clicked.connect(self.add_quote)
        self.load_quotes()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_quotes()

    def load_quotes(self):
        st = self.search_box.text().strip()
        self.table.setRowCount(0)
        with sqlite3.connect(DB_NAME, timeout=10) as conn:
            query = """
                SELECT c.name, p.project_name, q.project_no, q.quote_date, q.version, q.amount, q.file_path 
                FROM quotations q 
                JOIN projects p ON q.project_no = p.project_no 
                JOIN customers c ON p.customer_id = c.id 
                WHERE (c.name LIKE ? OR p.project_name LIKE ?)
                ORDER BY q.quote_date DESC
            """
            cur = conn.execute(query, (f"%{st}%", f"%{st}%"))
            for r in cur:
                idx = self.table.rowCount()
                self.table.insertRow(idx)
                # 修改 row_data 映射：客户, 项目, 单号, 日期, 版本, 金额, 状态
                row_data = [r[0], r[1], r[2], r[3], r[4], f"{r[5]:,.2f}", "已关联" if r[6] else "未上传"]
                for i, v in enumerate(row_data):
                    self.table.setItem(idx, i, QTableWidgetItem(v))
                
                # 8. 操作区 (容器承载)
                actions = QWidget()
                h_layout = QHBoxLayout(actions)
                h_layout.setContentsMargins(5, 0, 5, 0); h_layout.setSpacing(10)
                
                # 查看附件 (如有)
                if r[6]:
                    btn_view = TransparentToolButton(FIF.FOLDER, actions)
                    btn_view.setToolTip("查看方案/附件")
                    def open_file(p=r[6]):
                        if os.path.exists(p):
                            QDesktopServices.openUrl(QUrl.fromLocalFile(os.path.abspath(p)))
                        else:
                            InfoBar.error("文件缺失", f"未找到文件: {os.path.basename(p)}", duration=3000, parent=self.window())
                    btn_view.clicked.connect(open_file)
                    h_layout.addWidget(btn_view)
                
                # 删除报价 (管理反馈：增加修改能力)
                btn_del = TransparentToolButton(FIF.DELETE, actions)
                btn_del.setToolTip("移除此条报价记录")
                btn_del.clicked.connect(lambda _, p_no=r[2], v=r[4]: self.delete_quote(p_no, v))
                h_layout.addWidget(btn_del)
                
                h_layout.addStretch()
                self.table.setCellWidget(idx, 7, actions)
        # 响应式拉伸
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

    def delete_quote(self, p_no, ver):
        """物理移除报价记录"""
        res = MessageBox("确定删除?", f"即将永久移除项目 [{p_no}] 的版本 [{ver}] 的报价记录。确认操作?", self.window())
        if res.exec():
            with sqlite3.connect(DB_NAME, timeout=10) as conn:
                conn.execute("DELETE FROM quotations WHERE project_no=? AND version=?", (p_no, ver))
                conn.commit()
            self.load_quotes()
            InfoBar.success("删除成功", "报价记录已移除", duration=2000)

    def add_quote(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("新建商务报价")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        proj_cb = ComboBox(); proj_nos = []
        with sqlite3.connect(DB_NAME) as conn:
            for r in conn.execute("SELECT project_no, project_name FROM projects"):
                proj_nos.append(r[0]); proj_cb.addItem(f"[{r[0]}] {r[1]}")
        
        if not proj_nos:
            InfoBar.warning("提示", "请先在【项目跟进】中立项", parent=self.window())
            return

        date_e = CalendarPicker(); date_e.setDate(QDate.currentDate())
        ver = LineEdit(); ver.setPlaceholderText("V1 / 2026-A 等")
        amt = LineEdit(); amt.setPlaceholderText("0.00")
        
        # 自动生成版本的辅助函数
        def auto_set_version():
            p_no = proj_nos[proj_cb.currentIndex()]
            with sqlite3.connect(DB_NAME, timeout=10) as conn:
                count = conn.execute("SELECT COUNT(*) FROM quotations WHERE project_no=?", (p_no,)).fetchone()[0]
                ver.setText(f"V{count + 1}")
        
        proj_cb.currentIndexChanged.connect(auto_set_version)
        auto_set_version() # 初始化运行一次
        
        self.file_path = ""
        file_btn = PrimaryPushButton("选取报价单文件 (PDF/Excel)")
        file_btn.clicked.connect(lambda: self.select_file())
        
        form.addRow("关联项目*:", proj_cb)
        form.addRow("报价日期:", date_e)
        form.addRow("建议版本:", ver)
        form.addRow("报价金额 (¥)*:", amt)
        form.addRow("附件管理:", file_btn)
        layout.addLayout(form)
        
        save_btn = PrimaryPushButton("确认报价")
        save_btn.clicked.connect(dlg.accept)
        layout.addWidget(save_btn)
        
        if dlg.exec():
            p_no = proj_nos[proj_cb.currentIndex()]
            try: val_amt = float(amt.text())
            except: val_amt = 0.0
            with sqlite3.connect(DB_NAME) as conn:
                conn.execute("INSERT INTO quotations (project_no, quote_date, amount, version, file_path) VALUES (?,?,?,?,?)",
                            (p_no, date_e.date.toString("yyyy-MM-dd"), val_amt, ver.text(), self.file_path))
            self.load_quotes()

    def select_file(self):
        try:
            path, _ = QFileDialog.getOpenFileName(self, "选择报价单附件", os.path.expanduser("~"), "Documents (*.pdf *.xlsx *.xls *.doc *.docx);;All Files (*)")
            if path: 
                self.file_path = path
                InfoBar.success("文件已关联", f"附件: {os.path.basename(path)}", duration=2000, parent=self)
        except Exception as e:
            InfoBar.error("附件失败", str(e), parent=self)

class ContractPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("ContractPage")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        
        btn_bar = QHBoxLayout()
        btn_bar.addWidget(TitleLabel("合同签署与回款监控"))
        btn_bar.addStretch()
        self.add_btn = PrimaryPushButton(FIF.ADD, "录入成交合同")
        btn_bar.addWidget(self.add_btn)
        layout.addLayout(btn_bar)
        
        self.table = TableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels(["项目编号", "合同周期", "总金额", "已收", "待收", "状态", "附件", "操作"])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)
        
        self.add_btn.clicked.connect(self.add_contract)
        self.load_contracts()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_contracts()

    def load_contracts(self):
        try:
            self.table.setRowCount(0)
            with sqlite3.connect(DB_NAME, timeout=10) as conn:
                query = "SELECT project_no, start_date, end_date, total_amount, COALESCE(paid_amount, 0.0), file_path FROM contracts"
                cur = conn.execute(query)
                for r in cur:
                    idx = self.table.rowCount()
                    self.table.insertRow(idx)
                    total, paid = (r[3] or 0.0), (r[4] or 0.0)
                    unpaid = total - paid
                    cycle = f"{r[1]} 至 {r[2]}"
                    status = "已结单" if unpaid <= 0 else "履行中"
                    
                    row_data = [r[0], cycle, f"¥{total:,.2f}", f"¥{paid:,.2f}", f"¥{unpaid:,.2f}", status, "有" if r[5] else "无"]
                    for i, v in enumerate(row_data):
                        item = QTableWidgetItem(v)
                        if i == 4 and unpaid > 0: item.setTextColor(QColor("#e67e22"))
                        self.table.setItem(idx, i, item)

                    if r[5]:
                        btn = TransparentToolButton(FIF.FOLDER, self)
                        def open_c_file(p=r[5]):
                            if os.path.exists(p):
                                QDesktopServices.openUrl(QUrl.fromLocalFile(os.path.abspath(p)))
                            else:
                                InfoBar.error("文件缺失", f"路径无效或文件已移动: {os.path.basename(p)}", duration=3000, parent=self.window())
                        btn.clicked.connect(open_c_file)
                        self.table.setCellWidget(idx, 7, btn)
            # 响应式拉伸
            self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        except Exception as e:
            traceback.print_exc()
            InfoBar.error("加载失败", f"合同数据载入异常: {str(e)}", duration=0, parent=self.window())

    def add_contract(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("合同签约登记")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        proj_cb = ComboBox(); proj_nos = []
        with sqlite3.connect(DB_NAME) as conn:
            for r in conn.execute("SELECT project_no, project_name FROM projects"):
                proj_nos.append(r[0]); proj_cb.addItem(f"[{r[0]}] {r[1]}")
        
        if not proj_nos: return

        start = CalendarPicker(); start.setDate(QDate.currentDate())
        end = CalendarPicker(); end.setDate(QDate.currentDate().addDays(365))
        total = LineEdit(); paid = LineEdit()
        self.c_file = ""
        f_btn = PrimaryPushButton("上传扫描件")
        f_btn.clicked.connect(lambda: self.select_c_file())
        
        form.addRow("关联项目*:", proj_cb)
        form.addRow("生效日期:", start); form.addRow("截止日期:", end)
        form.addRow("合同总额*:", total); form.addRow("首付/已收*:", paid)
        form.addRow("附件:", f_btn)
        layout.addLayout(form)
        
        btn = PrimaryPushButton("归档合同")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        
        if dlg.exec():
            p_no = proj_nos[proj_cb.currentIndex()]
            with sqlite3.connect(DB_NAME) as conn:
                try:
                    conn.execute("INSERT INTO contracts (project_no, start_date, end_date, total_amount, paid_amount, file_path) VALUES (?,?,?,?,?,?)",
                                (p_no, start.date.toString("yyyy-MM-dd"), end.date.toString("yyyy-MM-dd"), 
                                 float(total.text() or 0), float(paid.text() or 0), self.c_file))
                except Exception as e:
                    InfoBar.error("保存失败", f"数据库写入错误: {str(e)}", duration=3000, parent=self.window())
                    return
            self.load_contracts()
            InfoBar.success("合同已归档", f"项目 {p_no} 的合同已成功录入", duration=2000, parent=self.window())

    def select_c_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "选择文件", "", "All Files (*)")
        if path: self.c_file = path

# ==========================================
# 自动化热更新补丁：确保字段补全
# ==========================================
def ensure_columns():
    with sqlite3.connect(DB_NAME, timeout=10) as conn:
        cursor = conn.cursor()
        # 补全合同表关键字段
        try:
            cursor.execute("ALTER TABLE contracts ADD COLUMN paid_amount REAL DEFAULT 0.0")
        except sqlite3.OperationalError as e:
            if "duplicate" not in str(e).lower(): print(f"Ensure error (contracts): {e}")
        # 补全项目表关键字段
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN loss_reason TEXT")
            cursor.execute("ALTER TABLE projects ADD COLUMN stage TEXT")
        except sqlite3.OperationalError as e:
            if "duplicate" not in str(e).lower(): print(f"Ensure error (projects): {e}")
        conn.commit()

class MainWindow(FluentWindow):
    def __init__(self):
        super().__init__()
        init_db()
        ensure_columns() # 启动时强制补全字段
        
        self.setWindowTitle("CRM Enterprise - 单兵高管企业版")
        self.showMaximized() # 自动最大化适配屏幕
        setTheme(Theme.LIGHT)
        
        # 初始化页面
        self.dashboard_page = DashboardPage()
        self.master_page = MasterDataPage()
        self.project_page = ProjectPage()
        self.quotation_page = QuotationPage()
        self.contract_page = ContractPage()
        
        self.init_navigation()

    def init_navigation(self):
        self.addSubInterface(self.dashboard_page, FIF.HOME, "经营看板")
        self.addSubInterface(self.master_page, FIF.PEOPLE, "基础档案")
        self.addSubInterface(self.project_page, FIF.EDIT, "项目跟进")
        self.addSubInterface(self.quotation_page, FIF.DOCUMENT, "报价管理")
        self.addSubInterface(self.contract_page, FIF.PASTE, "合同财务")
        
        self.navigationInterface.setExpandWidth(200)

if __name__ == '__main__':
    # 多开检测逻辑 (单实例运行)
    server_name = "CRM_SINGLE_INSTANCE_LOCK"
    socket = QLocalSocket()
    socket.connectToServer(server_name)
    if socket.waitForConnected(500):
        print("Another instance is already running.")
        sys.exit(0)
    
    # 启动本地服务器用于锁定
    local_server = QLocalServer()
    local_server.listen(server_name)
    
    w = MainWindow()
    w.show()
    sys.exit(app.exec())

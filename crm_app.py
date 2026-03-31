import os
os.environ["QT_API"] = "PyQt5"
import sys
import sqlite3
import traceback
import shutil
from datetime import datetime, date, timedelta

def get_app_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def get_attachment_dir(sub_dir):
    d = os.path.join(get_app_dir(), "attachments", sub_dir)
    os.makedirs(d, exist_ok=True)
    return d

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
from PyQt5.QtGui import QIcon, QDesktopServices, QColor, QPainter, QFont
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QTableWidgetItem, QHeaderView, QFileDialog,
                             QScrollArea, QFrame, QDialog, QFormLayout, QTableWidget,
                             QMenu, QAction, QComboBox, QCompleter, QStackedWidget, QMessageBox,
                             QAbstractItemView, QInputDialog)
from PyQt5.QtNetwork import QLocalServer, QLocalSocket

# ==========================================
# 全局信号总线 (SignalBus) 用于组件解耦同步
# ==========================================
from PyQt5.QtCore import QObject, pyqtSignal
class SignalBus(QObject):
    projectChanged = pyqtSignal() # 项目数据变更信号 (通知各 Page 刷新)

SIGNAL_BUS = SignalBus()

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
                                TableWidget, PushButton, PrimaryPushButton, MessageBox,
                                LineEdit, SearchLineEdit, ComboBox, EditableComboBox, DateEdit, TextEdit, Pivot, 
                                InfoBar, InfoBarPosition, FluentIcon as FIF, ScrollArea,
                                ToolButton, TransparentToolButton, ProgressBar, CalendarPicker)
except ImportError:
    # 针对旧版本 qfluentwidgets 的降级方案
    from qfluentwidgets import (FluentWindow, NavigationItemPosition, Theme, setTheme,
                                CardWidget, BodyLabel, SubtitleLabel, TitleLabel, 
                                TableWidget, PushButton, PrimaryPushButton, MessageBox,
                                LineEdit, ComboBox, DateEdit, TextEdit, Pivot, 
                                InfoBar, InfoBarPosition, FluentIcon as FIF, ScrollArea,
                                ToolButton, TransparentToolButton, ProgressBar, CalendarPicker)
    EditableComboBox = ComboBox # Fallback

# ==========================================
# 崩溃修复补丁：针对 qfluentwidgets 与 PyQt5 的 QWheelEvent 兼容性架构优化
# ==========================================
from PyQt5.QtCore import QObject, QEvent
def safe_event_filter(self, obj, event):
    # 拦截可能导致崩溃的滚轮事件属性读取
    if event.type() == QEvent.Wheel:
        return False # 允许正常滚动，但不触发库内部的样式属性检索
    return False

# 暂时屏蔽库自带的、在某些版本中不稳定的事件过滤器
TableWidget.eventFilter = safe_event_filter
ScrollArea.eventFilter = safe_event_filter

# ==========================================
# 核心数据库初始化 (7 表 Schema)
# ==========================================
DB_NAME = "crm_enterprise.db"

# ==========================================
# 核心数据库连接工厂 (性能调优关键)
# ==========================================
def get_db_conn(timeout=10):
    """
    获取统一配置的数据库连接，强制注入性能 PRAGMA
    """
    conn = sqlite3.connect(DB_NAME, timeout=timeout)
    # 强制开启高性能配置
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    """统一数据库初始化与结构查体 (CRM 核心)"""
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("PRAGMA busy_timeout = 5000")
        
        # 1. 基础建表 (核心结构)
        tables = {
            "customers": "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, industry TEXT, level TEXT, address TEXT",
            "contacts": "id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id INTEGER, name TEXT, post TEXT, dept TEXT, phone TEXT, email TEXT, birthday TEXT, is_decision_maker INTEGER, role_type TEXT DEFAULT '经办人', FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE",
            "suppliers": "id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, category TEXT, contact_person TEXT, phone TEXT, note TEXT",
            "projects": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT UNIQUE, customer_id INTEGER, project_name TEXT, stage TEXT DEFAULT '初期线索', loss_reason TEXT, next_visit_date TEXT, next_plan TEXT, FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE",
            "follow_ups": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, follow_date TEXT, contact_name TEXT, contact_method TEXT DEFAULT '电话', follow_duration INTEGER DEFAULT 0, stage TEXT, detail TEXT, next_plan TEXT, FOREIGN KEY(project_no) REFERENCES projects(project_no) ON DELETE CASCADE",
            "quotations": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, quote_date TEXT, amount REAL, file_path TEXT, version TEXT, remark TEXT, FOREIGN KEY (project_no) REFERENCES projects(project_no) ON DELETE CASCADE",
            "contracts": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, start_date TEXT, end_date TEXT, total_amount REAL, paid_amount REAL DEFAULT 0.0, file_path TEXT, contract_memo TEXT, FOREIGN KEY (project_no) REFERENCES projects(project_no) ON DELETE CASCADE",
            "payment_plans": "id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, plan_date TEXT, plan_amount REAL, actual_amount REAL DEFAULT 0.0, status TEXT DEFAULT '待收', remark TEXT, FOREIGN KEY (project_no) REFERENCES projects(project_no) ON DELETE CASCADE",
            "action_logs": "id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, module TEXT, action_type TEXT, target_id TEXT, details TEXT"
        }
        for name, schema in tables.items():
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {name} ({schema})")
        
        # 3. 自动运维：清理 90 天前的陈旧审计日志
        try:
            cursor.execute("DELETE FROM action_logs WHERE timestamp < datetime('now', '-90 days')")
            conn.commit()
        except: pass

        conn.close()
        
        # 4. 彻底执行字段查体 (热更新双保险)
        ensure_columns()
        return True
    except Exception as e:
        print(f"Database Init Error: {e}")
        return False

# ==========================================
# 全局审计记录函数
# ==========================================
def log_action(module, action_type, target_id, details="", conn=None):
    """
    记录操作审计日志
    :param conn: 可选，如果传入已有连接，则在同一事务中提交，不独立开表 (性能爆破点)
    """
    sql = "INSERT INTO action_logs (timestamp, module, action_type, target_id, details) VALUES (datetime('now', 'localtime'),?,?,?,?)"
    params = (module, action_type, str(target_id), details)
    
    try:
        if conn:
            # 复用业务逻辑的连接，实现原子事务
            conn.execute(sql, params)
        else:
            # 兼容模式：如果没有上下文连接，则独立开表（不推荐用于高频业务）
            with get_db_conn(timeout=20) as standalone_conn:
                standalone_conn.execute(sql, params)
                standalone_conn.commit()
    except Exception as e:
        print(f"Log Action Error (Module: {module}): {e}")

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
# 辅助组件：跟进气泡卡片 (BubbleCard)
# ==========================================
class BubbleCard(CardWidget):
    def __init__(self, follow_id, date_str, name, method, duration, stage, detail, parent=None):
        super().__init__(parent)
        self.follow_id = follow_id
        
        # 整体采用水平布局，左侧放置时间轴线条，右侧放置内容
        main_h_layout = QHBoxLayout(self)
        main_h_layout.setContentsMargins(0, 0, 0, 0)
        main_h_layout.setSpacing(0)
        
        # --- 左侧：时间轴视觉线条 ---
        line_color = "#81C784" if method in ["方案演示", "面谈"] else "#BBDEFB"
        self.timeline_line = QFrame()
        self.timeline_line.setFixedWidth(5)
        self.timeline_line.setStyleSheet(f"background-color: {line_color}; border-top-left-radius: 12px; border-bottom-left-radius: 12px;")
        main_h_layout.addWidget(self.timeline_line)
        
        # --- 右侧：内容区域 ---
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
        layout.setContentsMargins(15, 12, 15, 12)
        layout.setSpacing(8)
        main_h_layout.addWidget(content_widget, 1)
        
        # 视觉区分背景色
        if method in ["方案演示", "面谈"]:
            self.setStyleSheet("BubbleCard { background-color: #E8F5E9; border: 1px solid #C8E6C9; border-radius: 12px; }")
        else:
            self.setStyleSheet("BubbleCard { background-color: #F5FAFF; border: 1px solid #E3F2FD; border-radius: 12px; }")
        
        # 启用右键菜单
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)

        # 顶部栏：[阶段] | 今日标签? + 日期 时长
        top_layout = QHBoxLayout()
        lbl_stage = BodyLabel(f"[{stage}]")
        lbl_stage.setStyleSheet("color: #2E7D32; font-weight: bold;")
        
        # 判断是否为今日
        today_str = QDate.currentDate().toString("yyyy-MM-dd")
        is_today = (date_str == today_str)
        
        # 移除可能引起乱码的 Emoji，改用纯文本确保可见性
        time_text = ""
        if is_today:
            time_text = "<b style='color: #27ae60;'>[今日]</b> "
        time_text += f"日期: {date_str}  |  时长: {duration}min"
        
        # 使用 QLabel 并明确指定富文本模式
        lbl_time = QLabel()
        lbl_time.setTextFormat(Qt.RichText)
        lbl_time.setText(time_text)
        lbl_time.setStyleSheet("color: #666666; font-size: 11px;") # 加深颜色
        lbl_time.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        
        top_layout.addWidget(lbl_stage)
        top_layout.addStretch()
        top_layout.addWidget(lbl_time)
        layout.addLayout(top_layout)
        
        # 中间：联系人
        lbl_name = BodyLabel(f"👤 {name} ({method})")
        lbl_name.setStyleSheet("font-weight: bold; font-size: 14px; color: #333;")
        layout.addWidget(lbl_name)
        
        # 内容：详情内容 (支持换行)
        lbl_detail = BodyLabel(str(detail or "无详情"))
        lbl_detail.setWordWrap(True)
        lbl_detail.setStyleSheet("color: #444; line-height: 1.5;")
        layout.addWidget(lbl_detail)

    def show_context_menu(self, pos):
        """弹出气泡菜单，支持修改与删除"""
        menu = QMenu(self)
        edit_act = menu.addAction(FIF.EDIT.icon(), "修改日志内容")
        del_act = menu.addAction(FIF.DELETE.icon(), "移除此条跟进")
        
        action = menu.exec_(self.mapToGlobal(pos))
        if action == edit_act:
            # 这里的 parent 调用链需要谨慎，我们主要通过 Dialog 实例调用逻辑
            dialog = self.window()
            if hasattr(dialog, "edit_follow"):
                dialog.edit_follow(self.follow_id)
        elif action == del_act:
            dialog = self.window()
            if hasattr(dialog, "delete_follow"):
                dialog.delete_follow(self.follow_id)

# ==========================================
# 模块界面：经营看板 (Dashboard)
# ==========================================
class DashboardPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DashboardPage")
        self.init_ui()
        # 绑定全局信号：当项目状态或跟进变动时，自动刷新看板
        SIGNAL_BUS.projectChanged.connect(self.load_data)

    def init_ui(self):
        # 0. 属性预定义 (安全占位，防止 AttributeError)
        self.kpi_total = None; self.kpi_unpaid = None; self.kpi_active = None
        self.kpi_week = None; self.kpi_month = None
        self.remind_label = None; self.finance_alert_label = None
        self.visit_list = None; self.todo_list = None
        self.red_list = None; self.orange_list = None; self.blue_list = None

        # 核心修复：引入滚动区域容器，防止布局挤压
        self.main_layout = QVBoxLayout(self)
        self.scroll_area = ScrollArea(self)
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(30, 30, 30, 30)
        self.content_layout.setSpacing(25)
        
        header = QHBoxLayout()
        header.addWidget(TitleLabel("企业经营驾驶舱"))
        header.addStretch()
        self.content_layout.addLayout(header)
        
        kpi_layout = QHBoxLayout()
        self.kpi_total = KPICard("年度成交总额", "¥ 0.00", "#27ae60")
        self.kpi_unpaid = KPICard("待收尾款总计", "¥ 0.00", "#e67e22")
        self.kpi_active = KPICard("跟进中项目数", "0 个", "#2980b9")
        self.kpi_week = KPICard("本周拜访次数", "0 次", "#9b59b6")
        self.kpi_month = KPICard("本月拜访次数", "0 次", "#34495e")
        for k in [self.kpi_total, self.kpi_unpaid, self.kpi_active, self.kpi_week, self.kpi_month]:
            kpi_layout.addWidget(k)
        self.content_layout.addLayout(kpi_layout)
        
        self.remind_label = BodyLabel("今日无特别提醒")
        self.remind_label.setStyleSheet("color: #2980b9; font-weight: bold;")
        self.content_layout.addWidget(self.remind_label)

        self.finance_alert_label = BodyLabel("")
        self.finance_alert_label.setStyleSheet("color: #D35400; font-weight: bold; background-color: #FFF3E0; border: 2px solid #FFE0B2; padding: 15px; border-radius: 10px; font-size: 14px;")
        self.finance_alert_label.hide()
        self.content_layout.addWidget(self.finance_alert_label)

        # 增加今日核心任务展示区
        self.today_task_card = CardWidget()
        self.today_task_card.setStyleSheet("background-color: #F3E5F5;")
        tt_layout = QVBoxLayout(self.today_task_card)
        self.tt_title = SubtitleLabel("🚀 今日核心待办")
        self.tt_content = BodyLabel("暂无紧急待办")
        self.tt_content.setWordWrap(True)
        tt_layout.addWidget(self.tt_title)
        tt_layout.addWidget(self.tt_content)
        
        # [NEW] 列表展开逻辑
        self.todo_expand_btn = PushButton("查看全部待办 (0)", self.today_task_card, FIF.DOWN)
        self.todo_expand_btn.setCursor(Qt.PointingHandCursor)
        self.todo_expand_btn.hide()
        self.todo_expand_btn.clicked.connect(self.toggle_todo_table)
        
        self.todo_expand_table = TableWidget(self.today_task_card)
        self.todo_expand_table.setColumnCount(4)
        self.todo_expand_table.setHorizontalHeaderLabels(["客户", "项目名称", "下一步计划", "执行日期"])
        self.todo_expand_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.todo_expand_table.setFixedHeight(200)
        self.todo_expand_table.hide()
        self.todo_expand_table.itemDoubleClicked.connect(self.on_row_double_clicked)
        
        tt_layout.addWidget(self.todo_expand_btn)
        tt_layout.addWidget(self.todo_expand_table)
        self.content_layout.addWidget(self.today_task_card)

        self.content_layout.addWidget(SubtitleLabel("📅 近期拜访预警 (30天内行程)"))
        self.visit_list = TableWidget()
        self.visit_list.setColumnCount(4)
        self.visit_list.setHorizontalHeaderLabels(["客户名称", "项目名称", "计划日期", "倒计时"])
        self.visit_list.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        # 统一视觉高度
        self.visit_list.setFixedHeight(200)
        self.visit_list.itemDoubleClicked.connect(self.on_row_double_clicked)
        self.content_layout.addWidget(self.visit_list)

        self.content_layout.addWidget(SubtitleLabel("合同到期红绿灯预警 (三级管控)"))
        alert_layout = QHBoxLayout()
        self.red_list = TableWidget(); self.orange_list = TableWidget(); self.blue_list = TableWidget()
        for title, color, table in [("紧急(30天)", "#e74c3c", self.red_list), 
                                   ("关注(60天)", "#e67e22", self.orange_list), 
                                   ("预见(90天)", "#2980b9", self.blue_list)]:
            v = QVBoxLayout()
            l = BodyLabel(title); l.setStyleSheet(f"color: {color}; font-weight: bold;")
            v.addWidget(l)
            table.setColumnCount(3)
            table.setHorizontalHeaderLabels(["客户", "项目", "天数"])
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            table.setFixedHeight(140)
            table.itemDoubleClicked.connect(self.on_row_double_clicked)
            v.addWidget(table)
            alert_layout.addLayout(v)
        self.content_layout.addLayout(alert_layout)
        
        self.scroll_area.setWidget(self.content_widget)
        self.main_layout.addWidget(self.scroll_area)
        
        # 核心改进：延迟首次加载，确保 UI 完全准备好，并防止死锁
        QTimer.singleShot(100, self.load_data)

    def toggle_todo_table(self):
        """切换待办表格在卡片中的可见性"""
        is_visible = self.todo_expand_table.isVisible()
        self.todo_expand_table.setVisible(not is_visible)
        if not is_visible:
            self.todo_expand_btn.setText("收起全部待办")
            self.todo_expand_btn.setIcon(FIF.UP)
        else:
            self.update_expand_btn_text()
            self.todo_expand_btn.setIcon(FIF.DOWN)

    def update_expand_btn_text(self):
        count = self.todo_expand_table.rowCount()
        self.todo_expand_btn.setText(f"查看全部待办 ({count})")

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(50, self.load_data)

    def on_row_double_clicked(self, item):
        tw = item.tableWidget()
        if not tw: return
        first_item = tw.item(item.row(), 0)
        p_no = first_item.data(Qt.UserRole) if first_item else None
        if p_no:
            try:
                dlg = ProjectDetailDialog(p_no, self)
                dlg.exec(); self.load_data()
            except Exception as e:
                InfoBar.error("失败", str(e), parent=self.window())

    def load_data(self):
        try:
            with get_db_conn() as conn:
                # 增强健壮性：优先检查关键表是否存在，防止 patch_db 未完成导致的崩溃
                table_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='payment_plans'").fetchone()
                if not table_check:
                    print("DEBUG: payment_plans 表尚未就绪，跳过财务载入")
                    return

                # 1. KPI 核心统计
                stats = conn.execute("SELECT SUM(total_amount), (SELECT SUM(actual_amount) FROM payment_plans WHERE status='已收'), (SELECT COUNT(*) FROM projects WHERE loss_reason IS NULL OR loss_reason='') FROM contracts").fetchone()
                total = stats[0] or 0.0; paid = stats[1] or 0.0; active = stats[2] or 0
                self.kpi_total.value_label.setText(f"¥ {total:,.2f}")
                self.kpi_unpaid.value_label.setText(f"¥ {total-paid:,.2f}")
                self.kpi_active.value_label.setText(f"{active} 个")
                
                # 2. 跟进频率与方式分布
                today = date.today(); t_str = today.strftime("%Y-%m-%d"); t_mmdd = today.strftime("-%m-%d")
                w_start = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
                m_start = today.replace(day=1).strftime("%Y-%m-%d")
                
                week_count = conn.execute("SELECT COUNT(*) FROM follow_ups WHERE follow_date >= ?", (w_start,)).fetchone()[0]
                month_count = conn.execute("SELECT COUNT(*) FROM follow_ups WHERE follow_date >= ?", (m_start,)).fetchone()[0]
                self.kpi_week.value_label.setText(f"{week_count} 次")
                self.kpi_month.value_label.setText(f"{month_count} 次")
                
                # 跟进方式小计 (本月)
                method_stats = conn.execute("SELECT contact_method, COUNT(*) FROM follow_ups WHERE follow_date >= ? GROUP BY contact_method", (m_start,)).fetchall()
                method_txt = " | ".join([f"{m[0]}:{m[1]}" for m in method_stats])
                
                # 3. 今日综合提醒 (拜访与生日)
                v_today = conn.execute("SELECT project_name FROM projects WHERE next_visit_date=?", (t_str,)).fetchall()
                b_today = conn.execute("SELECT name FROM contacts WHERE birthday LIKE ?", (f"%{t_mmdd}",)).fetchall()
                rems = []
                if v_today: rems.append(f"📅 今日拜访: {len(v_today)}项")
                if b_today: rems.append(f"🎂 客户生日: {', '.join([b[0] for b in b_today])}")
                if method_txt: rems.append(f"📊 本月动态: {method_txt}")
                self.remind_label.setText(" | ".join(rems) if rems else "今日无特别提醒")
                
                # 4. 7天回款闹钟 (从 payment_plans 待收逻辑抓取)
                seven_days = (today + timedelta(days=7)).strftime("%Y-%m-%d")
                f_rems = conn.execute("""
                    SELECT p.project_name, pp.plan_amount, pp.plan_date, 
                           (julianday(pp.plan_date) - julianday(?)) as days_left
                    FROM payment_plans pp 
                    JOIN projects p ON pp.project_no = p.project_no 
                    WHERE pp.status='待收' AND pp.plan_date BETWEEN ? AND ? 
                    ORDER BY pp.plan_date ASC
                """, (t_str, t_str, seven_days)).fetchall()
                
                if f_rems:
                    txts = []
                    for n, a, d, left in f_rems:
                        d_str = "今日" if int(left) == 0 else f"{int(left)}天后"
                        txts.append(f"• 【{n}】预计 {d_str} ({d}) 需回款 ¥{a:,.2f}")
                    self.finance_alert_label.setText("💰 近 7 天待收回款预警：\n" + "\n".join(txts))
                    self.finance_alert_label.show()
                else: 
                    self.finance_alert_label.hide()

                # 5. 表格刷新 (增加健壮性校验，防止 UI 未完全初始化时报错)
                target_tables = [self.todo_expand_table, self.red_list, self.orange_list, self.blue_list, self.visit_list]
                for t in target_tables:
                    if t is None: 
                        print("DEBUG: 发现未初始化的表格组件，跳过本次加载")
                        return
                    t.setRowCount(0)
                
                # 6. 获取所有待办记录 (只显示档案库中真实存在的客户项目，并增加客户名称)
                todos = conn.execute("""
                    SELECT p.project_name, p.next_plan, p.next_visit_date, p.project_no, c.name
                    FROM projects p
                    JOIN customers c ON p.customer_id = c.id
                    WHERE (p.loss_reason IS NULL OR p.loss_reason='') 
                      AND (p.stage != '已成交') 
                      AND p.next_visit_date IS NOT NULL 
                    ORDER BY p.next_visit_date ASC LIMIT 15
                """).fetchall()
                
                if todos:
                    # 6.1 更新核心大卡片内容（增加客户维度展示，防重名）
                    n, p, d, pno, cust_n = todos[0]
                    # 判空显示修复
                    disp_plan = str(p or "").strip() or "需尽快明确下一步动作"
                    self.tt_content.setText(f"客户: {cust_n}\n项目: {n}\n计划: {disp_plan}\n日期: {d}")
                    
                    # 6.2 填充折叠表格（全量显示，增加列）
                    for n, p, d, pno, cust_n in todos:
                        ix = self.todo_expand_table.rowCount(); self.todo_expand_table.insertRow(ix)
                        disp_plan = str(p or "").strip() or "需尽快明确下一步动作"
                        data = [cust_n, n, disp_plan, d]
                        for i, v in enumerate(data):
                            it = QTableWidgetItem(str(v))
                            # 挂载项目编号用于双击跳转
                            it.setData(Qt.UserRole, pno)
                            self.todo_expand_table.setItem(ix, i, it)
                    
                    # 6.3 更新按钮状态
                    self.todo_expand_btn.show()
                    self.update_expand_btn_text()
                else:
                    self.tt_content.setText("暂无近期紧迫任务")
                    self.todo_expand_btn.hide()
                    self.todo_expand_table.hide()

                # 7. 合同到期三色灯 (30/60/90天)
                cons = conn.execute("""
                    SELECT c.name, p.project_name, ct.end_date, p.project_no 
                    FROM contracts ct 
                    JOIN projects p ON ct.project_no = p.project_no 
                    JOIN customers c ON p.customer_id = c.id
                """).fetchall()
                for cust, proj, d, pno in cons:
                    try:
                        diff = (datetime.strptime(d, "%Y-%m-%d").date() - today).days
                        target = None
                        if 0 <= diff <= 30: target = self.red_list
                        elif 30 < diff <= 60: target = self.orange_list
                        elif 60 < diff <= 90: target = self.blue_list
                        if target:
                            ix = target.rowCount(); target.insertRow(ix)
                            it = QTableWidgetItem(cust); it.setData(Qt.UserRole, pno)
                            target.setItem(ix, 0, it)
                            target.setItem(ix, 1, QTableWidgetItem(proj))
                            target.setItem(ix, 2, QTableWidgetItem(f"{diff}天"))
                    except: pass

                # 8. 近期拜访预警
                v_30 = conn.execute("""
                    SELECT c.name, p.project_name, p.next_visit_date, p.project_no 
                    FROM projects p 
                    JOIN customers c ON p.customer_id = c.id 
                    WHERE p.next_visit_date IS NOT NULL AND p.next_visit_date != '' 
                      AND (loss_reason IS NULL OR loss_reason='') 
                    ORDER BY p.next_visit_date ASC
                """).fetchall()
                for cust, proj, d, pno in v_30:
                    try:
                        diff = (datetime.strptime(d, "%Y-%m-%d").date() - today).days
                        if 0 <= diff <= 30:
                            ix = self.visit_list.rowCount(); self.visit_list.insertRow(ix)
                            it = QTableWidgetItem(cust); it.setData(Qt.UserRole, pno)
                            self.visit_list.setItem(ix, 0, it)
                            self.visit_list.setItem(ix, 1, QTableWidgetItem(proj))
                            self.visit_list.setItem(ix, 2, QTableWidgetItem(d))
                            it_d = QTableWidgetItem(f"{diff}天")
                            if diff <= 7: it_d.setForeground(QColor("#e74c3c"))
                            self.visit_list.setItem(ix, 3, it_d)
                    except: pass
        except Exception as e:
            traceback.print_exc()
            self.remind_label.setText(f"数据加载异常: {str(e)}")


class CustomerContactMatrix(QDialog):
    """客户联系人决策矩阵展示"""
    def __init__(self, cust_id, cust_name, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"联系人矩阵 - {cust_name}")
        self.resize(800, 400)
        layout = QVBoxLayout(self)
        
        # 统计信息
        self.info = BodyLabel(f"当前共有该客户联系人记录")
        layout.addWidget(self.info)
        
        self.table = TableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["姓名", "职位", "部门", "电话", "角色", "主谈人"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)
        
        self.load_contacts(cust_id)

    def load_contacts(self, cust_id):
        self.table.setRowCount(0)
        with get_db_conn() as conn:
            cur = conn.execute("SELECT name, post, dept, phone, role_type, is_decision_maker FROM contacts WHERE customer_id=?", (cust_id,))
            rows = cur.fetchall()
            self.info.setText(f"客户「{self.windowTitle().split(' - ')[-1]}」决策链记录: 共 {len(rows)} 人")
            for r in rows:
                idx = self.table.rowCount(); self.table.insertRow(idx)
                for i in range(5):
                    self.table.setItem(idx, i, QTableWidgetItem(str(r[i] or "")))
                # 决策者标记
                dm = "★ 关键决策" if r[5] else "-"
                it = QTableWidgetItem(dm)
                if r[5]: it.setForeground(QColor("#d32f2f"))
                self.table.setItem(idx, 5, it)


class MasterDataPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("MasterDataPage")
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(30, 30, 30, 30)
        self.main_layout.setSpacing(20)
        
        self.main_layout.addWidget(TitleLabel("基础档案中心"))
        
        # 核心改进：卡片化容器包装 Pivot 和 StackedWidget
        self.container = CardWidget(self)
        self.container_layout = QVBoxLayout(self.container)
        self.container_layout.setContentsMargins(10, 10, 10, 10)
        
        self.pivot = Pivot(self.container)
        self.stacked = QStackedWidget(self.container)
        
        # 子界面
        self.customer_view = QWidget()
        self.supplier_view = QWidget()
        
        self.init_customer_view()
        self.init_supplier_view()
        
        self.stacked.addWidget(self.customer_view)
        self.stacked.addWidget(self.supplier_view)
        
        self.pivot.addItem("customers", "客户档案库", lambda: self.stacked.setCurrentWidget(self.customer_view))
        self.pivot.addItem("suppliers", "供应商库", lambda: self.stacked.setCurrentWidget(self.supplier_view))
        
        self.container_layout.addWidget(self.pivot)
        self.container_layout.addWidget(self.stacked)
        self.main_layout.addWidget(self.container)
        
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
        layout.setContentsMargins(0, 15, 0, 0) # 内部留白
        
        btn_bar = QHBoxLayout()
        self.add_cust_btn = PrimaryPushButton(FIF.ADD, "新增客户")
        
        # [NEW] 快捷全局搜索框
        self.cust_search = SearchLineEdit()
        self.cust_search.setPlaceholderText("快速过滤名称或行业...")
        self.cust_search.setFixedWidth(300)
        self.cust_search.textChanged.connect(self.load_customers)
        
        btn_bar.addWidget(self.add_cust_btn)
        btn_bar.addStretch()
        btn_bar.addWidget(self.cust_search)
        layout.addLayout(btn_bar)
        
        self.cust_table = TableWidget()
        self.cust_table.setColumnCount(6) # 增加一列 [操作]
        self.cust_table.setHorizontalHeaderLabels(["ID", "客户名称", "行业", "级别", "地址", "操作"])
        
        # 细节优化：ID 列瘦身
        self.cust_table.setColumnWidth(0, 60)
        self.cust_table.setColumnWidth(2, 100)
        self.cust_table.setColumnWidth(3, 80)
        self.cust_table.setColumnWidth(5, 160) # 调宽操作列以容纳 4 个按钮
        self.cust_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Fixed)
        self.cust_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch) # 地址拉长
        
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
        keyword = self.cust_search.text().strip()
        self.cust_table.setRowCount(0)
        with get_db_conn() as conn:
            sql = "SELECT id, name, industry, level, address FROM customers"
            if keyword:
                sql += f" WHERE name LIKE '%{keyword}%' OR industry LIKE '%{keyword}%'"
            
            cur = conn.execute(sql)
            for r in cur:
                idx = self.cust_table.rowCount()
                self.cust_table.insertRow(idx)
                
                # 填充数据
                for i, v in enumerate(r):
                    item = QTableWidgetItem(str(v or ""))
                    
                    # 行业字段颜色化 (政府/国企/企业)
                    if i == 2: 
                        val = str(v)
                        if "政府" in val: item.setForeground(QColor("#1976d2")) # 蓝色
                        elif "国企" in val: item.setForeground(QColor("#f57c00")) # 橙色
                        elif "企业" in val: item.setForeground(QColor("#388e3c")) # 绿色
                        item.setFont(QFont("Microsoft YaHei", 9, QFont.Bold))
                    
                    # 地址列 ToolTip
                    if i == 4: item.setToolTip(str(v or ""))
                        
                    self.cust_table.setItem(idx, i, item)
                
                # 6. 为操作列添加快捷图标按钮
                self.add_customer_actions(idx, r[0], r[1])

    def add_customer_actions(self, row, cust_id, cust_name):
        """添加 [✏️ 修改] [🗑️ 删除] [👤 查看/联系] 与 [🚀 快速立项] 按钮"""
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(5, 0, 5, 0)
        layout.setSpacing(8)
        
        # ✏️ 修改按钮
        btn_edit = TransparentToolButton(FIF.EDIT, container)
        btn_edit.setToolTip("修改此客户基本信息")
        btn_edit.clicked.connect(lambda: self.show_edit_customer(cust_id))
        
        # 👤 预览联系人按钮
        contact_btn = TransparentToolButton(FIF.PEOPLE, container)
        contact_btn.setToolTip("决策链/联系人矩阵预览")
        contact_btn.clicked.connect(lambda: self.show_customer_contacts(cust_id, cust_name))
        
        # 🚀 一键立项按钮
        project_btn = TransparentToolButton(FIF.SEND, container)
        project_btn.setToolTip("对此客户一键开启新立项")
        project_btn.clicked.connect(lambda: self.quick_create_project(cust_name))
        
        # 🗑️ 删除按钮
        btn_del = TransparentToolButton(FIF.DELETE, container)
        btn_del.setToolTip("永久删除客户档案")
        btn_del.clicked.connect(lambda: self.confirm_delete_customer(cust_id, cust_name))
        
        layout.addWidget(btn_edit)
        layout.addWidget(contact_btn)
        layout.addWidget(project_btn)
        layout.addWidget(btn_del)
        layout.addStretch()
        self.cust_table.setCellWidget(row, 5, container)

    def confirm_delete_customer(self, cust_id, cust_name):
        """深度安全删除客户逻辑：确保物理级联清理"""
        res = MessageBox("确定永久删除客户?", f"即将移除客户 [{cust_name}]。这将同步永久删除该客户下所有的：\n1. 联系人矩阵\n2. 销售项目\n3. 跟进记录与待办\n4. 报价与合同资料\n\n此操作不可撤销，确认吗？", self.window())
        if res.exec():
            try:
                with get_db_conn() as conn:
                    # 1. 识别该客户名下的所有项目编号
                    p_nos = [r[0] for r in conn.execute("SELECT project_no FROM projects WHERE customer_id=?", (cust_id,)).fetchall()]
                    
                    # 2. 手动清理关联业务数据
                    for p_no in p_nos:
                        conn.execute("DELETE FROM follow_ups WHERE project_no=?", (p_no,))
                        conn.execute("DELETE FROM quotations WHERE project_no=?", (p_no,))
                        conn.execute("DELETE FROM contracts WHERE project_no=?", (p_no,))
                        conn.execute("DELETE FROM payment_plans WHERE project_no=?", (p_no,))
                    
                    # 3. 删除项目与联系人
                    conn.execute("DELETE FROM projects WHERE customer_id=?", (cust_id,))
                    conn.execute("DELETE FROM contacts WHERE customer_id=?", (cust_id,))
                    
                    # 4. 删除客户主体
                    conn.execute("DELETE FROM customers WHERE id=?", (cust_id,))
                    
                    # 5. 原子级记录日志 (注意这里传入了 conn)
                    log_action("基础档案", "深度删除客户", cust_name, f"ID: {cust_id}", conn=conn)
                    
                    conn.commit()
            except Exception as e:
                InfoBar.error("删除失败", str(e), duration=3000, parent=self.window())
                return
            
            # [核心刷新] 只有在事务成功提交后才触发 UI 信号
            SIGNAL_BUS.projectChanged.emit() 
            self.load_customers()
            InfoBar.success("深度清理完成", f"客户 {cust_name} 及其所有历史业务数据已从系统中彻底移除", duration=3000, parent=self.window())

    def show_edit_customer(self, cust_id):
        """弹出修改客户对话框"""
        with get_db_conn() as conn:
            data = conn.execute("SELECT name, industry, level, address FROM customers WHERE id=?", (cust_id,)).fetchone()
        
        if not data: return
        
        dlg = QDialog(self)
        dlg.setWindowTitle("修改客户档案")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        name = LineEdit(); name.setText(data[0])
        industry = EditableComboBox(); industry.setText(data[1])
        industry.addItems(["政府", "国企", "企业"])
        
        level = ComboBox()
        level_items = ["A", "B", "C"]
        level.addItems(["A (重点)", "B (普通)", "C (初触)"])
        try: level.setCurrentIndex(level_items.index(data[2]))
        except: pass
        
        addr = LineEdit(); addr.setText(data[3] or "")
        
        form.addRow("客户全称*:", name)
        form.addRow("所属行业:", industry)
        form.addRow("客户等级:", level)
        form.addRow("联系地址:", addr)
        layout.addLayout(form)
        
        btn = PrimaryPushButton("保存变更")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        
        if dlg.exec():
            if not name.text(): return
            with get_db_conn() as conn:
                conn.execute("UPDATE customers SET name=?, industry=?, level=?, address=? WHERE id=?",
                            (name.text(), industry.currentText(), level.currentText()[0], addr.text(), cust_id))
                conn.commit()
            log_action("基础档案", "更新客户", name.text(), f"级别: {level.currentText()}")
            self.load_customers()
            InfoBar.success("已更新", f"客户 {name.text()} 资料已同步至数据库", duration=2000, parent=self.window())

    def show_customer_contacts(self, cust_id, cust_name):
        """弹出联系人决策矩阵对话框"""
        dlg = CustomerContactMatrix(cust_id, cust_name, self)
        dlg.exec()

    def quick_create_project(self, cust_name):
        """跨页面联动：跳转至项目管理并自动携带客户信息"""
        main_win = self.window()
        if hasattr(main_win, 'create_new_project_for'):
            main_win.create_new_project_for(cust_name)

    def load_suppliers(self):
        self.supp_table.setRowCount(0)
        with get_db_conn() as conn:
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
            try:
                with get_db_conn() as conn:
                    conn.execute("INSERT INTO customers (name, industry, level, address) VALUES (?,?,?,?)",
                                (name.text(), industry.currentText(), level.currentText()[0], addr.text()))
                    # 原子化日志记录
                    log_action("基础档案", "新增客户", name.text(), f"级别: {level.currentText()}", conn=conn)
                    conn.commit()
                self.load_customers()
                InfoBar.success("保存成功", f"客户 {name.text()} 已入库", duration=2000, parent=self.window())
            except Exception as e:
                InfoBar.error("错误", f"保存失败: {str(e)}", parent=self.window())

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
            try:
                with get_db_conn() as conn:
                    conn.execute("INSERT INTO suppliers (name, category, contact_person, phone) VALUES (?,?,?,?)",
                                (name.text(), cat.currentText(), cp.text(), ph.text()))
                    # 原子化日志记录
                    log_action("基础档案", "新增供应商", name.text(), f"类别: {cat.currentText()}", conn=conn)
                    conn.commit()
                self.load_suppliers()
                InfoBar.success("保存成功", f"供应商 {name.text()} 已入库", duration=2000, parent=self.window())
            except Exception as e:
                InfoBar.error("错误", f"保存失败: {str(e)}", parent=self.window())

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
        with get_db_conn() as conn:
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
        self.table.setHorizontalHeaderLabels(["ID", "姓名", "职务", "电话", "角色类型", "生日"])
        self.table.setEditTriggers(TableWidget.NoEditTriggers)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.itemDoubleClicked.connect(self.on_item_double_click)
        layout.addWidget(self.table)
        self.load_contacts()

    def show_context_menu(self, pos):
        menu = QMenu(self)
        edit_act = menu.addAction(FIF.EDIT.icon(), "修改联系人")
        del_act = menu.addAction(FIF.DELETE.icon(), "移除此联系人")
        action = menu.exec_(self.table.mapToGlobal(pos))
        
        idx = self.table.indexAt(pos).row()
        if idx < 0: return
        cid = self.table.item(idx, 0).data(Qt.UserRole)

        if action == edit_act:
            self.add_contact(contact_id=cid)
        elif action == del_act:
            self.delete_contact(cid)

    def on_item_double_click(self, item):
        cid = self.table.item(item.row(), 0).data(Qt.UserRole)
        self.add_contact(contact_id=cid)

    def delete_contact(self, contact_id):
        res = MessageBox("确定删除?", "即将物理移除该联系人档案，确认继续？", self)
        if res.exec():
            with get_db_conn() as conn:
                conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,))
                conn.commit()
            # 记录删除日志
            log_action("联系人", "删除联系人", str(contact_id), f"所属客户: {self.name}")
            self.load_contacts()
            InfoBar.success("已移除", "联系人数据已从矩阵中注销", duration=2000, parent=self)

    def load_contacts(self):
        self.table.setRowCount(0)
        # 响应式拉伸
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        with get_db_conn() as conn:
            cur = conn.execute("SELECT id, name, post, phone, role_type, birthday FROM contacts WHERE customer_id=?", (self.customer_id,))
            for r in cur:
                idx = self.table.rowCount()
                self.table.insertRow(idx)
                is_decision_maker = (r[4] == "决策者")
                for i, v in enumerate(r):
                    item = QTableWidgetItem(str(v or ""))
                    if i == 0: # 将 ID 存入第 0 列的 UserRole
                        item.setData(Qt.UserRole, v)
                    if is_decision_maker:
                        # 决策人视觉高亮：金色背景 + 红色粗体
                        item.setBackground(QColor("#f1c40f"))
                        if i == 1: # 姓名列加粗红字
                            font = item.font()
                            font.setBold(True)
                            item.setFont(font)
                            item.setForeground(QColor("#c0392b"))
                    self.table.setItem(idx, i, item)

    def add_contact(self, contact_id=None):
        dlg = QDialog(self)
        dlg.setWindowTitle("新增联系人" if not contact_id else "修改联系人资料")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        name = LineEdit(); post = LineEdit(); phone = LineEdit(); role_cb = ComboBox(); bday = DateEdit()
        role_cb.addItems(["经办人", "决策者", "影响者"])
        
        if contact_id:
            with get_db_conn() as conn:
                res = conn.execute("SELECT name, post, phone, role_type, birthday FROM contacts WHERE id=?", (contact_id,)).fetchone()
                if res:
                    name.setText(res[0]); post.setText(res[1]); phone.setText(res[2])
                    role_cb.setCurrentText(res[3])
                    if res[4]:
                        bday.setDate(QDate.fromString(res[4], "yyyy-MM-dd"))
        
        form.addRow("姓名*:", name)
        form.addRow("职务/部门:", post)
        form.addRow("手机/电话:", phone)
        form.addRow("角色类型:", role_cb)
        form.addRow("生日:", bday)
        layout.addLayout(form)
        btn = PrimaryPushButton("保存变更" if contact_id else "确认新增")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        if dlg.exec():
            with get_db_conn() as conn:
                if contact_id:
                    conn.execute("UPDATE contacts SET name=?, post=?, phone=?, role_type=?, birthday=? WHERE id=?",
                                (name.text(), post.text(), phone.text(), role_cb.currentText(), bday.date().toString("yyyy-MM-dd"), contact_id))
                    log_action("联系人", "修改联系人", name.text(), f"职位: {post.text()}, 客户: {self.name}")
                else:
                    conn.execute("INSERT INTO contacts (customer_id, name, post, phone, role_type, birthday) VALUES (?,?,?,?,?,?)",
                                (self.customer_id, name.text(), post.text(), phone.text(), role_cb.currentText(), bday.date().toString("yyyy-MM-dd")))
                    log_action("联系人", "新增联系人", name.text(), f"职位: {post.text()}, 客户: {self.name}")
            self.load_contacts()
            InfoBar.success("已更新", "联系人档案操作成功", duration=2000, parent=self)

# ==========================================
# 占位页面：项目、报价、合同
# ==========================================
def get_next_project_no():
    prefix = datetime.now().strftime("%y%m%d")
    with get_db_conn() as conn:
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
        self.init_ui(layout)
        self.load_projects()

    def show_add_project_with_customer(self, customer_name):
        """[NEW] 为快速立项设计的入口"""
        self.add_project(prefilled_customer=customer_name)

    def init_ui(self, layout):
        
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
        
        # 绑定全局信号：当项目状态或跟进变动时，自动刷新列表
        SIGNAL_BUS.projectChanged.connect(self.load_projects)
        
        self.load_projects()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_projects()

    def load_projects(self):
        st = self.search_box.text().strip()
        try:
            self.table.setRowCount(0)
            with get_db_conn(timeout=10) as conn:
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

    def add_project(self, prefilled_customer=None):
        dlg = QDialog(self)
        dlg.setWindowTitle("项目立项")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        # 客户选择器
        cust_cb = ComboBox()
        cust_ids = []
        preselect_idx = -1
        with get_db_conn() as conn:
            for i, r in enumerate(conn.execute("SELECT id, name FROM customers")):
                cust_ids.append(r[0]); cust_cb.addItem(r[1])
                if prefilled_customer and r[1] == prefilled_customer:
                    preselect_idx = i
        
        if not cust_ids:
            InfoBar.warning("提示", "请先在【基础档案】中建立客户库", parent=self.window())
            return

        if preselect_idx != -1:
            cust_cb.setCurrentIndex(preselect_idx)
            cust_cb.setEnabled(False) # 锁定，防止误改

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
            try:
                with get_db_conn() as conn:
                    conn.execute("INSERT INTO projects (project_no, customer_id, project_name, stage) VALUES (?,?,?,?)",
                                (p_no, cid, name.text(), stage.currentText()))
                    # 原子化日志记录
                    log_action("项目跟进", "新立项", p_no, f"项目名: {name.text()}, 初始阶段: {stage.currentText()}", conn=conn)
                    conn.commit()
                self.load_projects()
                InfoBar.success("立项成功", f"项目编号 {p_no} 已创建", duration=2000, parent=self.window())
            except Exception as e:
                InfoBar.error("立项失败", f"数据库写入错误: {str(e)}", duration=3000, parent=self.window())

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
    def __init__(self, project_no, parent=None, start_tab="follows"):
        super().__init__(parent)
        print(f"DEBUG: 初始化项目详情 [{project_no}]，初始标签: {start_tab}")
        self.project_no = project_no
        self.start_tab = start_tab
        self.customer_id = None
        self.current_stage = ""
        self.customer_name = ""
        self.p_name = "" 
        
        try:
            with get_db_conn() as conn:
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
        
        # 1. 跟进记录页
        self.follow_page = QWidget()
        self.init_follow_page()
        self.stacked.addWidget(self.follow_page)
        self.pivot.addItem("follows", "过程跟进记录", lambda: self.stacked.setCurrentWidget(self.follow_page))
        
        # 2. 报价历史页
        self.quote_page = QWidget()
        self.init_quote_page()
        self.stacked.addWidget(self.quote_page)
        self.pivot.addItem("quotes", "报价历史版本", lambda: self.stacked.setCurrentWidget(self.quote_page))

        # 3. 财务与合同页
        self.finance_page = QWidget()
        self.init_finance_page()
        self.stacked.addWidget(self.finance_page)
        self.pivot.addItem("finance", "财务合同与回款", lambda: self.stacked.setCurrentWidget(self.finance_page))

        self.pivot.setCurrentItem(self.start_tab)
        print(f"DEBUG: 已设置初始标签为 {self.start_tab}")
        print("DEBUG: UI 组件加载完成")

    def init_follow_page(self):
        layout = QVBoxLayout(self.follow_page)
        btn_bar = QHBoxLayout()
        add_btn = PrimaryPushButton(FIF.ADD, "新增跟进日志")
        add_btn.clicked.connect(self.add_follow)
        btn_bar.addWidget(add_btn)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        
        self.follow_scroll = ScrollArea()
        self.follow_scroll.setWidgetResizable(True)
        self.follow_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        
        self.follow_container = QWidget()
        self.follow_layout = QVBoxLayout(self.follow_container)
        self.follow_layout.setAlignment(Qt.AlignTop)
        self.follow_layout.setSpacing(15)
        self.follow_scroll.setWidget(self.follow_container)
        
        layout.addWidget(self.follow_scroll, 1)
        self.load_follows()

    def load_follows(self):
        # 清除旧内容
        while self.follow_layout.count():
            item = self.follow_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        with get_db_conn() as conn:
            query = """
                SELECT id, follow_date, contact_name, contact_method, follow_duration, stage, detail, next_plan 
                FROM follow_ups WHERE project_no=? ORDER BY follow_date DESC, id DESC
            """
            cur = conn.execute(query, (self.project_no,))
            records = cur.fetchall()
            for r in records:
                f_id, f_date, c_name, c_method, c_dur, stage, detail, next_p = r
                card = BubbleCard(f_id, f_date, c_name, c_method, c_dur, stage, detail, self)
                # 如果有下一步计划，在底层加一个小提示
                if next_p:
                    it_p = BodyLabel(f"🎯 下一步: {next_p}")
                    # 将下一步计划也放入 content_widget 的布局中（BubbleCard 第 1 个 widget 的内容布局）
                    # 注意：BubbleCard 结构变了，这里需要准确定位
                    content_layout = card.layout().itemAt(1).widget().layout()
                    it_p.setStyleSheet("color: #E65100; font-size: 11px; margin-top: -5px; padding-left: 10px;")
                    content_layout.addWidget(it_p)
                self.follow_layout.addWidget(card)
            
            if not records:
                self.follow_layout.addWidget(BodyLabel("暂无跟进记录", self))

    def add_follow(self):
        self._show_follow_dialog()

    def edit_follow(self, follow_id):
        self._show_follow_dialog(follow_id)

    def delete_follow(self, follow_id):
        res = MessageBox("确定删除记录?", "即将永久移除此条跟进日志，操作无法撤销。确认吗？", self)
        if res.exec():
            with get_db_conn() as conn:
                conn.execute("DELETE FROM follow_ups WHERE id=?", (follow_id,))
                conn.commit()
            log_action("项目跟进", "删除跟进", self.project_no, f"记录ID: {follow_id}")
            self.load_follows()
            SIGNAL_BUS.projectChanged.emit() # 全局刷新信号
            InfoBar.success("已移除", "跟进记录已物理删除", duration=2000, parent=self)

    def _show_follow_dialog(self, edit_id=None):
        dlg = QDialog(self)
        dlg.setWindowTitle("记录商谈跟进" if not edit_id else "修改跟进日志")
        dlg.setMinimumWidth(450)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        date_e = CalendarPicker(); date_e.setDate(QDate.currentDate())
        stage_cb = ComboBox()
        stages = ["初期线索", "有预算", "商机接洽", "报价阶段", "合同阶段", "已成交", "已流失"]
        stage_cb.addItems(stages)
        if self.current_stage in stages: stage_cb.setCurrentText(self.current_stage)
        
        contact_cb = EditableComboBox()
        with get_db_conn() as conn:
            contacts = conn.execute("SELECT name, phone FROM contacts WHERE customer_id=?", (self.customer_id,)).fetchall()
            for name, phone in contacts:
                contact_cb.addItem(f"{name} ({phone})")
        
        detail = TextEdit()
        plan = LineEdit()
        visit_date = CalendarPicker()
        visit_date.setDate(QDate.currentDate().addDays(7))
        
        loss_reason_input = LineEdit(); loss_reason_input.setPlaceholderText("如果是流失/丢单，请在此输入原因...")
        loss_reason_input.hide()
        
        def update_visibility(t):
            is_closed = t in ["已成交", "已流失"]
            loss_reason_input.setVisible(t == "已流失")
            visit_date.setDisabled(is_closed)
            if is_closed: visit_date.setDate(QDate.currentDate())
        
        stage_cb.currentTextChanged.connect(update_visibility)
        
        method_cb = ComboBox()
        method_cb.addItems(["面谈", "电话", "微信", "方案演示", "商务宴请", "其他"])
        duration_le = LineEdit()
        duration_le.setPlaceholderText("分钟")
        duration_le.setText("30")

        # 如果是编辑模式，载入旧数据
        if edit_id:
            with get_db_conn() as conn:
                r = conn.execute("SELECT follow_date, contact_name, contact_method, follow_duration, stage, detail, next_plan FROM follow_ups WHERE id=?", (edit_id,)).fetchone()
                if r:
                    date_e.setDate(QDate.fromString(r[0], "yyyy-MM-dd"))
                    contact_cb.setCurrentText(r[1])
                    method_cb.setCurrentText(r[2])
                    duration_le.setText(str(r[3]))
                    stage_cb.setCurrentText(r[4])
                    detail.setText(r[5])
                    plan.setText(r[6] or "")
        else:
            # 智能继承：自动填充最后一次录入的联系人和方式
            try:
                with get_db_conn() as conn:
                    last_r = conn.execute("SELECT contact_name, contact_method FROM follow_ups WHERE project_no=? ORDER BY id DESC LIMIT 1", (self.project_no,)).fetchone()
                    if last_r:
                        contact_cb.setCurrentText(last_r[0])
                        method_cb.setCurrentText(last_r[1])
            except: pass

        update_visibility(stage_cb.currentText())

        form.addRow("跟进日期*:", date_e)
        form.addRow("跟进阶段*:", stage_cb)
        form.addRow("流失原因:", loss_reason_input) 
        form.addRow("跟进方式*:", method_cb)
        form.addRow("沟通时长(分钟):", duration_le)
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
            if not contact_cb.currentText():
                InfoBar.warning("必填项缺失", "请选择或输入面会人", duration=2000, parent=dlg)
                return
            if not is_closed and not detail.toPlainText().strip():
                InfoBar.warning("必填项缺失", "请填写详细跟进内容", duration=2000, parent=dlg)
                return
            dlg.accept()

        btn.clicked.connect(on_save)
        
        if dlg.exec():
            new_stage = stage_cb.currentText()
            d_str = date_e.date.toString("yyyy-MM-dd")
            v_str = visit_date.date.toString("yyyy-MM-dd")
            try: dur = int(duration_le.text() or 0)
            except: dur = 0
            try:
                with get_db_conn() as conn:
                    if edit_id:
                        conn.execute("UPDATE follow_ups SET follow_date=?, contact_name=?, contact_method=?, follow_duration=?, stage=?, detail=?, next_plan=? WHERE id=?",
                                    (d_str, contact_cb.currentText(), method_cb.currentText(), dur, new_stage, detail.toPlainText(), plan.text(), edit_id))
                    else:
                        conn.execute("INSERT INTO follow_ups (project_no, follow_date, contact_name, contact_method, follow_duration, stage, detail, next_plan) VALUES (?,?,?,?,?,?,?,?)",
                                    (self.project_no, d_str, contact_cb.currentText(), method_cb.currentText(), dur, new_stage, detail.toPlainText(), plan.text()))
                    
                    # 更新项目状态 (同步发射全局信号)
                    if new_stage in ["已成交", "已流失"]:
                        conn.execute("UPDATE projects SET stage=?, next_visit_date=NULL, loss_reason=? WHERE project_no=?",
                                    (new_stage, loss_reason_input.text() if new_stage=="已流失" else None, self.project_no))
                    else:
                        conn.execute("UPDATE projects SET stage=?, next_visit_date=?, loss_reason=NULL WHERE project_no=?",
                                    (new_stage, v_str, self.project_no))
                    
                    self.current_stage = new_stage
                    # 原子化日志记录
                    log_action("项目跟进", "修改跟进" if edit_id else "新增跟进", self.project_no, 
                               f"面会人: {contact_cb.currentText()}, 阶段: {new_stage}, 详情: {detail.toPlainText()[:30]}...", conn=conn)
                    conn.commit()
                self.load_follows()
            except Exception as e:
                InfoBar.error("同步失败", f"数据库写入错误: {str(e)}", duration=3000, parent=self.window())
            SIGNAL_BUS.projectChanged.emit() # 全局刷新信号
            InfoBar.success("已更新", "跟进数据已同步至云端" if not edit_id else "记录已修正", duration=2000)

    # --- 报价历史子页逻辑 ---
    def init_quote_page(self):
        layout = QVBoxLayout(self.quote_page)
        self.quote_table = TableWidget()
        self.quote_table.setColumnCount(5)
        self.quote_table.setHorizontalHeaderLabels(["报价日期", "版本", "金额 (¥)", "附件", "操作"])
        self.quote_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.quote_table)
        self.load_project_quotes()

    def load_project_quotes(self):
        self.quote_table.setRowCount(0)
        with get_db_conn() as conn:
            # 引入 remark 以便编辑时透传数据
            cur = conn.execute("SELECT quote_date, version, amount, file_path, remark FROM quotations WHERE project_no=? ORDER BY quote_date DESC", (self.project_no,))
            for r in cur:
                idx = self.quote_table.rowCount()
                self.quote_table.insertRow(idx)
                items = [str(r[0]), str(r[1]), f"¥{r[2]:,.2f}", "有附件" if r[3] else "无"]
                for i, v in enumerate(items):
                    self.quote_table.setItem(idx, i, QTableWidgetItem(v))
                
                # 1. 附件展示 (保持原逻辑，但移至附件列)
                if r[3]:
                    btn_view = TransparentToolButton(FIF.FOLDER, self)
                    btn_view.setToolTip("打开方案附件")
                    btn_view.clicked.connect(lambda _, p=r[3]: self._open_quote_file(p))
                    self.quote_table.setCellWidget(idx, 3, btn_view)

                # 2. 操作列 (注入修改与删除)
                self.add_quote_actions(idx, r)

    def _open_quote_file(self, p):
        abs_p = os.path.join(get_app_dir(), p) if not os.path.isabs(p) else p
        if os.path.exists(abs_p):
            QDesktopServices.openUrl(QUrl.fromLocalFile(os.path.abspath(abs_p)))
        else:
            InfoBar.error("文件缺失", f"未找到文件: {os.path.basename(abs_p)}", parent=self)

    def add_quote_actions(self, row, r):
        """注入修改与删除按钮"""
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(5, 0, 5, 0); layout.setSpacing(8)
        
        edit_data = {"p_no": self.project_no, "date": r[0], "ver": r[1], "amt": r[2], "path": r[3], "remark": r[4]}
        
        btn_edit = TransparentToolButton(FIF.EDIT, container)
        btn_edit.setToolTip("修改此版本报价")
        btn_edit.clicked.connect(lambda: self.edit_project_quote(edit_data))
        
        btn_del = TransparentToolButton(FIF.DELETE, container)
        btn_del.setToolTip("物理删除此记录")
        btn_del.clicked.connect(lambda: self.delete_project_quote(self.project_no, r[1]))
        
        layout.addWidget(btn_edit)
        layout.addWidget(btn_del)
        layout.addStretch()
        self.quote_table.setCellWidget(row, 4, container)

    def delete_project_quote(self, p_no, ver):
        """物理删除项目详情中的特定报价"""
        res = MessageBox("确定删除记录?", f"即将永久移除项目 [{p_no}] 的版本 [{ver}]。确认吗？", self)
        if res.exec():
            with get_db_conn(timeout=10) as conn:
                conn.execute("DELETE FROM quotations WHERE project_no=? AND version=?", (p_no, ver))
                conn.commit()
            log_action("报价管理", "删除报价(详情页)", p_no, f"版本: {ver}")
            self.load_project_quotes()
            # 同样通知主页面刷新
            SIGNAL_BUS.projectChanged.emit() 
            InfoBar.success("已移除", "报价记录已从数据库物理删除", duration=2000, parent=self)

    def edit_project_quote(self, edit_data):
        """修复：通过递归向上层查找主窗口实例，桥接报价组件的编辑功能"""
        p = self.parent()
        while p:
            if hasattr(p, 'quotation_page'):
                p.quotation_page.add_quote(edit_data=edit_data)
                # 修改完成后刷新本详情页的报价列表
                self.load_project_quotes()
                return
            p = p.parent()
        
        # 如果遍历到顶层仍未找到（理论上不会发生），给出明确提示
        InfoBar.warning("操作受限", "无法直接联动报价组件，请前往【报价管理】主页面更新", parent=self)

    # --- 财务合同子页逻辑 ---
    def init_finance_page(self):
        layout = QVBoxLayout(self.finance_page)
        
        # 上半部分：合同摘要
        self.contract_info = CardWidget()
        ci_layout = QFormLayout(self.contract_info)
        self.lbl_contract_period = BodyLabel("未签署合同")
        self.lbl_total_amt = BodyLabel("¥ 0.00")
        self.lbl_paid_amt = BodyLabel("¥ 0.00")
        self.progress_bar = ProgressBar()
        self.progress_bar.setFixedWidth(200)
        self.txt_memo = BodyLabel("-")
        self.txt_memo.setWordWrap(True)
        
        ci_layout.addRow("合同周期:", self.lbl_contract_period)
        ci_layout.addRow("合同总额:", self.lbl_total_amt)
        ci_layout.addRow("已收总额:", self.lbl_paid_amt)
        ci_layout.addRow("回款进度:", self.progress_bar)
        ci_layout.addRow("核心备注:", self.txt_memo)
        layout.addWidget(self.contract_info)
        
        # 中间：操作栏
        btn_bar = QHBoxLayout()
        btn_bar.addWidget(SubtitleLabel("回款计划与执行记录"))
        btn_bar.addStretch()
        add_plan_btn = PrimaryPushButton(FIF.ADD, "登记新计划/回款")
        add_plan_btn.clicked.connect(self.add_payment_plan)
        btn_bar.addWidget(add_plan_btn)
        layout.addLayout(btn_bar)
        
        # 下半部分：计划表
        self.payment_table = TableWidget()
        self.payment_table.setColumnCount(6) # 增加一列：操作
        self.payment_table.setHorizontalHeaderLabels(["计划日期", "计划金额", "实际回款", "状态", "备注", "操作"])
        self.payment_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive) # 允许手动调整
        self.payment_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.payment_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.payment_table.setEditTriggers(TableWidget.NoEditTriggers)
        # 移除右键菜单策略，改用显式操作列
        layout.addWidget(self.payment_table)
        
        self.load_project_finance()

    def load_project_finance(self):
        with get_db_conn() as conn:
            # 1. 载入合同基础信息
            contract = conn.execute("SELECT COALESCE(start_date, ''), COALESCE(end_date, ''), COALESCE(total_amount, 0.0), COALESCE(contract_memo, '') FROM contracts WHERE project_no=?", (self.project_no,)).fetchone()
            if contract:
                self.lbl_contract_period.setText(f"{contract[0]} 至 {contract[1]}")
                self.lbl_total_amt.setText(f"¥ {contract[2]:,.2f}")
                self.txt_memo.setText(contract[3] or "无特殊备注")
            
            # 2. 计算已收总额
            paid_sum = conn.execute("SELECT SUM(actual_amount) FROM payment_plans WHERE project_no=? AND status='已收'", (self.project_no,)).fetchone()[0] or 0.0
            self.lbl_paid_amt.setText(f"¥ {paid_sum:,.2f}")
            
            # 更新进度条
            if contract and contract[2] > 0:
                percent = int((paid_sum / contract[2]) * 100)
                self.progress_bar.setValue(min(percent, 100))
            else:
                self.progress_bar.setValue(0)

            if contract and paid_sum >= contract[2]:
                self.lbl_paid_amt.setTextColor(QColor("#27ae60"), QColor("#27ae60"))
            else:
                self.lbl_paid_amt.setTextColor(QColor("#e67e22"), QColor("#e67e22"))

            # 3. 载入计划列表 (包含 ID 以便后续修改删除)
            self.payment_table.setRowCount(0)
            plans = conn.execute("SELECT id, plan_date, plan_amount, actual_amount, status, remark FROM payment_plans WHERE project_no=? ORDER BY plan_date ASC", (self.project_no,)).fetchall()
            for r in plans:
                idx = self.payment_table.rowCount()
                self.payment_table.insertRow(idx)
                plan_id = r[0]
                for i in range(1, 6): # 对应 plan_date 到 remark
                    v = r[i]
                    val = f"¥{v:,.2f}" if i in [2, 3] else str(v or "")
                    item = QTableWidgetItem(val)
                    if i == 1: # 计划日期列存储 ID
                        item.setData(Qt.UserRole, plan_id)
                    if i == 4: # 状态着色
                        color = "#27ae60" if v == "已收" else "#e67e22"
                        item.setForeground(QColor(color))
                    self.payment_table.setItem(idx, i-1, item)
                
                # 注入操作按钮
                self.add_payment_actions(idx, plan_id)
            
            # 设置列宽分层
            self.payment_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch) # 备注拉伸
            self.payment_table.setColumnWidth(5, 100) # 操作列固定

    def add_payment_actions(self, row, plan_id):
        """为回款记录行注入显式操作按钮"""
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(5, 0, 5, 0)
        layout.setSpacing(8)
        
        # 修改按钮
        btn_edit = TransparentToolButton(FIF.EDIT, container)
        btn_edit.setToolTip("修改此笔计划/回款")
        btn_edit.clicked.connect(lambda: self.add_payment_plan(edit_id=plan_id))
        
        # 删除按钮
        btn_del = TransparentToolButton(FIF.DELETE, container)
        btn_del.setToolTip("移除此记录")
        btn_del.clicked.connect(lambda: self.delete_payment_plan(plan_id))
        
        layout.addWidget(btn_edit)
        layout.addWidget(btn_del)
        layout.addStretch()
        self.payment_table.setCellWidget(row, 5, container)

    def delete_payment_plan(self, plan_id):
        """删除回款记录"""
        res = MessageBox("确定删除?", "财务数据删除后无法找回，确认移除此项记录?", self.window())
        if res.exec():
            with get_db_conn(timeout=10) as conn:
                conn.execute("DELETE FROM payment_plans WHERE id=?", (plan_id,))
                conn.commit()
            # [NEW] 记录审计日志
            log_action("金融财务", "删除回款", "ID:" + str(plan_id), "手动移除一笔回款/计划记录")
            self.load_project_finance()
            InfoBar.success("已删除", "回款记录已成功锁定并移除", duration=2000)

    def add_payment_plan(self, edit_id=None):
        """登记或修改回款计划/记录"""
        dlg = QDialog(self)
        mode_title = "录入新计划/回款" if not edit_id else "修改回款记录"
        dlg.setWindowTitle(mode_title)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        p_date = CalendarPicker(); p_date.setDate(QDate.currentDate())
        p_amt = LineEdit(); p_amt.setPlaceholderText("计划金额")
        a_amt = LineEdit(); a_amt.setPlaceholderText("实际收款 (如已收)")
        status = ComboBox(); status.addItems(["待收", "已收"])
        remark = LineEdit()
        
        # [NEW] 如果是编辑模式，预填数据
        if edit_id:
            with get_db_conn() as conn:
                r = conn.execute("SELECT plan_date, plan_amount, actual_amount, status, remark FROM payment_plans WHERE id=?", (edit_id,)).fetchone()
                if r:
                    p_date.setDate(QDate.fromString(r[0], "yyyy-MM-dd"))
                    p_amt.setText(str(r[1]))
                    a_amt.setText(str(r[2]))
                    status.setCurrentText(r[3])
                    remark.setText(r[4] or "")

        form.addRow("日期*:", p_date)
        form.addRow("计划金额*:", p_amt)
        form.addRow("状态:", status)
        form.addRow("实际收款:", a_amt)
        form.addRow("摘要/备注:", remark)
        layout.addLayout(form)
        
        btn = PrimaryPushButton("保存记录")
        layout.addWidget(btn)
        
        def on_save():
            if not p_amt.text().strip():
                InfoBar.warning("输入缺失", "计划金额不能为空", duration=2000, parent=dlg)
                return
            dlg.accept()

        btn.clicked.connect(on_save)
        
        if dlg.exec():
            try:
                pa = float(p_amt.text() or 0)
                aa = float(a_amt.text() or 0)
                if status.currentText() == "已收" and aa == 0: aa = pa # 快捷处理
                
                with get_db_conn(timeout=10) as conn:
                    if edit_id:
                        conn.execute("UPDATE payment_plans SET plan_date=?, plan_amount=?, actual_amount=?, status=?, remark=? WHERE id=?",
                                    (p_date.date.toString("yyyy-MM-dd"), pa, aa, status.currentText(), remark.text(), edit_id))
                    else:
                        conn.execute("INSERT INTO payment_plans (project_no, plan_date, plan_amount, actual_amount, status, remark) VALUES (?,?,?,?,?,?)",
                                    (self.project_no, p_date.date.toString("yyyy-MM-dd"), pa, aa, status.currentText(), remark.text()))
                    conn.commit()
                log_action("金融财务", "修改回款" if edit_id else "新增回款", self.project_no, f"状态: {status.currentText()}, 金额: {aa}")
                self.load_project_finance()
                InfoBar.success("保存成功", "回款计划/记录已更新", duration=2000)
            except Exception as e:
                InfoBar.error("保存失败", f"数据处理异常: {str(e)}", parent=self)

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
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels(["客户名称", "项目名称", "关联单号", "报价日期", "版本", "金额 (¥)", "报价备注", "附件状态", "操作"])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)
        
        self.add_btn.clicked.connect(self.add_quote)
        self.table.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.load_quotes()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_quotes()

    def load_quotes(self):
        try:
            ensure_columns() # 双保险：确保查询前字段已补全
            st = self.search_box.text().strip()
            
            # [原子化渲染 第一步]：在连接保持期间仅读取数据
            rows = []
            with get_db_conn(timeout=10) as conn:
                query = """
                    SELECT c.name, p.project_name, q.project_no, q.quote_date, q.version, 
                           COALESCE(q.amount, 0.0), COALESCE(q.file_path, ''), COALESCE(q.remark, '')
                    FROM quotations q 
                    JOIN projects p ON q.project_no = p.project_no 
                    JOIN customers c ON p.customer_id = c.id 
                    WHERE (c.name LIKE ? OR p.project_name LIKE ?)
                    ORDER BY q.quote_date DESC, q.id DESC
                """
                rows = conn.execute(query, (f"%{st}%", f"%{st}%")).fetchall()
            
            # [原子化渲染 第二步]：查询成功后才清空并重建表格，防止因报错导致界面数据「消失」
            self.table.setRowCount(0)
            seen_projects = set()
            for r in rows:
                idx = self.table.rowCount(); self.table.insertRow(idx)
                p_no = str(r[2])
                is_latest = False
                if p_no not in seen_projects:
                    is_latest = True; seen_projects.add(p_no)
                
                row_data = [str(r[0]), str(r[1]), p_no, str(r[3]), str(r[4]), f"{r[5]:,.2f}", str(r[7]), "已关联" if r[6] else "未上传"]
                for i, v in enumerate(row_data):
                    item = QTableWidgetItem(v)
                    # 最新版本加粗显示
                    if is_latest:
                        font = item.font(); font.setBold(True); item.setFont(font)
                        if i == 4: item.setForeground(QColor("#2980b9"))
                        
                        # 特殊样式：金额列 (Index 5) 始终加粗且设为深橙色
                        if i == 5:
                            f = item.font(); f.setBold(True); item.setFont(f)
                            item.setForeground(QColor("#D35400"))
                        
                        # 隐式存储全量数据，供双击使用
                        if i == 0:
                            edit_dict = {"p_no": r[2], "date": r[3], "ver": r[4], "amt": r[5], "path": r[6], "remark": r[7]}
                            item.setData(Qt.UserRole, edit_dict)

                    self.table.setItem(idx, i, item)
            
                # 核心操作区 (5 图标布局)
                actions = QWidget()
                h_layout = QHBoxLayout(actions)
                h_layout.setContentsMargins(5, 0, 5, 0); h_layout.setSpacing(5)
                
                # 1. 修改 (Edit)
                btn_edit = TransparentToolButton(FIF.EDIT, actions)
                btn_edit.setToolTip("修改原报价内容")
                edit_dict = {"p_no": r[2], "date": r[3], "ver": r[4], "amt": r[5], "path": r[6], "remark": r[7]}
                btn_edit.clicked.connect(lambda _, d=edit_dict: self.add_quote(edit_data=d))
                h_layout.addWidget(btn_edit)

                # 2. 复制 (Copy)
                btn_copy = TransparentToolButton(FIF.COPY, actions)
                btn_copy.setToolTip("基于此版本快速发起新报价")
                copy_dict = {"p_no": r[2], "amt": r[5], "remark": r[7]}
                btn_copy.clicked.connect(lambda _, d=copy_dict: self.add_quote(copy_data=d))
                h_layout.addWidget(btn_copy)

                # 3. 删除 (Delete)
                btn_del = TransparentToolButton(FIF.DELETE, actions)
                btn_del.setToolTip("移除此版本报价记录")
                btn_del.clicked.connect(lambda _, p_no=r[2], v=r[4]: self.delete_quote(p_no, v))
                h_layout.addWidget(btn_del)

                # 4. 查看 (View)
                f_path = str(r[6]).lower()
                view_icon = FIF.DOCUMENT if f_path.endswith(".pdf") else FIF.FOLDER
                if f_path.endswith((".xlsx", ".xls")): view_icon = FIF.DOCUMENT
                
                btn_view = TransparentToolButton(view_icon, actions)
                btn_view.setToolTip("查看方案附件")
                btn_view.setEnabled(bool(r[6]))
                btn_view.clicked.connect(lambda _, p=r[6]: self.open_quote_file(p))
                h_layout.addWidget(btn_view)

                # 5. 转合同 (Convert)
                btn_conv = TransparentToolButton(FIF.SEND, actions)
                btn_conv.setToolTip("流转至合同阶段")
                btn_conv.clicked.connect(lambda _, p_no=r[2], amt=r[5], f=r[6]: self.convert_to_contract(p_no, amt, f))
                h_layout.addWidget(btn_conv)
                
                h_layout.addStretch()
                self.table.setCellWidget(idx, 8, actions)
        
            # 设置列宽分层约束 (优先级：关键信息完全显示 > 备注拉伸)
            header = self.table.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.ResizeToContents) # 客户名称
            header.setSectionResizeMode(1, QHeaderView.ResizeToContents) # 项目名称
            header.setSectionResizeMode(2, QHeaderView.ResizeToContents) # 关联单号
            header.setSectionResizeMode(3, QHeaderView.ResizeToContents) # 报价日期
            header.setSectionResizeMode(4, QHeaderView.ResizeToContents) # 版本
            header.setSectionResizeMode(5, QHeaderView.Fixed); self.table.setColumnWidth(5, 120)  # 金额
            header.setSectionResizeMode(6, QHeaderView.Stretch) # 备注弹性拉伸
            header.setSectionResizeMode(7, QHeaderView.ResizeToContents) # 附件状态
            header.setSectionResizeMode(8, QHeaderView.Fixed); self.table.setColumnWidth(8, 180)  # 操作列
        except Exception as e:
            traceback.print_exc()
            InfoBar.error("数据加载失败", str(e), duration=0, parent=self)

    def on_item_double_clicked(self, item):
        """双击行任意位置触发修改"""
        # 从该行首个单元格获取存储的字典数据
        first_item = self.table.item(item.row(), 0)
        if first_item:
            edit_data = first_item.data(Qt.UserRole)
            if edit_data:
                self.add_quote(edit_data=edit_data)

    def open_quote_file(self, p):
        abs_p = os.path.join(get_app_dir(), p) if not os.path.isabs(p) else p
        if os.path.exists(abs_p):
            QDesktopServices.openUrl(QUrl.fromLocalFile(os.path.abspath(abs_p)))
        else:
            InfoBar.error("文件缺失", f"未找到文件: {os.path.basename(abs_p)}", parent=self.window())
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

    def delete_quote(self, p_no, ver):
        """物理移除报价记录"""
        res = MessageBox("确定删除?", f"即将永久移除项目 [{p_no}] 的版本 [{ver}] 的报价记录。确认操作?", self.window())
        if res.exec():
            with get_db_conn(timeout=10) as conn:
                conn.execute("DELETE FROM quotations WHERE project_no=? AND version=?", (p_no, ver))
                conn.commit()
            # 移出 with 块后执行日志和刷新
            log_action("报价管理", "删除报价", p_no, f"项目: {p_no}, 版本: {ver}")
            self.load_quotes()
            InfoBar.success("删除成功", "报价记录已移除", duration=2000)

    def convert_to_contract(self, p_no, amount, quote_file):
        """将报价单流转为合同"""
        msg = f"确认将项目 [{p_no}] 的报价 (¥{amount:,.2f}) 转为合同吗？\n系统将自动带入金额及附件。"
        res = MessageBox("业务流转确认", msg, self.window())
        if res.exec():
            # 获取 MainWindow 实例
            main_window = self.window()
            # qfluentwidgets 的 FluentWindow 切换页面通常使用接口实例或 objectName
            try:
                # 记录一下报价附件，后续 add_contract 可以参考
                if quote_file:
                    self.temp_quote_file = quote_file
                
                # 切换到合同页面
                main_window.navigationInterface.setCurrentItem(main_window.contract_page.objectName())
                # 记录日志
                log_action("报价管理", "转合同流转", p_no, f"金额: {amount}, 源报价: {os.path.basename(quote_file) if quote_file else 'N/A'}")
                # 触发新增对话框并预填
                main_window.contract_page.add_contract(p_no, amount, quote_file)
            except Exception as e:
                InfoBar.error("流转失败", f"无法自动切换页面: {str(e)}", parent=self)

    def add_quote(self, edit_data=None, copy_data=None):
        dlg = QDialog(self)
        if edit_data: dlg.setWindowTitle(f"修改报价单: {edit_data['p_no']} ({edit_data['ver']})")
        elif copy_data: dlg.setWindowTitle(f"快速复制报价: {copy_data['p_no']} (新版本)")
        else: dlg.setWindowTitle("新建商务报价方案")
        
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        
        proj_cb = ComboBox(); proj_nos = []
        preselect_idx = -1
        # 复制或编辑模式下，需要定位关联项目
        target_p_no = edit_data['p_no'] if edit_data else (copy_data['p_no'] if copy_data else None)
        
        with get_db_conn() as conn:
            for i, r in enumerate(conn.execute("SELECT project_no, project_name FROM projects")):
                proj_nos.append(r[0]); proj_cb.addItem(f"[{r[0]}] {r[1]}")
                if target_p_no and r[0] == target_p_no:
                    preselect_idx = i
        
        if not proj_nos:
            InfoBar.warning("提示", "请先在【项目跟进】中立项", parent=self.window())
            return

        date_e = CalendarPicker(); date_e.setDate(QDate.currentDate())
        ver = LineEdit(); ver.setPlaceholderText("例如: V1")
        amt = LineEdit(); amt.setPlaceholderText("0.00")
        
        # 将报价备注升级为 TextEdit 以支持多行详细录入
        remark = TextEdit()
        remark.setPlaceholderText("填写报价核心项、特殊赠品或商务条款说明...")
        remark.setFixedHeight(100) # 设定合适的高度
        self.file_path = edit_data['path'] if edit_data else ""
        
        if edit_data:
            proj_cb.setCurrentIndex(preselect_idx); proj_cb.setEnabled(False) 
            date_e.setDate(QDate.fromString(edit_data['date'], "yyyy-MM-dd"))
            ver.setText(edit_data['ver']); ver.setEnabled(False) 
            amt.setText(str(edit_data['amt']))
            remark.setPlainText(edit_data['remark'])
        elif copy_data:
            proj_cb.setCurrentIndex(preselect_idx) # 预选项目但不锁定
            amt.setText(str(copy_data['amt']))
            remark.setPlainText(copy_data['remark'])
            # 自动生成后续版本号
            def refresh_ver():
                p_no_val = proj_nos[proj_cb.currentIndex()]
                with get_db_conn(timeout=10) as conn:
                    count = conn.execute("SELECT COUNT(*) FROM quotations WHERE project_no=?", (p_no_val,)).fetchone()[0]
                    ver.setText(f"V{count + 1}")
            proj_cb.currentIndexChanged.connect(refresh_ver)
            refresh_ver()
        else:
            # 自动版本号逻辑 (纯新增)
            def auto_set_version():
                p_no_val = proj_nos[proj_cb.currentIndex()]
                with get_db_conn(timeout=10) as conn:
                    count = conn.execute("SELECT COUNT(*) FROM quotations WHERE project_no=?", (p_no_val,)).fetchone()[0]
                    ver.setText(f"V{count + 1}")
            proj_cb.currentIndexChanged.connect(auto_set_version)
            auto_set_version()

        # --- 方案附件管理优化 (同步合同逻辑) ---
        file_container = QWidget()
        file_layout = QHBoxLayout(file_container)
        file_layout.setContentsMargins(0, 0, 0, 0)
        
        file_btn = PrimaryPushButton(FIF.DOCUMENT, "重新上传附件" if edit_data else "选择报价单附件")
        del_file_btn = TransparentToolButton(FIF.DELETE, file_container)
        del_file_btn.setToolTip("移除当前方案附件")
        del_file_btn.setVisible(bool(self.file_path))
        
        def update_quote_file_ui():
            if self.file_path:
                file_btn.setText(f"已关联: {os.path.basename(self.file_path)}")
                del_file_btn.show()
            else:
                file_btn.setText("选择方案附件 (PDF/Excel)")
                del_file_btn.hide()

        def on_select_quote_file():
            path, _ = QFileDialog.getOpenFileName(dlg, "选择报价单附件", "", "Documents (*.pdf *.xlsx *.xls *.doc *.docx);;All Files (*)")
            if path:
                self.file_path = path
                update_quote_file_ui()

        def on_remove_quote_file():
            if MessageBox("确认移除", "确定要删除此方案的附件关联吗？", dlg).exec():
                self.file_path = ""
                update_quote_file_ui()

        file_btn.clicked.connect(on_select_quote_file)
        del_file_btn.clicked.connect(on_remove_quote_file)
        file_layout.addWidget(file_btn, 1)
        file_layout.addWidget(del_file_btn)
        
        update_quote_file_ui() # 初始化
        
        form.addRow("关联项目*:", proj_cb)
        form.addRow("报价日期:", date_e)
        form.addRow("版本号*:", ver)
        form.addRow("报价金额 (¥)*:", amt)
        form.addRow("报价备注:", remark)
        form.addRow("方案附件:", file_container)
        layout.addLayout(form)
        
        save_btn = PrimaryPushButton("发布方案" if not edit_data else "保存变更")
        save_btn.clicked.connect(dlg.accept)
        layout.addWidget(save_btn)
        
        if dlg.exec():
            # 冲突检测 (新增时)
            p_no_val = proj_nos[proj_cb.currentIndex()]
            v_val = ver.text().strip()
            if not edit_data:
                with get_db_conn() as conn:
                    exists = conn.execute("SELECT 1 FROM quotations WHERE project_no=? AND version=?", (p_no_val, v_val)).fetchone()
                    if exists:
                        InfoBar.warning("版本冲突", f"项目 [{p_no_val}] 已存有版本 [{v_val}]，请微调版本号", parent=self)
                        return

            val_amt = 0.0
            try: val_amt = float(amt.text().replace(",", ""))
            except: pass
            
            # 附件物理拷贝
            final_path = self.file_path
            # 如果是外部选取的路径 (非 attachments 开头)
            if self.file_path and not self.file_path.startswith("attachments/"):
                try:
                    dest_dir = get_attachment_dir("quotations")
                    fname = os.path.basename(self.file_path)
                    ts = datetime.now().strftime("%Y%m%d%H%M%S")
                    save_name = f"{p_no_val}_{v_val}_{ts}_{fname}"
                    abs_dest = os.path.join(dest_dir, save_name)
                    shutil.copy2(self.file_path, abs_dest)
                    final_path = f"attachments/quotations/{save_name}"
                except Exception as e:
                    print(f"File copy error: {e}")

            # [重构]：将数据库写入与审计日志分离，防止嵌套连接引发的 disk I/O error
            with get_db_conn(timeout=15) as conn:
                if edit_data:
                    conn.execute("UPDATE quotations SET quote_date=?, amount=?, remark=?, file_path=? WHERE project_no=? AND version=?",
                                (date_e.date.toString("yyyy-MM-dd"), val_amt, remark.toPlainText(), final_path, p_no_val, v_val))
                    action = "修改报价"
                else:
                    conn.execute("INSERT INTO quotations (project_no, quote_date, amount, version, file_path, remark) VALUES (?,?,?,?,?,?)",
                                (p_no_val, date_e.date.toString("yyyy-MM-dd"), val_amt, v_val, final_path, remark.toPlainText()))
                    action = "发布报价"
            
            # 移出 with 块后执行日志和刷新
            log_action("报价管理", action, p_no_val, f"版本: {v_val}, 金额: {val_amt}")
            self.load_quotes()
            log_action("报价管理", "更新数据列表", p_no_val, "刷新列表")
            InfoBar.success("已存档", "商务报价方案更新成功", duration=2000)

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
        # 绑定系统级刷新信号
        SIGNAL_BUS.projectChanged.connect(self.load_contracts)
        
        btn_bar = QHBoxLayout()
        btn_bar.addWidget(TitleLabel("合同签署与回款监控"))
        btn_bar.addStretch()
        self.add_btn = PrimaryPushButton(FIF.ADD, "录入成交合同")
        btn_bar.addWidget(self.add_btn)
        layout.addLayout(btn_bar)
        
        self.table = TableWidget()
        self.table.setColumnCount(10) # 增加一列 [客户名称]
        self.table.setHorizontalHeaderLabels(["客户名称", "项目编号", "合同周期", "总金额", "已收", "进度", "待收", "状态", "附件", "操作"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setEditTriggers(TableWidget.NoEditTriggers)
        self.table.verticalHeader().setDefaultSectionSize(45) # 增加行高确保图标不挤压
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        layout.addWidget(self.table)
        
        self.add_btn.clicked.connect(self.add_contract)
        # 绑定双击穿透逻辑
        self.table.itemDoubleClicked.connect(self.on_row_double_clicked)
        self.load_contracts()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_contracts()

    def load_contracts(self):
        try:
            # [原子化渲染 第一步]：仅读取数据
            rows = []
            with get_db_conn(timeout=10) as conn:
                # 升级 SQL：增加客户名称关联，并实时通过子查询计算已收金额
                query = """
                    SELECT c.project_no, cust.name as customer_name,
                           COALESCE(c.start_date, ''), COALESCE(c.end_date, ''), 
                           COALESCE(c.total_amount, 0.0), 
                           (SELECT COALESCE(SUM(actual_amount), 0.0) FROM payment_plans WHERE project_no=c.project_no AND status='已收'), 
                           COALESCE(c.file_path, '') 
                    FROM contracts c
                    JOIN projects p ON c.project_no = p.project_no
                    JOIN customers cust ON p.customer_id = cust.id
                """
                rows = conn.execute(query).fetchall()
            
            # [原子化渲染 第二步]：查询成功后才清空并重建表格
            self.table.setRowCount(0)
            for r in rows:
                idx = self.table.rowCount()
                self.table.insertRow(idx)
                
                p_no, cust_name, s_date, e_date, total, paid, f_path = r
                unpaid = total - paid
                cycle = f"{s_date} 至 {e_date}"
                status = "已结清" if unpaid <= 0 else "履行中"
                # 进度条
                prog = ProgressBar()
                percent = int((paid / total * 100)) if total > 0 else 0
                prog.setValue(min(percent, 100))
                prog.setFixedWidth(100)
                
                # 组装数据预览 (索引 0:客户名称, 1:项目编号)
                row_data = [cust_name, p_no, cycle, f"¥{total:,.2f}", f"¥{paid:,.2f}", "", f"¥{unpaid:,.2f}", status, "", ""]
                
                # [第一步] 先填充所有单元格 of Item
                for i, v in enumerate(row_data):
                    item = QTableWidgetItem(v)
                    item.setData(Qt.UserRole, p_no) # 确保双击穿透可用
                    
                    # 财务视觉增强：待收金额加粗且设为深橙色
                    if i == 6:
                        font = item.font()
                        font.setBold(True)
                        item.setFont(font)
                        item.setForeground(QColor("#D35400"))
                        
                    self.table.setItem(idx, i, item) # 必须先 setItem
                
                # [第二步] 之后再进行 Widget 注入
                self.table.setCellWidget(idx, 5, prog) # 进度条
                if f_path:
                    self.render_attachment_icon(idx, 8, f_path) # 传递行号和路径
                self.add_contract_actions(idx, 9, p_no) # 传递行号和 ID

                # [第三步] 强制固定操作列宽度与拉伸保护
                self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive) # 允许交互
                self.table.setColumnWidth(1, 100)  # 项目编号列固定 100 像素
                self.table.setColumnWidth(8, 60)   # 附件列固定 60 像素
                self.table.setColumnWidth(9, 150)  # 操作列固定 150 像素
                self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch) # 让客户名称列执行 Stretch 拉伸
                self.table.verticalHeader().setDefaultSectionSize(50) # 强制设置固定行高，防止 UI 挤压
        except Exception as e:
            traceback.print_exc()
            InfoBar.error("加载失败", f"合同数据载入异常: {str(e)}", duration=0, parent=self.window())

    def open_attachment(self, p):
        abs_p = os.path.join(get_app_dir(), p) if not os.path.isabs(p) else p
        if os.path.exists(abs_p):
            QDesktopServices.openUrl(QUrl.fromLocalFile(os.path.abspath(abs_p)))
        else:
            InfoBar.error("文件缺失", f"无法找到附件: {os.path.basename(abs_p)}", duration=3000, parent=self.window())

    def add_contract_actions(self, row, col, p_no):
        """注入快捷操作按钮组 (标准图标逻辑)"""
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(5, 5, 5, 5); layout.setSpacing(10)
        
        # 1. 💰 登记回款
        pay_btn = TransparentToolButton(FIF.SEARCH, container)
        pay_btn.setToolTip("回款登记")
        pay_btn.setFixedSize(32, 32)
        pay_btn.clicked.connect(lambda: self.register_payment(p_no))
        
        # 2. ✏️ 修改合同
        edit_btn = TransparentToolButton(FIF.EDIT, container)
        edit_btn.setToolTip("编辑合同")
        edit_btn.setFixedSize(32, 32)
        edit_btn.clicked.connect(lambda: self.add_contract(project_no=p_no, is_edit=True))
        
        # 3. 🗑️ 删除合同
        del_btn = TransparentToolButton(FIF.DELETE, container)
        del_btn.setToolTip("物理删除")
        del_btn.setFixedSize(32, 32)
        del_btn.clicked.connect(lambda: self.delete_contract(p_no))
        
        layout.addWidget(pay_btn)
        layout.addWidget(edit_btn)
        layout.addWidget(del_btn)
        layout.addStretch()
        
        self.table.setCellWidget(row, col, container)

    def render_attachment_icon(self, row, col, f_path):
        """渲染附件图标 (标准图标逻辑)"""
        btn = TransparentToolButton(FIF.DOCUMENT, self)
        btn.setToolTip("查看合同附件")
        btn.setFixedSize(32, 32)
        btn.clicked.connect(lambda: self.open_attachment(f_path))
        self.table.setCellWidget(row, col, btn)

    def delete_contract(self, p_no):
        """物理删除合同记录"""
        res = MessageBox("确定删除合同?", f"即将删除项目 [{p_no}] 的合同记录。注意：这不会删除项目本身，但会清空此合同的所有回款概况。确认继续？", self.window())
        if res.exec():
            try:
                with get_db_conn(timeout=15) as conn:
                    conn.execute("DELETE FROM contracts WHERE project_no=?", (p_no,))
                    conn.commit()
                # 移出事务块后记录日志和刷新
                log_action("生效合同", "解除合同", p_no, "物理删除合同记录及关联概况")
                self.load_contracts()
                InfoBar.success("已移除", f"合同 [{p_no}] 已成功从系统中注销", duration=3000, parent=self.window())
            except Exception as e:
                InfoBar.error("删除失败", str(e), parent=self.window())

    def on_row_double_clicked(self, item):
        """双击全行穿透至项目详情全景图"""
        p_no = item.data(Qt.UserRole)
        if p_no:
            dlg = ProjectDetailDialog(p_no, self)
            dlg.exec()
            self.load_contracts()

    def register_payment(self, p_no):
        """跳转至项目全景窗并自动聚焦财务标签页"""
        dlg = ProjectDetailDialog(p_no, self, start_tab="finance")
        dlg.exec()
        self.load_contracts()

    def add_contract(self, project_no=None, amount=None, quote_file=None, is_edit=False):
        """
        合同录入/修改统一入口
        is_edit=True 时执行更新逻辑
        """
        dlg = QDialog(self)
        dlg.setWindowTitle("商务成交 - 合同签约登记" if not is_edit else f"合同维护 - {project_no}")
        layout = QVBoxLayout(dlg)
        layout.setSpacing(15)
        form = QFormLayout()
        
        proj_cb = ComboBox(); proj_nos = []
        pre_fill_idx = -1
        # [MOD] 获取所有项目供选择（如果是编辑模式，则仅显示该项目且锁定）
        sql_proj = "SELECT project_no, project_name FROM projects"
        if is_edit:
            sql_proj += f" WHERE project_no='{project_no}'"

        with get_db_conn() as conn:
            for i, r in enumerate(conn.execute(sql_proj)):
                proj_nos.append(r[0])
                proj_cb.addItem(f"[{r[0]}] {r[1]}")
                if project_no and r[0] == project_no:
                    pre_fill_idx = i
        
        if not proj_nos: 
            InfoBar.warning("提示", "未找到可关联的项目记录", parent=self.window())
            return

        if pre_fill_idx != -1:
            proj_cb.setCurrentIndex(pre_fill_idx)
        
        if is_edit: proj_cb.setEnabled(False) # 编辑模式锁定项目

        start = CalendarPicker(); start.setDate(QDate.currentDate())
        end = CalendarPicker(); end.setDate(QDate.currentDate().addDays(365))
        total = LineEdit(); total.setPlaceholderText("合同总价值")
        paid = LineEdit(); paid.setPlaceholderText("首付或已收金额 (仅在录入时生效)")
        if is_edit: paid.setDisabled(True) # 编辑模式下，首付通过回款模块维护
        
        memo = TextEdit(); memo.setPlaceholderText("核心条款预览...")
        self.c_file = quote_file or ""
        
        # [NEW] 编辑模式加载旧数据
        if is_edit:
            with get_db_conn() as conn:
                r = conn.execute("SELECT start_date, end_date, total_amount, file_path, contract_memo FROM contracts WHERE project_no=?", (project_no,)).fetchone()
                if r:
                    start.setDate(QDate.fromString(r[0], "yyyy-MM-dd"))
                    end.setDate(QDate.fromString(r[1], "yyyy-MM-dd"))
                    total.setText(str(r[2]))
                    self.c_file = r[3] or ""
                    memo.setPlainText(r[4] or "")

        # --- 附件管理行优化 ---
        file_container = QWidget()
        file_layout = QHBoxLayout(file_container)
        file_layout.setContentsMargins(0, 0, 0, 0)
        
        f_btn = PrimaryPushButton("上传/更换扫描件")
        del_f_btn = TransparentToolButton(FIF.DELETE, file_container)
        del_f_btn.setToolTip("移除当前附件记录")
        del_f_btn.setVisible(bool(self.c_file)) # 仅在有附件时显示
        
        def update_file_ui():
            if self.c_file:
                f_btn.setText(f"当前附件: {os.path.basename(self.c_file)}")
                del_f_btn.show()
            else:
                f_btn.setText("选择附件归档 (PDF/图片)")
                del_f_btn.hide()

        def on_select_file():
            path, _ = QFileDialog.getOpenFileName(dlg, "选择合同扫描件", "", "Documents (*.pdf *.jpg *.png *.zip);;All Files (*)")
            if path:
                self.c_file = path
                update_file_ui()
                
        def on_remove_file():
            if MessageBox("确认移除", "确定要解绑当前合同附件吗？", dlg).exec():
                self.c_file = ""
                update_file_ui()

        f_btn.clicked.connect(on_select_file)
        del_f_btn.clicked.connect(on_remove_file)
        file_layout.addWidget(f_btn, 1)
        file_layout.addWidget(del_f_btn)
        
        update_file_ui() # 初始化 UI 状态
        
        form.addRow("关联项目*:", proj_cb)
        form.addRow("生效日期:", start); form.addRow("截止日期:", end)
        form.addRow("合同总额*:", total)
        form.addRow("已付/首付:", paid)
        form.addRow("扫描件归档:", file_container)
        form.addRow("核心备注:", memo)
        layout.addLayout(form)
        
        btn = PrimaryPushButton("确 认 保 存" if is_edit else "确认签约并归档")
        btn.clicked.connect(dlg.accept)
        layout.addWidget(btn)
        
        if dlg.exec():
            p_no = proj_nos[proj_cb.currentIndex()]
            final_path = ""
            if self.c_file:
                if os.path.isabs(self.c_file):
                    try:
                        dest_dir = get_attachment_dir("contracts")
                        fname = os.path.basename(self.c_file)
                        ts = datetime.now().strftime("%Y%m%d%H%M%S")
                        save_name = f"{p_no}_{ts}_{fname}"
                        abs_dest = os.path.join(dest_dir, save_name)
                        shutil.copy2(self.c_file, abs_dest)
                        final_path = f"attachments/contracts/{save_name}"
                    except: final_path = self.c_file
                else: final_path = self.c_file

            try:
                with get_db_conn() as conn:
                    val_total = float(total.text() or 0)
                    dt_s = start.date.toString("yyyy-MM-dd")
                    dt_e = end.date.toString("yyyy-MM-dd")
                    
                    if is_edit:
                        conn.execute("UPDATE contracts SET start_date=?, end_date=?, total_amount=?, file_path=?, contract_memo=? WHERE project_no=?",
                                    (dt_s, dt_e, val_total, final_path, memo.toPlainText(), p_no))
                    else:
                        val_paid = float(paid.text() or 0)
                        conn.execute("INSERT INTO contracts (project_no, start_date, end_date, total_amount, paid_amount, file_path, contract_memo) VALUES (?,?,?,?,?,?,?)",
                                    (p_no, dt_s, dt_e, val_total, val_paid, final_path, memo.toPlainText()))
                        if val_paid > 0:
                            conn.execute("INSERT INTO payment_plans (project_no, plan_date, plan_amount, actual_amount, status, remark) VALUES (?,?,?,?,'已收','合同首付款登记')", (p_no, dt_s, val_paid, val_paid))
                    
                    # 原子化日志记录
                    log_action("生效合同", "修改合同" if is_edit else "签约登记", p_no, f"总额: {val_total}", conn=conn)
                    conn.commit()
                self.load_contracts()
                InfoBar.success("操作成功", "合同档案已同步更新" if is_edit else f"项目 {p_no} 签约资料已入库", duration=3000, parent=self.window())
            except Exception as e:
                InfoBar.error("操作失败", f"数据库写入错误: {str(e)}", duration=3000, parent=self.window())

    def select_c_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "选择文件", "", "All Files (*)")
        if path: self.c_file = path

# ==========================================
# 自动化热更新补丁：确保字段补全
# ==========================================
def ensure_columns():
    """数据库“查体医生”：确保所有必备字段存在，防止热更新导致的崩溃"""
    with get_db_conn(timeout=10) as conn:
        cursor = conn.cursor()
        
        # 辅助函数：安全添加字段
        def add_col(table, col, col_type):
            try:
                cursor.execute(f"PRAGMA table_info({table})")
                cols = [c[1] for c in cursor.fetchall()]
                if col not in cols:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                    print(f"DEBUG: 补齐字段 {table}.{col}")
            except Exception as e:
                print(f"DEBUG: 补修数据库失败 {table}.{col}: {e}")

        # 1. 补齐报价单表
        add_col("quotations", "amount", "REAL DEFAULT 0.0")
        add_col("quotations", "file_path", "TEXT")
        add_col("quotations", "remark", "TEXT")
        
        # 2. 补齐合同表
        add_col("contracts", "paid_amount", "REAL DEFAULT 0.0")
        add_col("contracts", "contract_memo", "TEXT")

        # 3. 补齐项目表 (重点)
        add_col("projects", "stage", "TEXT DEFAULT '初期线索'")
        add_col("projects", "loss_reason", "TEXT")
        add_col("projects", "next_plan", "TEXT")

        # 4. 补齐联系人表
        add_col("contacts", "role_type", "TEXT DEFAULT '经办人'")

        # 5. 补齐跟进记录表
        add_col("follow_ups", "contact_method", "TEXT DEFAULT '电话'")
        add_col("follow_ups", "follow_duration", "INTEGER DEFAULT 0")

        # 确保 payment_plans 表存在
        cursor.execute("CREATE TABLE IF NOT EXISTS payment_plans (id INTEGER PRIMARY KEY AUTOINCREMENT, project_no TEXT, plan_date TEXT, plan_amount REAL, actual_amount REAL DEFAULT 0.0, status TEXT DEFAULT '待收', remark TEXT, FOREIGN KEY (project_no) REFERENCES projects(project_no))")
        
        # 6. 历史数据迁移与同步逻辑 (从 init_db 迁入)
        try:
            cursor.execute("SELECT project_no, paid_amount, start_date FROM contracts WHERE paid_amount > 0")
            for p_no, amt, s_date in cursor.fetchall():
                c = cursor.execute("SELECT COUNT(*) FROM payment_plans WHERE project_no=? AND remark='历史首付款迁移'", (p_no,)).fetchone()[0]
                if c == 0:
                    dt = s_date or date.today().strftime("%Y-%m-%d")
                    cursor.execute("INSERT INTO payment_plans (project_no, plan_date, plan_amount, actual_amount, status, remark) VALUES (?, ?, ?, ?, '已收', '历史首付款迁移')", (p_no, dt, amt, amt))
        except: pass
        
        conn.commit()

# ==========================================
# 日志中心页面 (Audit Center)
# ==========================================
class LogPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("LogPage")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        
        header = QHBoxLayout()
        header.addWidget(TitleLabel("系统审计日志"))
        header.addStretch()
        
        self.search_box = SearchLineEdit()
        self.search_box.setPlaceholderText("搜模块、动作或详情...")
        self.search_box.setFixedWidth(300)
        self.search_box.textChanged.connect(self.load_logs)
        
        self.export_btn = TransparentToolButton(FIF.DOWNLOAD, self)
        self.export_btn.setToolTip("导出日志到 CSV (Excel 兼容)")
        self.export_btn.clicked.connect(self.export_logs)
        
        header.addWidget(self.search_box)
        header.addWidget(self.export_btn)
        layout.addLayout(header)
        
        self.table = TableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["执行时间", "所属模块", "操作动作", "关联目标", "操作详情摘要"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)
        
        self.load_logs()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_logs()

    def load_logs(self):
        try:
            st = self.search_box.text().strip()
            self.table.setRowCount(0)
            with get_db_conn() as conn:
                query = """
                    SELECT timestamp, module, action_type, target_id, details 
                    FROM action_logs 
                    WHERE (module LIKE ? OR action_type LIKE ? OR details LIKE ? OR target_id LIKE ?)
                    ORDER BY timestamp DESC LIMIT 500
                """
                cur = conn.execute(query, (f"%{st}%", f"%{st}%", f"%{st}%", f"%{st}%"))
                for r in cur:
                    idx = self.table.rowCount(); self.table.insertRow(idx)
                    # 时间格式化：保持单行显示以确保具体时间可见
                    self.table.setItem(idx, 0, QTableWidgetItem(str(r[0])))
                    self.table.setItem(idx, 1, QTableWidgetItem(str(r[1])))
                    
                    # 动作列颜色映射
                    act = str(r[2])
                    it_act = QTableWidgetItem(act)
                    if "删除" in act or "移除" in act or "解除" in act or "流失" in act:
                        it_act.setForeground(QColor("#c0392b")) # 红色
                    elif "新增" in act or "签约" in act or "发布" in act:
                        it_act.setForeground(QColor("#27ae60")) # 绿色
                    elif "修改" in act or "修正" in act:
                        it_act.setForeground(QColor("#2980b9")) # 蓝色
                    
                    font = it_act.font(); font.setBold(True); it_act.setFont(font)
                    self.table.setItem(idx, 2, it_act)
                    self.table.setItem(idx, 3, QTableWidgetItem(str(r[3])))
                    
                    it_det = QTableWidgetItem(str(r[4]))
                    it_det.setToolTip(str(r[4]))
                    self.table.setItem(idx, 4, it_det)
            
            # 列宽初始设定：加宽时间列以完整显示次分秒
            self.table.setColumnWidth(0, 200)
            self.table.setColumnWidth(1, 100)
            self.table.setColumnWidth(2, 100)
            self.table.setColumnWidth(3, 120)
        except Exception as e:
            print(f"Load Logs Error: {e}")

    def export_logs(self):
        """导出当前视图日志为 CSV (Excel 友好)"""
        import csv
        path, _ = QFileDialog.getSaveFileName(self, "保存日志导出", f"CRM_Audit_Log_{datetime.now().strftime('%Y%m%d')}.csv", "CSV Files (*.csv)")
        if not path: return
        
        try:
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(["时间", "模块", "动作", "目标", "详情"])
                for r in range(self.table.rowCount()):
                    row_data = [self.table.item(r, c).text().replace("\n", " ") for c in range(5)]
                    writer.writerow(row_data)
            InfoBar.success("导出成功", f"日志已保存至: {os.path.basename(path)}", duration=3000, parent=self)
        except Exception as e:
            InfoBar.error("导出失败", str(e), parent=self)

# ==========================================
# 备份管理核心类 (BackupManager)
# ==========================================
class BackupManager:
    """负责数据库物理快照、版本剪枝与安全恢复"""
    BACKUP_DIR = os.path.join(get_app_dir(), "backups")
    
    @staticmethod
    def ensure_dir():
        if not os.path.exists(BackupManager.BACKUP_DIR):
            os.makedirs(BackupManager.BACKUP_DIR)

    @staticmethod
    def get_project_count():
        """统计项目数，用于备份文件名生成"""
        try:
            with get_db_conn() as conn:
                res = conn.execute("SELECT COUNT(*) FROM projects").fetchone()
                return res[0] if res else 0
        except: return 0

    @staticmethod
    def create_backup(note="用户手动快照", is_auto=False):
        """执行物理对拷备份"""
        try:
            BackupManager.ensure_dir()
            p_count = BackupManager.get_project_count()
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            file_name = f"CRM_Bkp_{p_count}Prj_{ts}.db"
            dest_path = os.path.join(BackupManager.BACKUP_DIR, file_name)
            
            # 执行物理拷贝
            shutil.copy2(DB_NAME, dest_path)
            
            # 获取文件大小 (MB)
            size_mb = os.path.getsize(dest_path) / (1024 * 1024)
            
            # 记录审计日志
            log_action("系统管理", "数据备份" if not is_auto else "自动存档", file_name, f"备注: {note} | 大小: {size_mb:.2f}MB")
            return True, file_name
        except Exception as e:
            return False, str(e)

    @staticmethod
    def prune_backups(keep_count=1):
        """过期清理：只保留最近的 N 个备份 (按时间排序)"""
        try:
            BackupManager.ensure_dir()
            files = [f for f in os.listdir(BackupManager.BACKUP_DIR) if f.startswith("CRM_Bkp_") and f.endswith(".db")]
            if len(files) <= keep_count: return
            
            # 按修改时间排序
            files.sort(key=lambda x: os.path.getmtime(os.path.join(BackupManager.BACKUP_DIR, x)), reverse=True)
            
            # 删除多余文件
            for f in files[keep_count:]:
                os.remove(os.path.join(BackupManager.BACKUP_DIR, f))
        except: pass

    @staticmethod
    def perform_restore(backup_name, parent_window):
        """执行“时空穿梭”恢复逻辑"""
        try:
            backup_path = os.path.join(BackupManager.BACKUP_DIR, backup_name)
            if not os.path.exists(backup_path): return False, "备份文件不存在"
            
            # 1. 强制冷备份：在覆盖前保存当前现场
            emergency_path = os.path.join(get_app_dir(), "EMERGENCY_BEFORE_RESTORE.db")
            shutil.copy2(DB_NAME, emergency_path)
            
            # 2. 物理替换 (SQLite 在连接未活跃时 copy2 覆盖是安全的)
            shutil.copy2(backup_path, DB_NAME)
            
            # 3. 自动架构对齐 (热更新查体)
            ensure_columns()
            
            log_action("系统管理", "数据恢复", backup_name, "执行了版本回溯，当前环境已更新")
            return True, "数据库已成功恢复，架构已自动平衡"
        except Exception as e:
            return False, str(e)

# ==========================================
# 数据管理页面 (BackupPage)
# ==========================================
class BackupPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("BackupPage")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        
        header = QHBoxLayout()
        header.addWidget(TitleLabel("数据维护与快照管理"))
        header.addStretch()
        
        self.snap_btn = PrimaryPushButton(FIF.CAMERA, "立即创建快照")
        self.folder_btn = TransparentToolButton(FIF.FOLDER, self)
        self.folder_btn.setToolTip("打开备份文件夹")
        
        header.addWidget(self.snap_btn)
        header.addWidget(self.folder_btn)
        layout.addLayout(header)
        
        self.table = TableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["备份时间", "文件名称", "备注摘要", "文件大小", "管理操作"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)
        
        self.snap_btn.clicked.connect(self.on_snapshot_clicked)
        self.folder_btn.clicked.connect(self.on_open_folder)
        
        self.load_history()

    def showEvent(self, event):
        super().showEvent(event)
        self.load_history()

    def load_history(self):
        """加载备份历史"""
        self.table.setRowCount(0)
        BackupManager.ensure_dir()
        files = [f for f in os.listdir(BackupManager.BACKUP_DIR) if f.endswith(".db")]
        # 按修改时间倒序
        files.sort(key=lambda x: os.path.getmtime(os.path.join(BackupManager.BACKUP_DIR, x)), reverse=True)
        
        for f_name in files:
            path = os.path.join(BackupManager.BACKUP_DIR, f_name)
            mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
            size_mb = os.path.getsize(path) / (1024 * 1024)
            
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(mtime))
            self.table.setItem(row, 1, QTableWidgetItem(f_name))
            
            # 从审计日志中尝试获取备注
            note = self.get_note_from_logs(f_name)
            self.table.setItem(row, 2, QTableWidgetItem(note))
            self.table.setItem(row, 3, QTableWidgetItem(f"{size_mb:.2f} MB"))
            
            # 操作列
            self.add_table_actions(row, f_name)

    def get_note_from_logs(self, file_name):
        try:
            with get_db_conn() as conn:
                res = conn.execute("SELECT details FROM action_logs WHERE target_id=? AND action_type IN ('数据备份', '自动存档') LIMIT 1", (file_name,)).fetchone()
                return res[0] if res else "系统快照"
        except: return "未知"

    def add_table_actions(self, row, file_name):
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        
        restore_btn = TransparentToolButton(FIF.HISTORY, container)
        restore_btn.setToolTip("恢复此版本 (时空穿梭)")
        restore_btn.clicked.connect(lambda: self.on_restore_clicked(file_name))
        
        del_btn = TransparentToolButton(FIF.DELETE, container)
        del_btn.setToolTip("彻底删除")
        del_btn.clicked.connect(lambda: self.on_delete_clicked(file_name))
        
        layout.addWidget(restore_btn)
        layout.addWidget(del_btn)
        layout.addStretch()
        self.table.setCellWidget(row, 4, container)

    def on_snapshot_clicked(self):
        text, ok = QInputDialog.getText(self, '创建快照', '请输入备份备注 (如：调价前存档):')
        if ok and text:
            success, msg = BackupManager.create_backup(text)
            if success:
                InfoBar.success("备份成功", f"快照文件已入库: {msg}", duration=3000, parent=self)
                self.load_history()
            else:
                InfoBar.error("备份失败", msg, parent=self)

    def on_open_folder(self):
        os.startfile(BackupManager.BACKUP_DIR) if sys.platform == 'win32' else os.system(f'open "{BackupManager.BACKUP_DIR}"')

    def on_restore_clicked(self, file_name):
        msg = f"确定要恢复备份 [{file_name}] 吗？\n\n警告：当前所有数据将被覆盖！系统会自动创建紧急备用版本以防万一。"
        res = MessageBox("高风险操作确认", msg, self.window())
        if res.exec():
            success, msg = BackupManager.perform_restore(file_name, self.window())
            if success:
                MessageBox("恢复成功", "数据已成功切换至选定版本。程序将立即重启以加载新数据。", self.window()).exec()
                os.execl(sys.executable, sys.executable, *sys.argv) # 重启程序
            else:
                InfoBar.error("恢复失败", msg, parent=self)

    def on_delete_clicked(self, file_name):
        if MessageBox("确认删除", f"确定要彻底删除备份文件 [{file_name}] 吗？", self.window()).exec():
            try:
                os.remove(os.path.join(BackupManager.BACKUP_DIR, file_name))
                self.load_history()
                InfoBar.success("已删除", "备份文件已物理移除", parent=self)
            except Exception as e:
                InfoBar.error("删除失败", str(e), parent=self)

class MainWindow(FluentWindow):
    def __init__(self):
        super().__init__()
        # 核心：启动时刻最先进行数据库初始化与查体，确保后续 UI 渲染安全
        if not init_db():
            QMessageBox.critical(None, "系统错误", "数据库初始化失败，请检查程序目录权限或数据库文件是否被锁定。")
            sys.exit(1)
        
        self.setWindowTitle("CRM Enterprise - 单兵高管企业版")
        self.showMaximized() # 自动最大化适配屏幕
        setTheme(Theme.LIGHT)
        
        # 初始化页面
        self.dashboard_page = DashboardPage()
        self.master_page = MasterDataPage()
        self.project_page = ProjectPage()
        self.quotation_page = QuotationPage()
        self.contract_page = ContractPage()
        self.log_page = LogPage()
        self.backup_page = BackupPage()
        
        self.init_navigation()

    def init_navigation(self):
        self.addSubInterface(self.dashboard_page, FIF.HOME, "经营看板")
        self.addSubInterface(self.master_page, FIF.PEOPLE, "基础档案")
        self.addSubInterface(self.project_page, FIF.EDIT, "项目跟进")
        self.addSubInterface(self.quotation_page, FIF.DOCUMENT, "报价管理")
        self.addSubInterface(self.contract_page, FIF.ACCEPT, "生效合同")
        self.navigationInterface.addSeparator() # 分割线
        self.addSubInterface(self.log_page, FIF.HISTORY, "系统日志")
        self.addSubInterface(self.backup_page, FIF.SETTING, "数据管理")
        self.navigationInterface.setExpandWidth(200)

    def closeEvent(self, event):
        """软件退出时执行自动存档与清理"""
        try:
            today_ts = datetime.now().strftime("%Y%m%d")
            # 简单查重：通过日志判断今日是否已自动备份过
            with get_db_conn() as conn:
                res = conn.execute("SELECT id FROM action_logs WHERE action_type='自动存档' AND timestamp LIKE ?", (f"{datetime.now().strftime('%Y-%m-%d')}%",)).fetchone()
                if not res:
                    BackupManager.create_backup("软件退出自动存档", is_auto=True)
            
            # 清理陈旧备份 (策略：保留最近 1 个)
            BackupManager.prune_backups(keep_count=1)
        except: pass
        super().closeEvent(event)

    def create_new_project_for(self, customer_name):
        """一键跳转并在项目页开启该客户的立项对话框"""
        self.switchTo(self.project_page)
        # 给页面切换一点缓冲时间，然后触发弹窗
        QTimer.singleShot(300, lambda: self.project_page.show_add_project_with_customer(customer_name))

# ==========================================

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

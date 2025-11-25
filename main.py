import os
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import io
import base64
from collections import Counter, defaultdict
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
import tempfile
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = "8481320208:AAFTLeBjr8SWJkzo06lfzixTMMAop2IcbqY"
ADMIN_IDS = [1393492435]
TIMEZONE_OFFSET = 4

DARK_THEME = {
    'background': '#1a1a1a',
    'card_bg': '#2d2d2d',
    'text_primary': '#ffffff',
    'text_secondary': '#b0b0b0',
    'accent': '#4a76fd',
    'accent_light': '#6c8eff',
    'success': '#4CAF50',
    'warning': '#FF9800',
    'error': '#f44336',
    'border': '#404040'
}

BELL_SCHEDULE = {
    "понедельник": {
        "1": "8.40 – 10.15",
        "2": "10.25 – 12.00", 
        "3": "12.30 – 14.05",
        "4": "14.45 – 16.20",
        "5": "16.30 – 18.05",
        "6": "18.10 – 19.40",
        "7": "18.50 – 20.20"
    },
    "вторник": {
        "1": "8.00 – 9.35",
        "2": "9.45 – 11.20",
        "3": "12.00 – 13.35", 
        "4": "13.45 – 15.20",
        "5": "15.40 – 17.15",
        "6": "17.20 – 18.50"
    },
    "среда": {
        "1": "8.00 – 9.35",
        "2": "9.45 – 11.20",
        "3": "12.00 – 13.35",
        "4": "13.45 – 15.20", 
        "5": "15.40 – 17.15",
        "6": "17.20 – 18.50"
    },
    "четверг": {
        "1": "8.00 – 9.35",
        "2": "9.45 – 11.20",
        "3": "12.00 – 13.35",
        "4": "13.45 – 15.20",
        "5": "15.40 – 17.15", 
        "6": "17.20 – 18.50"
    },
    "пятница": {
        "1": "8.00 – 9.35",
        "2": "9.45 – 11.20",
        "3": "12.00 – 13.35",
        "4": "13.45 – 15.20",
        "5": "15.40 – 17.15",
        "6": "17.20 – 18.50"
    },
    "суббота": {
        "1": "8.00 – 9.25",
        "2": "9.35 – 11.00",
        "3": "11.30 – 12.55",
        "4": "13.05 – 14.30",
        "5": "14.40 – 16.00",
        "6": "16.10 – 17.30"
    }
}

class DataManager:
    def __init__(self):
        self.groups_file = "groups.json"
        self.schedule_file = "schedule.json"
        self.users_file = "users.json"
        self.stats_file = "statistics.json"
        self.tickets_file = "tickets.json"
        self.templates_file = "templates.json"
        self.classmates_file = "classmates.json"
        self.notifications_file = "notifications.json"
        self.settings_file = "settings.json"
        self.init_data()
    
    def init_data(self):
        default_schedule = {}
        default_templates = {
            "расписание": "Расписание можно посмотреть через главное меню бота",
            "звонки": "📅 РАСПИСАНИЕ ЗВОНКОВ\n\n📌 ПОНЕДЕЛЬНИК:\n• Разговоры о важном (1 смена): 8.00–8.30\n• 1 пара: 8.40–10.15\n• 2 пара: 10.25–12.00\n• Обед: 12.00–12.30\n• 3 пара: 12.30–14.05\n• Разговоры о важном (2 смена): 14.10–14.40\n• 4 пара: 14.45–16.20\n• 5 пара: 16.30–18.05\n• 6 пара: 18.10–19.40\n\n📌 ВТОРНИК-ПЯТНИЦА:\n• 1 пара: 8.00–9.35\n• 2 пара: 9.45–11.20\n• Обед: 11.20–12.00\n• 3 пара: 12.00–13.35\n• 4 пара: 13.45–15.20\n• Обед: 15.20–15.40\n• 5 пара: 15.40–17.15\n• 6 пара: 17.20–18.50\n\n📌 СУББОТА:\n• 1 пара: 8.00–9.25\n• 2 пара: 9.35–11.00\n• Обед: 11.00–11.30\n• 3 пара: 11.30–12.55\n• 4 пара: 13.05–14.30\n• 5 пара: 14.40–16.00\n• 6 пара: 16.10–17.30",
            "помощь": "По техническим вопросам обращайтесь через раздел 'Техподдержка' в главном меню",
            "администрация": "По вопросам расписания обращайтесь к заму директора по учебной работе"
        }
        default_classmates = {}
        default_notifications = {}
        default_settings = {
            "notification_time": "18:00",
            "enabled_groups": []
        }
        
        groups_list = [
            "ОПУ(24)-9-21", "ОПУ(24)-9-22", "ОПУ(24)-9-23",
            "АТ(24)-9-21", "АТ(24)-9-22", "С(24)-9-21", "С(24)-9-22",
            "САД(24)-9-21", "ИСИП(24)-9-21", "ОПУ(23)-9-31", "ОПУ(23)-9-32",
            "ОПУ(23)-9-33", "АТ(23)-9-31", "АТ(23)-9-32", "С(23)-9-31",
            "С(23)-9-32", "САД(23)-9-31", "ИСИП(23)-9-31", "АТ(22)-9-41",
            "АТ(22)-9-42", "С(22)-9-41", "С(22)-9-42", "САД(22)-9-41",
            "САД(22)-9-42", "ИСИП(22)-9-41"
        ]
        
        files_data = {
            self.groups_file: groups_list,
            self.schedule_file: default_schedule,
            self.users_file: {},
            self.stats_file: {
                "user_activity": {}, "group_usage": {}, "feature_usage": {},
                "errors": [], "attendance": {}, "popular_functions": {}
            },
            self.templates_file: default_templates,
            self.tickets_file: [],
            self.classmates_file: default_classmates,
            self.notifications_file: default_notifications,
            self.settings_file: default_settings
        }
        
        for file, data in files_data.items():
            if not os.path.exists(file):
                with open(file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
    
    def load_data(self, file):
        try:
            with open(file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {file}: {e}")
            return {}
    
    def save_data(self, file, data):
        try:
            with open(file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving {file}: {e}")
            return False
    
    def get_groups(self):
        return self.load_data(self.groups_file)
    
    def add_group(self, group_name):
        groups = self.get_groups()
        if group_name not in groups:
            groups.append(group_name)
            return self.save_data(self.groups_file, groups)
        return False
    
    def get_schedule(self, group, month=None, day=None):
        schedule = self.load_data(self.schedule_file)
        if not month:
            return schedule.get(group, {})
        if not day:
            return schedule.get(group, {}).get(month, {})
        return schedule.get(group, {}).get(month, {}).get(day, [])
    
    def update_schedule(self, group, month, day, schedule_data):
        schedule = self.load_data(self.schedule_file)
        if group not in schedule:
            schedule[group] = {}
        if month not in schedule[group]:
            schedule[group][month] = {}
        schedule[group][month][day] = schedule_data
        return self.save_data(self.schedule_file, schedule)
    
    def get_user_group(self, user_id):
        users = self.load_data(self.users_file)
        user_data = users.get(str(user_id))
        if isinstance(user_data, dict):
            return user_data.get('group')
        return user_data
    
    def set_user_group(self, user_id, group):
        users = self.load_data(self.users_file)
        user_key = str(user_id)
        if user_key not in users or isinstance(users[user_key], str):
            users[user_key] = {}
        users[user_key]['group'] = group
        return self.save_data(self.users_file, users)
    
    def get_user_profile(self, user_id):
        users = self.load_data(self.users_file)
        user_data = users.get(str(user_id), {})
        if isinstance(user_data, str):
            return {'group': user_data}
        return user_data
    
    def update_user_profile(self, user_id, profile_data):
        users = self.load_data(self.users_file)
        user_key = str(user_id)
        if user_key not in users or isinstance(users[user_key], str):
            users[user_key] = {'group': users.get(user_key, '')}
        users[user_key].update(profile_data)
        return self.save_data(self.users_file, users)
    
    def add_classmate(self, group, user_data):
        classmates = self.load_data(self.classmates_file)
        if group not in classmates:
            classmates[group] = []
        for i, classmate in enumerate(classmates[group]):
            if classmate.get('id') == user_data['id']:
                classmates[group][i] = user_data
                return self.save_data(self.classmates_file, classmates)
        classmates[group].append(user_data)
        return self.save_data(self.classmates_file, classmates)
    
    def get_classmates(self, group):
        classmates = self.load_data(self.classmates_file)
        return classmates.get(group, [])
    
    def get_notification_settings(self, user_id):
        notifications = self.load_data(self.notifications_file)
        return notifications.get(str(user_id), {"enabled": True})
    
    def set_notification_settings(self, user_id, settings):
        notifications = self.load_data(self.notifications_file)
        notifications[str(user_id)] = settings
        return self.save_data(self.notifications_file, notifications)
    
    def get_users_with_notifications(self, group=None):
        users = self.load_data(self.users_file)
        notifications = self.load_data(self.notifications_file)
        result = []
        for user_id, user_data in users.items():
            user_notifications = notifications.get(user_id, {"enabled": True})
            if user_notifications.get("enabled", True):
                user_group = user_data.get('group') if isinstance(user_data, dict) else user_data
                if not group or user_group == group:
                    result.append(int(user_id))
        return result
    
    def get_settings(self):
        return self.load_data(self.settings_file)
    
    def update_settings(self, settings):
        return self.save_data(self.settings_file, settings)
    
    def log_activity(self, user_id, action, group=None):
        stats = self.load_data(self.stats_file)
        user_id_str = str(user_id)
        today = datetime.now().strftime("%Y-%m-%d")
        if "user_activity" not in stats:
            stats["user_activity"] = {}
        if user_id_str not in stats["user_activity"]:
            stats["user_activity"][user_id_str] = {}
        if today not in stats["user_activity"][user_id_str]:
            stats["user_activity"][user_id_str][today] = []
        stats["user_activity"][user_id_str][today].append({
            "action": action, "timestamp": datetime.now().isoformat()
        })
        if "feature_usage" not in stats:
            stats["feature_usage"] = {}
        if action not in stats["feature_usage"]:
            stats["feature_usage"][action] = 0
        stats["feature_usage"][action] += 1
        if group:
            if "group_usage" not in stats:
                stats["group_usage"] = {}
            if group not in stats["group_usage"]:
                stats["group_usage"][group] = 0
            stats["group_usage"][group] += 1
        self.save_data(self.stats_file, stats)
    
    def log_error(self, error_msg, user_id=None):
        stats = self.load_data(self.stats_file)
        if "errors" not in stats:
            stats["errors"] = []
        stats["errors"].append({
            "error": error_msg, "user_id": user_id, "timestamp": datetime.now().isoformat()
        })
        self.save_data(self.stats_file, stats)
    
    def get_all_users(self):
        users = self.load_data(self.users_file)
        result = {}
        for user_id, user_data in users.items():
            if isinstance(user_data, dict):
                result[user_id] = user_data.get('group', '')
            else:
                result[user_id] = user_data
        return result
    
    def get_statistics(self):
        return self.load_data(self.stats_file)
    
    def get_templates(self):
        return self.load_data(self.templates_file)
    
    def update_template(self, name, content):
        templates = self.get_templates()
        templates[name] = content
        return self.save_data(self.templates_file, templates)
    
    def create_ticket(self, user_id, message):
        tickets = self.load_data(self.tickets_file)
        ticket_id = len(tickets) + 1
        ticket = {
            "id": ticket_id, "user_id": user_id, "message": message,
            "status": "open", "created_at": datetime.now().isoformat(), "replies": []
        }
        tickets.append(ticket)
        return self.save_data(self.tickets_file, tickets)
    
    def get_tickets(self, status=None):
        tickets = self.load_data(self.tickets_file)
        if status:
            return [t for t in tickets if t.get("status") == status]
        return tickets
    
    def update_ticket(self, ticket_id, updates):
        tickets = self.load_data(self.tickets_file)
        for ticket in tickets:
            if ticket.get("id") == ticket_id:
                ticket.update(updates)
                return self.save_data(self.tickets_file, tickets)
        return False

data_manager = DataManager()

def is_admin(user_id):
    return user_id in ADMIN_IDS

def get_current_month():
    months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", 
              "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    return months[datetime.now().month - 1]

def get_available_months():
    current_month = datetime.now().month
    months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", 
              "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    available_months = []
    for i in range(3):
        month_index = (current_month - 1 + i) % 12
        available_months.append(months[month_index])
    return available_months

def get_day_of_week(day, month, year=2025):
    months_ru = {"Январь": 1, "Февраль": 2, "Март": 3, "Апрель": 4, "Май": 5, "Июнь": 6,
                "Июль": 7, "Август": 8, "Сентябрь": 9, "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12}
    date = datetime(year, months_ru[month], int(day))
    days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
    return days[date.weekday()]

def get_today_date():
    today = datetime.now()
    months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", 
              "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    return str(today.day), months[today.month - 1]

def get_tomorrow_date():
    tomorrow = datetime.now() + timedelta(days=1)
    months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", 
              "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    return str(tomorrow.day), months[tomorrow.month - 1]

def create_clean_schedule_image(schedule_data, group, month, day):
    try:
        width, height = 800, 400 + len(schedule_data) * 70
        image = Image.new('RGB', (width, height), color='white')
        draw = ImageDraw.Draw(image)
        try:
            font_large = ImageFont.truetype("arial.ttf", 24)
            font_medium = ImageFont.truetype("arial.ttf", 18)
            font_small = ImageFont.truetype("arial.ttf", 14)
            font_bold = ImageFont.truetype("arialbd.ttf", 16)
        except:
            font_large = ImageFont.load_default()
            font_medium = ImageFont.load_default()
            font_small = ImageFont.load_default()
            font_bold = ImageFont.load_default()
        title = f"Расписание {group}"
        subtitle = f"{day} {month} 2025"
        draw.text((width//2, 30), title, fill='black', font=font_large, anchor='mm')
        draw.text((width//2, 60), subtitle, fill='#666666', font=font_medium, anchor='mm')
        draw.line([(50, 90), (width - 50, 90)], fill='#e0e0e0', width=2)
        if schedule_data:
            headers = ["Пара", "Время", "Предмет", "Преподаватель", "Аудитория"]
            col_widths = [60, 100, 250, 200, 100]
            x_pos = 40
            y_pos = 120
            for i, header in enumerate(headers):
                draw.rectangle([x_pos, y_pos, x_pos + col_widths[i], y_pos + 35], 
                             outline='#cccccc', fill='#f5f5f5')
                draw.text((x_pos + col_widths[i]//2, y_pos + 17), header, 
                         fill='#333333', font=font_bold, anchor='mm')
                x_pos += col_widths[i]
            y_pos += 40
            for idx, lesson in enumerate(schedule_data):
                x_pos = 40
                row_color = '#ffffff' if idx % 2 == 0 else '#f9f9f9'
                row_data = [
                    lesson['пара'], lesson['время'], lesson['предмет'],
                    lesson['преподаватель'], lesson['аудитория']
                ]
                for i, data in enumerate(row_data):
                    draw.rectangle([x_pos, y_pos, x_pos + col_widths[i], y_pos + 50], 
                                 outline='#eeeeee', fill=row_color)
                    text = str(data)
                    if len(text) > 25 and i in [2, 3]:
                        text = text[:25] + "..."
                    draw.text((x_pos + 5, y_pos + 25), text, 
                             fill='#333333', font=font_small, anchor='lm')
                    x_pos += col_widths[i]
                y_pos += 55
        else:
            no_schedule_text = "Выходной день - занятий нет"
            text_bbox = draw.textbbox((0, 0), no_schedule_text, font=font_medium)
            text_width = text_bbox[2] - text_bbox[0]
            draw.text(((width - text_width) // 2, height // 2), no_schedule_text, 
                     fill='#666666', font=font_medium)
        footer_text = "Бот расписания КГТУ"
        footer_bbox = draw.textbbox((0, 0), footer_text, font=font_small)
        footer_width = footer_bbox[2] - footer_bbox[0]
        draw.text((width - footer_width - 20, height - 30), footer_text, 
                 fill='#999999', font=font_small)
        buf = io.BytesIO()
        image.save(buf, format='PNG', quality=90)
        buf.seek(0)
        return buf
    except Exception as e:
        logger.error(f"Error creating image: {e}")
        width, height = 400, 200
        image = Image.new('RGB', (width, height), color='white')
        draw = ImageDraw.Draw(image)
        draw.text((width//2, height//2), "Ошибка создания расписания", 
                 fill='black', anchor='mm')
        buf = io.BytesIO()
        image.save(buf, format='PNG')
        buf.seek(0)
        return buf

def format_classmate_info(classmate):
    name = f"{classmate.get('first_name', '')} {classmate.get('last_name', '')}".strip()
    username = classmate.get('username', '')
    if username:
        return f"• {name} 👉 @{username}"
    else:
        user_id = classmate.get('id', '')
        return f"• {name} (ID: {user_id}) 📝"

def format_schedule_day(schedule_data, group, month, day):
    if not schedule_data:
        return f"📅 {day} {month} - {group}\n\n🎉 Выходной! Расписания на этот день нет"
    header = f"📅 {day} {month} 2025 - {group}\n\n"
    schedule_text = ""
    for lesson in schedule_data:
        subject = lesson['предмет'].lower()
        emoji = "📚"
        if any(word in subject for word in ['экзамен', 'зачет']):
            emoji = "🎓"
        elif any(word in subject for word in ['лабораторная', 'лаб']):
            emoji = "🔬"
        elif any(word in subject for word in ['практика', 'семинар']):
            emoji = "💼"
        elif any(word in subject for word in ['физра', 'спорт']):
            emoji = "⚽"
        schedule_text += f"{emoji} {lesson['пара']} пара ({lesson['время']})\n"
        schedule_text += f"   📖 {lesson['предмет']}\n"
        schedule_text += f"   👨‍🏫 {lesson['преподаватель']}\n"
        schedule_text += f"   🏫 {lesson['аудитория']}\n\n"
    return header + schedule_text

def get_groups_keyboard():
    groups = data_manager.get_groups()
    keyboard = []
    for group in groups:
        keyboard.append([InlineKeyboardButton(group, callback_data=f"group_{group}")])
    return InlineKeyboardMarkup(keyboard)

def get_main_menu_keyboard(user_group=None):
    keyboard = [
        [InlineKeyboardButton("📅 Получить расписание", callback_data="get_schedule")],
        [InlineKeyboardButton("🕒 Расписание звонков", callback_data="bell_schedule")],
        [InlineKeyboardButton("👥 Одногруппники", callback_data="classmates")],
        [InlineKeyboardButton("🔔 Уведомления", callback_data="notifications")],
        [InlineKeyboardButton("❓ Техподдержка", callback_data="support")],
        [InlineKeyboardButton("👨‍💻 Разработчик", callback_data="developer")]
    ]
    if user_group:
        keyboard.append([InlineKeyboardButton("🔄 Сменить группу", callback_data="change_group")])
    if is_admin(1393492435):
        keyboard.append([InlineKeyboardButton("⚙️ Админ-панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_notifications_keyboard(user_id):
    settings = data_manager.get_notification_settings(user_id)
    enabled = settings.get("enabled", True)
    keyboard = [
        [InlineKeyboardButton("✅ Включены" if enabled else "❌ Выключены", 
                            callback_data="toggle_notifications")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_schedule_main_keyboard():
    keyboard = [
        [InlineKeyboardButton("📅 На сегодня", callback_data="schedule_today")],
        [InlineKeyboardButton("📅 На завтра", callback_data="schedule_tomorrow")],
        [InlineKeyboardButton("📅 Выбрать дату", callback_data="schedule_custom")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_months_keyboard():
    months = get_available_months()
    keyboard = []
    row = []
    for month in months:
        row.append(InlineKeyboardButton(month, callback_data=f"month_{month}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_schedule_main")])
    return InlineKeyboardMarkup(keyboard)

def get_days_keyboard(month, group):
    schedule = data_manager.get_schedule(group, month)
    days = list(schedule.keys()) if schedule else []
    keyboard = []
    row = []
    for day in sorted(days, key=int):
        row.append(InlineKeyboardButton(day, callback_data=f"day_{month}_{day}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_months")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Дашборд активности", callback_data="admin_dashboard")],
        [InlineKeyboardButton("📈 Статистика посещаемости", callback_data="admin_attendance")],
        [InlineKeyboardButton("🏆 Рейтинг популярности", callback_data="admin_popular")],
        [InlineKeyboardButton("🚨 Мониторинг ошибок", callback_data="admin_errors")],
        [InlineKeyboardButton("📝 Шаблоны сообщений", callback_data="admin_templates")],
        [InlineKeyboardButton("🎫 Система тикетов", callback_data="admin_tickets")],
        [InlineKeyboardButton("📢 Отправить уведомление", callback_data="admin_notify")],
        [InlineKeyboardButton("🕒 Настройка уведомлений", callback_data="admin_notification_settings")],
        [InlineKeyboardButton("➕ Добавить группу", callback_data="admin_add_group")],
        [InlineKeyboardButton("📅 Добавить расписание", callback_data="admin_add_schedule")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_classmates_keyboard(group):
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить список", callback_data="refresh_classmates")],
        [InlineKeyboardButton("💬 Как добавить ссылку?", callback_data="how_to_add_link")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_groups_keyboard_for_notify():
    groups = data_manager.get_groups()
    keyboard = []
    for group in groups:
        keyboard.append([InlineKeyboardButton(group, callback_data=f"notify_group_{group}")])
    keyboard.append([InlineKeyboardButton("📢 Всем группам", callback_data="notify_all")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_groups_keyboard():
    groups = data_manager.get_groups()
    keyboard = []
    for group in groups:
        keyboard.append([InlineKeyboardButton(group, callback_data=f"admin_group_{group}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_add_schedule")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_months_keyboard():
    months = get_available_months()
    keyboard = []
    row = []
    for month in months:
        row.append(InlineKeyboardButton(month, callback_data=f"admin_month_{month}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back_to_groups")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_days_keyboard(month):
    keyboard = []
    row = []
    for day in range(1, 32):
        row.append(InlineKeyboardButton(str(day), callback_data=f"admin_day_{month}_{day}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back_to_months")])
    return InlineKeyboardMarkup(keyboard)

def get_manual_schedule_keyboard():
    keyboard = [
        [InlineKeyboardButton("1 пара", callback_data="add_lesson_1")],
        [InlineKeyboardButton("2 пара", callback_data="add_lesson_2")],
        [InlineKeyboardButton("3 пара", callback_data="add_lesson_3")],
        [InlineKeyboardButton("4 пара", callback_data="add_lesson_4")],
        [InlineKeyboardButton("5 пара", callback_data="add_lesson_5")],
        [InlineKeyboardButton("6 пара", callback_data="add_lesson_6")],
        [InlineKeyboardButton("7 пара", callback_data="add_lesson_7")],
        [InlineKeyboardButton("✅ Завершить добавление", callback_data="finish_schedule")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back_to_days")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_templates_keyboard():
    templates = data_manager.get_templates()
    keyboard = []
    for name in templates.keys():
        keyboard.append([InlineKeyboardButton(name.capitalize(), callback_data=f"template_{name}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_tickets_keyboard():
    tickets = data_manager.get_tickets("open")
    keyboard = []
    for ticket in tickets[:10]:
        keyboard.append([InlineKeyboardButton(f"🎫 #{ticket['id']}", callback_data=f"ticket_{ticket['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_notification_settings_keyboard():
    keyboard = [
        [InlineKeyboardButton("🕒 Изменить время уведомлений", callback_data="admin_change_notification_time")],
        [InlineKeyboardButton("👥 Управление группами для уведомлений", callback_data="admin_manage_notification_groups")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_notification_groups_keyboard():
    groups = data_manager.get_groups()
    settings = data_manager.get_settings()
    enabled_groups = settings.get("enabled_groups", [])
    keyboard = []
    for group in groups:
        status = "✅" if group in enabled_groups else "❌"
        keyboard.append([InlineKeyboardButton(f"{status} {group}", callback_data=f"admin_toggle_group_{group}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_notification_settings")])
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = update.effective_user
    data_manager.log_activity(user_id, "start")
    user_profile = {
        "id": user_id, "first_name": user.first_name,
        "last_name": user.last_name or "", "username": user.username or ""
    }
    data_manager.update_user_profile(user_id, user_profile)
    user_group = data_manager.get_user_group(user_id)
    if user_group:
        data_manager.add_classmate(user_group, user_profile)
        await show_main_menu(update, context, user_group)
    else:
        await update.message.reply_text(
            "👋 Добро пожаловать в бот расписания!\nВыберите вашу группу:",
            reply_markup=get_groups_keyboard()
        )

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_group=None):
    query = update.callback_query
    user_id = update.effective_user.id
    if not user_group:
        user_group = data_manager.get_user_group(user_id)
    menu_text = "🏠 Главное меню"
    if user_group:
        menu_text += f"\n🎓 Группа: {user_group}"
    menu_text += "\n\nВыберите действие:"
    if query:
        await query.answer()
        await query.edit_message_text(menu_text, reply_markup=get_main_menu_keyboard(user_group))
    else:
        await update.message.reply_text(menu_text, reply_markup=get_main_menu_keyboard(user_group))
    data_manager.log_activity(user_id, "main_menu", user_group)

async def show_notifications_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    settings = data_manager.get_notification_settings(user_id)
    enabled = settings.get("enabled", True)
    status_text = "✅ включены" if enabled else "❌ выключены"
    await query.edit_message_text(
        f"🔔 Настройки уведомлений\n\nСтатус: {status_text}\n\n"
        f"При включенных уведомлениях вы будете получать автоматические "
        f"сообщения о расписании на завтрашний день.",
        reply_markup=get_notifications_keyboard(user_id)
    )

async def toggle_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    settings = data_manager.get_notification_settings(user_id)
    current_status = settings.get("enabled", True)
    new_status = not current_status
    data_manager.set_notification_settings(user_id, {"enabled": new_status})
    status_text = "✅ включены" if new_status else "❌ выключены"
    await query.edit_message_text(
        f"🔔 Настройки уведомлений\n\nСтатус: {status_text}\n\n"
        f"Уведомления {'включены' if new_status else 'выключены'}.",
        reply_markup=get_notifications_keyboard(user_id)
    )

async def send_tomorrow_schedule_notifications(context: ContextTypes.DEFAULT_TYPE):
    try:
        day, month = get_tomorrow_date()
        settings = data_manager.get_settings()
        enabled_groups = settings.get("enabled_groups", [])
        groups_to_notify = data_manager.get_groups() if not enabled_groups else enabled_groups
        
        for group in groups_to_notify:
            schedule_data = data_manager.get_schedule(group, month, day)
            if schedule_data:
                users_to_notify = data_manager.get_users_with_notifications(group)
                for user_id in users_to_notify:
                    try:
                        image_buf = create_clean_schedule_image(schedule_data, group, month, day)
                        schedule_text = (
                            f"🔔 *Расписание на завтра*\n\n"
                            f"📅 {day} {month} - {group}\n\n"
                            f"Не забудьте подготовиться к занятиям! 📚"
                        )
                        await context.bot.send_photo(
                            chat_id=user_id,
                            photo=InputFile(image_buf, filename='schedule.png'),
                            caption=schedule_text,
                            parse_mode='Markdown'
                        )
                        logger.info(f"Sent tomorrow schedule notification to user {user_id}")
                    except Exception as e:
                        logger.error(f"Error sending notification to user {user_id}: {e}")
    except Exception as e:
        logger.error(f"Error in send_tomorrow_schedule_notifications: {e}")

async def show_classmates_list(update: Update, context: ContextTypes.DEFAULT_TYPE, group, refreshed=False):
    query = update.callback_query
    user_id = update.effective_user.id
    classmates = data_manager.get_classmates(group)
    if refreshed:
        user = update.effective_user
        user_profile = {
            "id": user_id, "first_name": user.first_name,
            "last_name": user.last_name or "", "username": user.username or ""
        }
        data_manager.add_classmate(group, user_profile)
        classmates = data_manager.get_classmates(group)
    if classmates:
        classmates_text = f"👥 Одногруппники ({group}):\n\n"
        users_with_username = []
        users_without_username = []
        for classmate in classmates:
            if classmate.get('username'):
                users_with_username.append(classmate)
            else:
                users_without_username.append(classmate)
        for classmate in users_with_username:
            classmates_text += format_classmate_info(classmate) + "\n"
        if users_without_username:
            classmates_text += "\n👤 Пользователи без username:\n"
            for classmate in users_without_username:
                classmates_text += format_classmate_info(classmate) + "\n"
        classmates_text += f"\n📊 Всего: {len(classmates)} человек"
        if users_without_username:
            classmates_text += f"\n⚠️ {len(users_without_username)} без username"
    else:
        classmates_text = f"👥 В группе {group} пока нет одногруппников"
    data_manager.log_activity(user_id, "view_classmates", group)
    await query.edit_message_text(classmates_text, reply_markup=get_classmates_keyboard(group))

async def show_schedule_for_date(update: Update, context: ContextTypes.DEFAULT_TYPE, group, month, day, date_description):
    query = update.callback_query
    user_id = update.effective_user.id
    schedule_data = data_manager.get_schedule(group, month, day)
    data_manager.log_activity(user_id, f"schedule_{date_description}", group)
    if schedule_data:
        image_buf = create_clean_schedule_image(schedule_data, group, month, day)
        schedule_text = format_schedule_day(schedule_data, group, month, day)
        await query.message.reply_photo(
            photo=InputFile(image_buf, filename='schedule.png'),
            caption=schedule_text,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_schedule_main")]])
        )
    else:
        text = f"📅 {date_description} - {group}\n\n🎉 Выходной! Расписания на этот день нет"
        await query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_schedule_main")]])
        )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    callback_data = query.data
    user_id = update.effective_user.id
    user_group = data_manager.get_user_group(user_id)
    
    try:
        if callback_data.startswith("group_"):
            group = callback_data.replace("group_", "")
            data_manager.set_user_group(user_id, group)
            user = update.effective_user
            user_profile = {
                "id": user_id, "first_name": user.first_name,
                "last_name": user.last_name or "", "username": user.username or ""
            }
            data_manager.add_classmate(group, user_profile)
            data_manager.log_activity(user_id, f"group_select_{group}", group)
            await show_main_menu(update, context, group)
        
        elif callback_data == "get_schedule":
            if user_group:
                data_manager.log_activity(user_id, "get_schedule", user_group)
                await query.edit_message_text(
                    f"📅 Расписание для группы {user_group}\nВыберите опцию:",
                    reply_markup=get_schedule_main_keyboard()
                )
            else:
                await query.edit_message_text("❌ Сначала выберите группу!")
        
        elif callback_data == "schedule_today":
            if user_group:
                day, month = get_today_date()
                await show_schedule_for_date(update, context, user_group, month, day, "сегодня")
            else:
                await query.edit_message_text("❌ Сначала выберите группу!")
        
        elif callback_data == "schedule_tomorrow":
            if user_group:
                day, month = get_tomorrow_date()
                await show_schedule_for_date(update, context, user_group, month, day, "завтра")
            else:
                await query.edit_message_text("❌ Сначала выберите группу!")
        
        elif callback_data == "schedule_custom":
            if user_group:
                data_manager.log_activity(user_id, "schedule_custom", user_group)
                await query.edit_message_text(
                    f"📅 Выберите месяц для группы {user_group}:",
                    reply_markup=get_months_keyboard()
                )
            else:
                await query.edit_message_text("❌ Сначала выберите группу!")
        
        elif callback_data == "bell_schedule":
            templates = data_manager.get_templates()
            data_manager.log_activity(user_id, "bell_schedule", user_group)
            await query.edit_message_text(
                templates.get("звонки", "Расписание звонков не настроено"),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])
            )
        
        elif callback_data == "classmates":
            if user_group:
                await show_classmates_list(update, context, user_group)
            else:
                await query.edit_message_text("❌ Сначала выберите группу!")
        
        elif callback_data == "notifications":
            await show_notifications_settings(update, context)
        
        elif callback_data == "toggle_notifications":
            await toggle_notifications(update, context)
        
        elif callback_data == "refresh_classmates":
            if user_group:
                await show_classmates_list(update, context, user_group, refreshed=True)
        
        elif callback_data == "how_to_add_link":
            await query.edit_message_text(
                "💡 Как добавить ссылку на ваш профиль?\n\n"
                "1. Откройте настройки Telegram\n"
                "2. Перейдите в 'Имя пользователь' (Username)\n"
                "3. Установите уникальный username\n"
                "4. Вернитесь в этот раздел и обновите список\n\n"
                "После этого одногруппники смогут найти вас по @username!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="classmates")]])
            )
        
        elif callback_data == "change_group":
            data_manager.log_activity(user_id, "change_group", user_group)
            await query.edit_message_text("🔄 Выберите новую группу:", reply_markup=get_groups_keyboard())
        
        elif callback_data == "support":
            data_manager.log_activity(user_id, "support", user_group)
            await query.edit_message_text(
                "❓ Техподдержка\n\nОпишите вашу проблему или вопрос:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])
            )
            context.user_data["awaiting_support"] = True
        
        elif callback_data == "developer":
            data_manager.log_activity(user_id, "developer", user_group)
            await query.edit_message_text(
                "👨‍💻 Разработчик\n\n🤖 Бот создан для удобного просмотра расписания занятий\n💡 По вопросам и предложениям: @Itired_siii\n🐛 Сообщения об ошибках приветствуются!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]])
            )
        
        elif callback_data == "admin_panel":
            if is_admin(user_id):
                data_manager.log_activity(user_id, "admin_panel")
                await query.edit_message_text("⚙️ Админ-панель\nВыберите действие:", reply_markup=get_admin_keyboard())
            else:
                await query.edit_message_text("⛔ Доступ запрещен")
        
        elif callback_data.startswith("month_"):
            month = callback_data.replace("month_", "")
            data_manager.log_activity(user_id, f"month_select_{month}", user_group)
            await query.edit_message_text(f"📅 Выберите день для {month}:", reply_markup=get_days_keyboard(month, user_group))
        
        elif callback_data.startswith("day_"):
            try:
                parts = callback_data.split("_")
                if len(parts) == 3:
                    _, month, day = parts
                    await show_schedule_for_date(update, context, user_group, month, day, f"{day} {month}")
                else:
                    await query.edit_message_text("❌ Ошибка формата даты")
            except Exception as e:
                logger.error(f"Error parsing day callback: {e}")
                await query.edit_message_text("❌ Ошибка при обработке запроса")
        
        elif callback_data == "back_to_main":
            await show_main_menu(update, context, user_group)
        
        elif callback_data == "back_to_schedule_main":
            if user_group:
                await query.edit_message_text(
                    f"📅 Расписание для группы {user_group}\nВыберите опцию:",
                    reply_markup=get_schedule_main_keyboard()
                )
        
        elif callback_data == "back_to_months":
            if user_group:
                await query.edit_message_text(
                    f"📅 Выберите месяц для группы {user_group}:",
                    reply_markup=get_months_keyboard()
                )
        
        elif callback_data == "admin_dashboard":
            if is_admin(user_id):
                await show_admin_dashboard(update, context)
        
        elif callback_data == "admin_attendance":
            if is_admin(user_id):
                await show_attendance_stats(update, context)
        
        elif callback_data == "admin_popular":
            if is_admin(user_id):
                await show_popular_stats(update, context)
        
        elif callback_data == "admin_errors":
            if is_admin(user_id):
                await show_error_monitor(update, context)
        
        elif callback_data == "admin_templates":
            if is_admin(user_id):
                await show_templates_manager(update, context)
        
        elif callback_data == "admin_tickets":
            if is_admin(user_id):
                await show_tickets_manager(update, context)
        
        elif callback_data == "admin_notify":
            if is_admin(user_id):
                await query.edit_message_text(
                    "📢 Отправка уведомления\n\nВыберите группу для отправки:",
                    reply_markup=get_groups_keyboard_for_notify()
                )
        
        elif callback_data == "admin_notification_settings":
            if is_admin(user_id):
                await show_admin_notification_settings(update, context)
        
        elif callback_data == "admin_change_notification_time":
            if is_admin(user_id):
                await query.edit_message_text(
                    "🕒 Изменение времени уведомлений\n\n"
                    "Текущее время: 18:00\n\n"
                    "Введите новое время в формате ЧЧ:ММ (например, 19:30):"
                )
                context.user_data["awaiting_notification_time"] = True
        
        elif callback_data == "admin_manage_notification_groups":
            if is_admin(user_id):
                await show_notification_groups_management(update, context)
        
        elif callback_data.startswith("admin_toggle_group_"):
            if is_admin(user_id):
                group = callback_data.replace("admin_toggle_group_", "")
                settings = data_manager.get_settings()
                enabled_groups = settings.get("enabled_groups", [])
                if group in enabled_groups:
                    enabled_groups.remove(group)
                else:
                    enabled_groups.append(group)
                settings["enabled_groups"] = enabled_groups
                data_manager.update_settings(settings)
                await show_notification_groups_management(update, context)
        
        elif callback_data == "admin_add_group":
            if is_admin(user_id):
                await query.edit_message_text("➕ Добавление новой группы\n\nВведите название новой группы:")
                context.user_data["awaiting_group_name"] = True
        
        elif callback_data == "admin_add_schedule":
            if is_admin(user_id):
                await query.edit_message_text("📅 Добавление расписания\n\nВыберите группу:", reply_markup=get_admin_groups_keyboard())
        
        elif callback_data.startswith("admin_group_"):
            if is_admin(user_id):
                group = callback_data.replace("admin_group_", "")
                context.user_data["schedule_group"] = group
                await query.edit_message_text(f"📅 Добавление расписания для {group}\n\nВыберите месяц:", reply_markup=get_admin_months_keyboard())
        
        elif callback_data.startswith("admin_month_"):
            if is_admin(user_id):
                month = callback_data.replace("admin_month_", "")
                context.user_data["schedule_month"] = month
                await query.edit_message_text(
                    f"📅 Добавление расписания\nГруппа: {context.user_data['schedule_group']}\nМесяц: {month}\n\nВыберите день:",
                    reply_markup=get_admin_days_keyboard(month)
                )
        
        elif callback_data.startswith("admin_day_"):
            if is_admin(user_id):
                try:
                    parts = callback_data.replace("admin_day_", "").split("_")
                    if len(parts) == 2:
                        month, day = parts
                        context.user_data["schedule_day"] = day
                        group = context.user_data["schedule_group"]
                        day_of_week = get_day_of_week(day, month)
                        context.user_data["current_schedule"] = []
                        context.user_data["schedule_day_of_week"] = day_of_week
                        await query.edit_message_text(
                            f"📅 Добавление расписания\nГруппа: {group}\nДата: {day} {month} ({day_of_week})\n\nВыберите пару для добавления:",
                            reply_markup=get_manual_schedule_keyboard()
                        )
                    else:
                        await query.edit_message_text("❌ Ошибка формата даты")
                except Exception as e:
                    logger.error(f"Error parsing admin day callback: {e}")
                    await query.edit_message_text("❌ Ошибка при обработке запроса")
        
        elif callback_data.startswith("add_lesson_"):
            if is_admin(user_id):
                lesson_num = callback_data.replace("add_lesson_", "")
                context.user_data["adding_lesson"] = lesson_num
                await query.edit_message_text(f"📚 Добавление {lesson_num} пары\n\nВведите название предмета:")
                context.user_data["awaiting_subject"] = True
        
        elif callback_data == "finish_schedule":
            if is_admin(user_id):
                await save_manual_schedule(update, context)
        
        elif callback_data == "admin_back_to_months":
            if is_admin(user_id):
                await query.edit_message_text(
                    f"📅 Добавление расписания\nГруппа: {context.user_data['schedule_group']}\n\nВыберите месяц:",
                    reply_markup=get_admin_months_keyboard()
                )
        
        elif callback_data == "admin_back_to_groups":
            if is_admin(user_id):
                await query.edit_message_text("📝 Добавление расписания\n\nВыберите группу:", reply_markup=get_admin_groups_keyboard())
        
        elif callback_data == "admin_back_to_days":
            if is_admin(user_id):
                group = context.user_data["schedule_group"]
                month = context.user_data["schedule_month"]
                await query.edit_message_text(
                    f"📅 Добавление расписания\nГруппа: {group}\nМесяц: {month}\n\nВыберите день:",
                    reply_markup=get_admin_days_keyboard(month)
                )
        
        elif callback_data.startswith("template_"):
            if is_admin(user_id):
                template_name = callback_data.replace("template_", "")
                templates = data_manager.get_templates()
                template_content = templates.get(template_name, "")
                await query.edit_message_text(
                    f"📝 Шаблон: {template_name}\n\n{template_content}\n\nВведите новый текст для этого шаблона:"
                )
                context.user_data["editing_template"] = template_name
        
        elif callback_data.startswith("ticket_"):
            if is_admin(user_id):
                ticket_id = int(callback_data.replace("ticket_", ""))
                await show_ticket_details(update, context, ticket_id)
        
        elif callback_data.startswith("notify_group_"):
            if is_admin(user_id):
                group = callback_data.replace("notify_group_", "")
                await query.edit_message_text(f"📢 Отправка уведомления группе {group}\n\nВведите текст уведомления:")
                context.user_data["sending_notification"] = group
        
        elif callback_data == "notify_all":
            if is_admin(user_id):
                await query.edit_message_text("📢 Отправка уведомления всем группам\n\nВведите текст уведомления:")
                context.user_data["sending_notification"] = "all"
        
        elif callback_data.startswith("reply_ticket_"):
            if is_admin(user_id):
                ticket_id = int(callback_data.replace("reply_ticket_", ""))
                await query.edit_message_text(f"💬 Ответ на тикет #{ticket_id}\n\nВведите ваш ответ:")
                context.user_data["replying_to_ticket"] = ticket_id
        
        elif callback_data.startswith("close_ticket_"):
            if is_admin(user_id):
                ticket_id = int(callback_data.replace("close_ticket_", ""))
                if data_manager.update_ticket(ticket_id, {"status": "closed"}):
                    await query.edit_message_text(f"✅ Тикет #{ticket_id} закрыт!", reply_markup=get_admin_keyboard())
                else:
                    await query.edit_message_text("❌ Ошибка при закрытии тикета", reply_markup=get_admin_keyboard())
    
    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        try:
            await query.edit_message_text("❌ Произошла ошибка при обработке запроса", reply_markup=get_main_menu_keyboard(user_group))
        except Exception as edit_error:
            await query.message.reply_text("❌ Произошла ошибка при обработке запроса", reply_markup=get_main_menu_keyboard(user_group))

async def show_admin_notification_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    settings = data_manager.get_settings()
    notification_time = settings.get("notification_time", "18:00")
    enabled_groups = settings.get("enabled_groups", [])
    
    if enabled_groups:
        groups_text = "\n".join([f"✅ {group}" for group in enabled_groups])
    else:
        groups_text = "📢 Все группы"
    
    await query.edit_message_text(
        f"⚙️ Настройки уведомлений\n\n"
        f"🕒 Время отправки: {notification_time}\n\n"
        f"👥 Группы для уведомлений:\n{groups_text}\n\n"
        f"Выберите действие:",
        reply_markup=get_notification_settings_keyboard()
    )

async def show_notification_groups_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    settings = data_manager.get_settings()
    enabled_groups = settings.get("enabled_groups", [])
    
    if enabled_groups:
        status_text = f"✅ Включено для {len(enabled_groups)} групп"
    else:
        status_text = "📢 Все группы включены"
    
    await query.edit_message_text(
        f"👥 Управление группами для уведомлений\n\n"
        f"Статус: {status_text}\n\n"
        f"Нажмите на группу чтобы включить/выключить:",
        reply_markup=get_notification_groups_keyboard()
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_text = update.message.text
    user_group = data_manager.get_user_group(user_id)
    
    try:
        if context.user_data.get("awaiting_support"):
            context.user_data["awaiting_support"] = False
            data_manager.create_ticket(user_id, message_text)
            await update.message.reply_text(
                "✅ Ваше сообщение отправлено в техподдержку. Мы ответим вам в ближайшее время.",
                reply_markup=get_main_menu_keyboard(user_group)
            )
            data_manager.log_activity(user_id, "support_ticket_created", user_group)
            return
        
        if context.user_data.get("awaiting_group_name") and is_admin(user_id):
            context.user_data["awaiting_group_name"] = False
            if data_manager.add_group(message_text):
                await update.message.reply_text(f"✅ Группа '{message_text}' успешно добавлена!", reply_markup=get_admin_keyboard())
            else:
                await update.message.reply_text(f"❌ Группа '{message_text}' уже существует или произошла ошибка!", reply_markup=get_admin_keyboard())
            return
        
        if context.user_data.get("awaiting_notification_time") and is_admin(user_id):
            context.user_data["awaiting_notification_time"] = False
            if re.match(r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$', message_text):
                settings = data_manager.get_settings()
                settings["notification_time"] = message_text
                data_manager.update_settings(settings)
                await update.message.reply_text(f"✅ Время уведомлений изменено на {message_text}", reply_markup=get_admin_keyboard())
            else:
                await update.message.reply_text("❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 18:00)", reply_markup=get_admin_keyboard())
            return
        
        if context.user_data.get("editing_template") and is_admin(user_id):
            template_name = context.user_data["editing_template"]
            context.user_data["editing_template"] = None
            if data_manager.update_template(template_name, message_text):
                await update.message.reply_text(f"✅ Шаблон '{template_name}' успешно обновлен!", reply_markup=get_admin_keyboard())
            else:
                await update.message.reply_text(f"❌ Ошибка при обновлении шаблона!", reply_markup=get_admin_keyboard())
            return
        
        if context.user_data.get("replying_to_ticket") and is_admin(user_id):
            ticket_id = context.user_data["replying_to_ticket"]
            context.user_data["replying_to_ticket"] = None
            tickets = data_manager.get_tickets()
            ticket = next((t for t in tickets if t.get("id") == ticket_id), None)
            if ticket:
                if "replies" not in ticket:
                    ticket["replies"] = []
                ticket["replies"].append({
                    "admin_id": user_id, "message": message_text, "timestamp": datetime.now().isoformat()
                })
                data_manager.update_ticket(ticket_id, ticket)
                try:
                    await context.bot.send_message(chat_id=ticket["user_id"], text=f"💬 Ответ от техподдержки:\n\n{message_text}")
                except Exception as e:
                    logger.error(f"Error sending message to user: {e}")
                await update.message.reply_text(f"✅ Ответ на тикет #{ticket_id} отправлен!", reply_markup=get_admin_keyboard())
            return
        
        if context.user_data.get("sending_notification") and is_admin(user_id):
            target_group = context.user_data["sending_notification"]
            context.user_data["sending_notification"] = None
            users = data_manager.get_all_users()
            sent_count = 0
            for uid, group in users.items():
                if target_group == "all" or group == target_group:
                    try:
                        await context.bot.send_message(chat_id=int(uid), text=f"📢 Уведомление от администратора:\n\n{message_text}")
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Error sending notification to {uid}: {e}")
            await update.message.reply_text(f"✅ Уведомление отправлено {sent_count} пользователям!", reply_markup=get_admin_keyboard())
            return
        
        if context.user_data.get("awaiting_subject") and is_admin(user_id):
            context.user_data["awaiting_subject"] = False
            context.user_data["current_subject"] = message_text
            context.user_data["awaiting_teacher"] = True
            await update.message.reply_text(f"📚 Предмет: {message_text}\n\nВведите ФИО преподавателя:")
            return
        
        elif context.user_data.get("awaiting_teacher") and is_admin(user_id):
            context.user_data["awaiting_teacher"] = False
            context.user_data["current_teacher"] = message_text
            context.user_data["awaiting_classroom"] = True
            await update.message.reply_text(f"📚 Предмет: {context.user_data['current_subject']}\n👨‍🏫 Преподаватель: {message_text}\n\nВведите номер аудитории:")
            return
        
        elif context.user_data.get("awaiting_classroom") and is_admin(user_id):
            context.user_data["awaiting_classroom"] = False
            lesson_num = context.user_data["adding_lesson"]
            subject = context.user_data["current_subject"]
            teacher = context.user_data["current_teacher"]
            classroom = message_text
            day_of_week = context.user_data["schedule_day_of_week"]
            time_slot = BELL_SCHEDULE.get(day_of_week, {}).get(lesson_num, "Время не указано")
            lesson_data = {
                "пара": lesson_num, "предмет": subject, "преподаватель": teacher,
                "аудитория": classroom, "время": time_slot
            }
            if "current_schedule" not in context.user_data:
                context.user_data["current_schedule"] = []
            context.user_data["current_schedule"] = [lesson for lesson in context.user_data["current_schedule"] if lesson["пара"] != lesson_num]
            context.user_data["current_schedule"].append(lesson_data)
            context.user_data["current_schedule"].sort(key=lambda x: int(x["пара"]))
            schedule_text = "✅ Пара добавлена!\n\nТекущее расписание:\n"
            for lesson in context.user_data["current_schedule"]:
                schedule_text += f"{lesson['пара']} пара: {lesson['предмет']} ({lesson['преподаватель']}) - {lesson['аудитория']}\n"
            group = context.user_data["schedule_group"]
            month = context.user_data["schedule_month"]
            day = context.user_data["schedule_day"]
            await update.message.reply_text(
                f"📅 Добавление расписания\nГруппа: {group}\nДата: {day} {month}\n\n{schedule_text}\nВыберите следующую пару:",
                reply_markup=get_manual_schedule_keyboard()
            )
            return
        
        await show_main_menu(update, context)
    
    except Exception as e:
        logger.error(f"Error in message handler: {e}")
        await update.message.reply_text("❌ Произошла ошибка при обработке сообщения", reply_markup=get_main_menu_keyboard(user_group))

async def save_manual_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        group = context.user_data["schedule_group"]
        month = context.user_data["schedule_month"]
        day = context.user_data["schedule_day"]
        schedule_data = context.user_data.get("current_schedule", [])
        if not schedule_data:
            await update.callback_query.edit_message_text("❌ Не добавлено ни одной пары!", reply_markup=get_manual_schedule_keyboard())
            return
        if data_manager.update_schedule(group, month, day, schedule_data):
            image_buf = create_clean_schedule_image(schedule_data, group, month, day)
            await update.callback_query.message.reply_photo(
                photo=InputFile(image_buf, filename='schedule.png'),
                caption=f"✅ Расписание успешно добавлено!\n\nГруппа: {group}\nДата: {day} {month}\nПар: {len(schedule_data)}",
                reply_markup=get_admin_keyboard()
            )
            context.user_data["current_schedule"] = []
            context.user_data["adding_lesson"] = None
            context.user_data["awaiting_subject"] = False
            context.user_data["awaiting_teacher"] = False
            context.user_data["awaiting_classroom"] = False
        else:
            await update.callback_query.edit_message_text("❌ Ошибка при сохранении расписания", reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Error saving manual schedule: {e}")
        await update.callback_query.edit_message_text("❌ Ошибка при сохранении расписания", reply_markup=get_admin_keyboard())

async def show_admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    stats = data_manager.get_statistics()
    total_users = len(stats.get("user_activity", {}))
    total_actions = sum(stats.get("feature_usage", {}).values())
    popular_feature = max(stats.get("feature_usage", {}).items(), key=lambda x: x[1], default=("Нет данных", 0))
    stats_text = f"📈 Статистика бота:\n\n👥 Всего пользователей: {total_users}\n📊 Всего действий: {total_actions}\n🏆 Популярная функция: {popular_feature[0]} ({popular_feature[1]} раз)\n🚨 Ошибок в логах: {len(stats.get('errors', []))}"
    await query.edit_message_text(stats_text, reply_markup=get_admin_keyboard())

async def show_attendance_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    stats = data_manager.get_statistics()
    group_stats = stats.get("group_usage", {})
    text_stats = "📊 Статистика посещаемости:\n\n"
    for group, count in group_stats.items():
        text_stats += f"🎓 {group}: {count} запросов\n"
    if not group_stats:
        text_stats += "Нет данных о посещаемости"
    await query.edit_message_text(text_stats, reply_markup=get_admin_keyboard())

async def show_popular_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    stats = data_manager.get_statistics()
    feature_usage = stats.get("feature_usage", {})
    text_stats = "🏆 Самые популярные функции:\n\n"
    for feature, count in sorted(feature_usage.items(), key=lambda x: x[1], reverse=True)[:10]:
        text_stats += f"📊 {feature}: {count} раз\n"
    if not feature_usage:
        text_stats += "Нет данных о популярности"
    await query.edit_message_text(text_stats, reply_markup=get_admin_keyboard())

async def show_error_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    stats = data_manager.get_statistics()
    errors = stats.get("errors", [])
    error_text = "🚨 Мониторинг ошибок:\n\n"
    if errors:
        recent_errors = errors[-10:] 
        for error in reversed(recent_errors):
            timestamp = datetime.fromisoformat(error["timestamp"]).strftime("%d.%m %H:%M")
            error_text += f"⏰ {timestamp}\n❌ {error['error'][:100]}...\n"
            if error.get('user_id'):
                error_text += f"👤 User: {error['user_id']}\n"
            error_text += "─" * 20 + "\n"
    else:
        error_text += "✅ Ошибок не обнаружено"
    await query.edit_message_text(error_text, reply_markup=get_admin_keyboard())

async def show_templates_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.edit_message_text("📝 Управление шаблонами сообщений\nВыберите шаблон для редактирования:", reply_markup=get_templates_keyboard())

async def show_tickets_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    tickets = data_manager.get_tickets("open")
    if tickets:
        tickets_text = "🎫 Открытые тикеты:\n\n"
        for ticket in tickets[:10]:
            created = datetime.fromisoformat(ticket["created_at"]).strftime("%d.%m %H:%M")
            tickets_text += f"#{ticket['id']} - {created}\n{ticket['message'][:50]}...\n\n"
    else:
        tickets_text = "✅ Нет открытых тикетов"
    await query.edit_message_text(tickets_text, reply_markup=get_tickets_keyboard())

async def show_ticket_details(update: Update, context: ContextTypes.DEFAULT_TYPE, ticket_id: int):
    query = update.callback_query
    tickets = data_manager.get_tickets()
    ticket = next((t for t in tickets if t.get("id") == ticket_id), None)
    if ticket:
        created = datetime.fromisoformat(ticket["created_at"]).strftime("%d.%m.%Y %H:%M")
        ticket_text = f"🎫 Тикет #{ticket['id']}\n👤 Пользователь: {ticket['user_id']}\n⏰ Создан: {created}\n📝 Статус: {ticket['status']}\n\n💬 Сообщение:\n{ticket['message']}\n\n"
        if ticket.get('replies'):
            ticket_text += f"💬 Ответы ({len(ticket['replies'])}):\n"
            for reply in ticket['replies']:
                reply_time = datetime.fromisoformat(reply['timestamp']).strftime("%d.%m %H:%M")
                ticket_text += f"👨‍💼 {reply_time}: {reply['message']}\n"
        keyboard = [
            [InlineKeyboardButton("💬 Ответить", callback_data=f"reply_ticket_{ticket_id}")],
            [InlineKeyboardButton("✅ Закрыть тикет", callback_data=f"close_ticket_{ticket_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_tickets")]
        ]
        await query.edit_message_text(ticket_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await query.edit_message_text("Тикет не найден")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error_msg = str(context.error)
    user_id = update.effective_user.id if update and update.effective_user else None
    data_manager.log_error(error_msg, user_id)
    logger.error(f"Exception while handling an update: {context.error}")

async def scheduled_notifications(context: ContextTypes.DEFAULT_TYPE):
    await send_tomorrow_schedule_notifications(context)

def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_error_handler(error_handler)
    
    job_queue = application.job_queue
    if job_queue:
        settings = data_manager.get_settings()
        notification_time = settings.get("notification_time", "18:00")
        try:
            job_queue.run_daily(
                scheduled_notifications,
                time=datetime.strptime(notification_time, "%H:%M").time(),
                days=(0, 1, 2, 3, 4, 5, 6)
            )
        except Exception as e:
            logger.error(f"Error setting up job queue: {e}")
    
    print("🤖 Бот запущен с полным функционалом уведомлений...")
    application.run_polling()

if __name__ == "__main__":
    main()
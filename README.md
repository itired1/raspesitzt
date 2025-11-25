```markdown
# 🎓 Schedule Bot | Бот Расписания

<div align="center">

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Telegram](https://img.shields.io/badge/Telegram-Bot-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**Smart schedule management bot for educational institutions**  
**Умный бот для управления расписанием учебных заведений**

[Features • Возможности](#-features--возможности) • 
[Installation • Установка](#-installation--установка) • 
[Usage • Использование](#-usage--использование) • 
[Admin • Администрирование](#-admin--администрирование)

</div>

---

## 🌟 Features • Возможности

### 🤖 Core Features • Основные функции

| Feature • Функция | Description • Описание |
|------------------|-----------------------|
| **📅 Schedule Viewing**<br>**Просмотр расписания** | View schedule for today, tomorrow or custom date<br>Просмотр расписания на сегодня, завтра или выбранную дату |
| **🎨 Clean Schedule Images**<br>**Красивые изображения** | Professional schedule cards with clean design<br>Профессиональные карточки расписания в чистом дизайне |
| **🔔 Smart Notifications**<br>**Умные уведомления** | Automatic tomorrow schedule alerts with customization<br>Автоуведомления о расписании на завтра с настройками |
| **👥 Classmates Directory**<br>**Список одногруппников** | Group member contacts with username integration<br>Контакты одногруппников с интеграцией username |
| **🛠 Support System**<br>**Система поддержки** | Ticket-based technical support with admin responses<br>Тикет-система техподдержки с ответами администраторов |

### ⚙️ Admin Panel • Админ-панель

| Admin Feature • Функция админа | Description • Описание |
|-------------------------------|-----------------------|
| **📊 Dashboard & Analytics**<br>**Дашборд и аналитика** | Comprehensive user activity and usage statistics<br>Полная статистика активности и использования |
| **🕒 Notification Management**<br>**Управление уведомлениями** | Custom timing and selective group notifications<br>Настройка времени и выборочные уведомления по группам |
| **📝 Content Management**<br>**Управление контентом** | Add groups, manage schedules and templates<br>Добавление групп, управление расписанием и шаблонами |
| **🎫 Support Management**<br>**Управление поддержкой** | Complete ticket system administration<br>Полное администрирование тикет-системы |

---

## 🚀 Installation • Установка

### Prerequisites • Требования

```bash
# Install Python 3.8+ 
# Установите Python 3.8+
python --version

# Install required packages
# Установите необходимые пакеты
pip install python-telegram-bot pillow pandas matplotlib
```

### Configuration • Настройка

1. **Get Bot Token • Получите токен бота**
   ```python
   # Create a new bot with @BotFather on Telegram
   # Создайте нового бота через @BotFather в Telegram
   BOT_TOKEN = "your_bot_token_here"
   ```

2. **Set Admin ID • Установите ID администратора**
   ```python
   # Add your Telegram user ID
   # Добавьте ваш ID пользователя Telegram
   ADMIN_IDS = [123456789]
   ```

3. **Run the Bot • Запустите бота**
   ```bash
   python main.py
   ```

---

## 📖 Usage • Использование

### For Users • Для пользователей

1. **Start the bot • Запустите бота**
   ```
   /start
   ```

2. **Select your group • Выберите вашу группу**
   - Choose from available groups list
   - Выберите из списка доступных групп

3. **Access features • Используйте функции**
   - 📅 Get schedule • Получить расписание
   - 🔔 Manage notifications • Управлять уведомлениями
   - 👥 View classmates • Посмотреть одногруппников
   - ❓ Get help • Получить помощь

### For Admins • Для администраторов

1. **Access admin panel • Откройте админ-панель**
   - Click "Admin Panel" in main menu
   - Нажмите "Админ-панель" в главном меню

2. **Manage content • Управляйте контентом**
   - Add new groups and schedules • Добавляйте группы и расписания
   - Configure notification settings • Настраивайте уведомления
   - Monitor statistics • Мониторьте статистику
   - Handle support tickets • Обрабатывайте тикеты поддержки

---

## 🔧 Admin Features • Администрирование

### Notification Settings • Настройки уведомлений

```python
# Default notification time • Время уведомлений по умолчанию
"notification_time": "18:00"

# Group-specific notifications • Уведомления для конкретных групп
"enabled_groups": ["GROUP_1", "GROUP_2"]

# User notification preferences • Предпочтения пользователей
"user_notifications": {"enabled": true}
```

### Schedule Management • Управление расписанием

- **Manual schedule addition • Ручное добавление расписания**
- **Automatic date detection • Автоматическое определение дат**
- **Bell schedule integration • Интеграция с расписанием звонков**
- **Multiple month support • Поддержка нескольких месяцев**

### User Management • Управление пользователями

- **Activity tracking • Отслеживание активности**
- **Group statistics • Статистика по группам**
- **Error monitoring • Мониторинг ошибок**
- **Popular features analytics • Аналитика популярных функций**

---

## 🏗 Project Structure • Структура проекта

```
schedule-bot/
├── 📁 data/
│   ├── groups.json          # Group list • Список групп
│   ├── schedule.json        # Schedule data • Данные расписания
│   ├── users.json           # User profiles • Профили пользователей
│   ├── settings.json        # Bot settings • Настройки бота
│   ├── templates.json       # Message templates • Шаблоны сообщений
│   └── statistics.json      # Usage statistics • Статистика использования
├── 🐍 main.py              # Main bot file • Основной файл бота
└── 📄 requirements.txt     # Dependencies • Зависимости
```

---

## 🎯 Key Features Detail • Детали основных функций

### 🔔 Smart Notifications • Умные уведомления

- **Automatic daily alerts • Ежедневные автоматические уведомления**
- **Customizable timing • Настраиваемое время отправки**
- **Group-specific targeting • Целевая отправка по группам**
- **User preference respect • Учет предпочтений пользователей**
- **Smart filtering • Умная фильтрация**

### 📊 Analytics • Аналитика

- **User activity tracking • Отслеживание активности**
- **Popular feature statistics • Статистика популярных функций**
- **Group usage analytics • Аналитика использования по группам**
- **Error monitoring • Мониторинг ошибок**
- **Attendance statistics • Статистика посещаемости**

### 🛠 Support System • Система поддержки

- **Ticket management • Управление тикетами**
- **Admin responses • Ответы администраторов**
- **Status tracking • Отслеживание статусов**
- **User notification • Уведомления пользователей**
- **History tracking • Отслеживание истории**

### 🎨 Schedule Generation • Генерация расписания

- **Clean image design • Чистый дизайн изображений**
- **Professional layout • Профессиональная верстка**
- **Automatic formatting • Автоматическое форматирование**
- **Multiple column support • Поддержка нескольких колонок**

---

## 🤝 Contributing • Участие в разработке

We welcome contributions! • Мы приветствуем участие в разработке!

1. **Fork the project • Сделайте форк проекта**
2. **Create a feature branch • Создайте ветку для функции**
   ```bash
   git checkout -b feature/amazing-feature
   ```
3. **Commit your changes • Зафиксируйте изменения**
   ```bash
   git commit -m 'Add amazing feature'
   ```
4. **Push to the branch • Отправьте в ветку**
   ```bash
   git push origin feature/amazing-feature
   ```
5. **Open a Pull Request • Откройте Pull Request**

---

## 📄 License • Лицензия

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.  
Этот проект лицензирован под MIT License - подробности в файле [LICENSE](LICENSE).

---

## 📞 Support • Поддержка

### Technical Issues • Технические проблемы

- **Create an issue in GitHub • Создайте issue на GitHub**
- **Contact via bot support • Обратитесь через поддержку бота**
- **Check existing issues • Проверьте существующие issues**

### Feature Requests • Запросы функций

- **Suggest new features • Предложите новые функции**
- **Report bugs • Сообщите об ошибках**
- **Request improvements • Запросите улучшения**

### Documentation • Документация

- **Code comments • Комментарии в коде**
- **Inline documentation • Встроенная документация**
- **Example configurations • Примеры конфигураций**

---

<div align="center">

**Made with ❤️ for educational communities**  
**Сделано с ❤️ для учебных сообществ**

[⬆ Back to top • Наверх](#-schedule-bot--бот-расписания)

</div>
```

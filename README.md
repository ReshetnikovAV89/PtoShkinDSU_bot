PtoShkinDSU_bot
(учебный PET-проект)

Telegram-бот на python-telegram-bot v20+.

Фичи:

FAQ из Excel (data/faq.xlsx), поддержка «особых» вкладок A/B/C/D.
Публикации в группу/канал: /post (с ожиданием вложений до 3 минут), /send, /publish.
Темы (forum): /bindhere, /settopic <thread_id|0>.
Предложения от пользователей с логом и уведомлениями.
Аудит действий: CSV + (опц.) уведомления в чат.
/deleteme — удаление сообщений в группе (с проверкой прав и возраста сообщения).
Работает как в режиме polling (локально), так и webhook (на хостинге).

Требования
Python 3.10+

Библиотеки:
python-telegram-bot>=20,<21
pandas
openpyxl
python-dotenv


Включённый Privacy Mode: OFF у бота (по желанию для групповых команд).
Репозиторий:

https://github.com/ReshetnikovAV89/PtoShkinDSU_bot.git
  
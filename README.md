# Certificates Generator

API для генерации сертификатов на основе CSV файлов.

## Локальный запуск

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Установите LibreOffice (для конвертации DOCX в PDF):
   - Windows: Скачайте с https://www.libreoffice.org/
   - Linux: `sudo apt-get install libreoffice-writer`
   - macOS: `brew install libreoffice`

3. Запустите сервер:
```bash
python test_local.py
```

Или напрямую:
```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

- `GET /` - Корневая страница с информацией об API
- `GET /health` - Проверка здоровья сервиса
- `POST /generate` - Генерация сертификатов

## Использование

1. Подготовьте CSV файл с колонками:
   - `Имя` / `имя` - имя участника
   - `Фамилия` / `фамилия` - фамилия участника
   - `Название тренинга` / `название тренинга` - название курса
   - `Даты` / `даты` - даты проведения (формат: DD.MM.YY или DD.MM.YYYY)
   - `ID` / `id` - уникальный идентификатор сертификата

2. Отправьте POST запрос на `/generate` с параметрами:
   - `csv_file` - ваш CSV файл
   - `mode` - "print" или "online"

3. Получите ZIP архив с PDF сертификатами

## Деплой на Render

Приложение автоматически настроено для деплоя на Render.com. Просто подключите репозиторий к Render и используйте следующие настройки:

- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `python -m uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## Структура проекта

```
certificates-generator/
├── app/
│   ├── __init__.py
│   └── main.py          # Основной код приложения
├── Templates/           # Шаблоны сертификатов
├── requirements.txt     # Python зависимости
├── Dockerfile          # Docker конфигурация
├── render.yaml         # Render конфигурация
└── test_local.py       # Локальный тест
```

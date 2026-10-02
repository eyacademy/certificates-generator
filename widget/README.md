# Виджет генератора сертификатов

`ey-cert-widget.html` — готовый сниппет (HTML + CSS + JS) для вставки на страницу сайта.
Копируется целиком в HTML-блок страницы.

## Что нового

Добавлены версии шаблонов с часами CPD/CPE:
- **CPD|CPE (eng)**, значение `cpd_cpe_eng` (папка `Templates_CPD_CPE_eng`)
- **CPD|CPE (ru)**, значение `cpd_cpe_ru` (папка `Templates_CPD_CPE_ru`)

Также добавлена поддержка колонки `acad/CPD/CPE` в Excel/CSV, в которой указываются 3 числа через запятую (например, `8,12,3`):
- 1-е число: `{{акад_время}}`
- 2-е число: `{{CPD}}`
- 3-е число: `{{CPE}}`

| Кнопка в виджете | value | Папка шаблонов |
|---|---|---|
| RU | `ru` | `Templates` |
| AZ | `az` | `Templates_AZ` |
| RU (текст) | `ru_text` | `Templates_RU_Text` |
| CPD\|CPE (eng) | `cpd_cpe_eng` | `Templates_CPD_CPE_eng` |
| CPD\|CPE (ru) | `cpd_cpe_ru` | `Templates_CPD_CPE_ru` |

## Настройка

Адрес API задаётся в первой строке скрипта:

```js
const API_BASE = 'https://certificates-generator-ard1.onrender.com';
```

Для локальной проверки: `const API_BASE = 'http://127.0.0.1:8000';`

## Важно

Опция «RU (текст)» заработает только после деплоя обновлённого `app/main.py`.
На старом бэкенде неизвестный регион молча откатывается на `ru` (обычные `Templates`).

Проверить, что сервер знает про новый регион:

```
GET /check-templates?region=ru_text
```

Должно вернуть `templates_dir_exists: true` и 12 доступных шаблонов без пропусков.

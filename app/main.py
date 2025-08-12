import io
import os
import csv
import re
import zipfile
import tempfile
import subprocess
import logging
import shutil
try:
    from openpyxl import load_workbook
    HAS_XLSX = True
except Exception:
    HAS_XLSX = False
from fastapi.responses import FileResponse, HTMLResponse, Response
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse
import asyncio
from concurrent.futures import ThreadPoolExecutor

from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import json
import time
from dataclasses import dataclass, field
from docxtpl import DocxTemplate

app = FastAPI(title="Certificates Generator")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("certefikati")

@app.head("/")
def head_root():
    return Response(status_code=200)

@app.get("/")
def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/ui")

@app.get("/ui")
def ui():
    html_content = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Генератор сертификатов</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 50px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            text-align: center;
            margin-bottom: 30px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
            color: #555;
        }
        input[type="file"] {
            width: 100%;
            padding: 10px;
            border: 2px dashed #ddd;
            border-radius: 5px;
            background: #fafafa;
        }
        .radio-group {
            display: flex;
            gap: 20px;
            margin-top: 10px;
        }
        .radio-item {
            display: flex;
            align-items: center;
            gap: 5px;
        }
        input[type="radio"] {
            margin: 0;
        }
        button {
            background: #007bff;
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 16px;
            width: 100%;
            margin-top: 20px;
        }
        button:hover {
            background: #0056b3;
        }
        button:disabled {
            background: #ccc;
            cursor: not-allowed;
        }
        .status {
            margin-top: 20px;
            padding: 10px;
            border-radius: 5px;
            display: none;
        }
        .status.success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .status.error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        .progress {
            width: 100%;
            height: 20px;
            background: #f0f0f0;
            border-radius: 10px;
            overflow: hidden;
            margin-top: 10px;
            display: none;
        }
        .progress-bar {
            height: 100%;
            background: #007bff;
            width: 0%;
            transition: width 0.3s;
        }
        .progress-info {
            margin-top: 6px;
            color: #555;
            font-size: 14px;
            display: none;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Генератор сертификатов</h1>
        
        <form id="certificateForm">
            <div class="form-group">
                <label for="csvFile">Выберите файл (CSV или Excel):</label>
                <input type="file" id="csvFile" name="csv_file" accept=".csv,.xlsx,.xls" required>
                <div id="fileStatus">Файл не выбран</div>
            </div>
            
            <div class="form-group">
                <label>Тип сертификата:</label>
                <div class="radio-group">
                    <div class="radio-item">
                        <input type="radio" id="print" name="mode" value="print" checked>
                        <label for="print">Печать</label>
                    </div>
                    <div class="radio-item">
                        <input type="radio" id="online" name="mode" value="online">
                        <label for="online">Онлайн</label>
                    </div>
                </div>
            </div>
            
            <button type="submit" id="generateBtn">Сгенерировать</button>
        </form>
        
    <div class="progress" id="progress">
            <div class="progress-bar" id="progressBar"></div>
        </div>
    <div class="progress-info" id="progressInfo"></div>
        
        <div class="status" id="status"></div>
    </div>

    <script>
        const form = document.getElementById('certificateForm');
        const fileInput = document.getElementById('csvFile');
        const fileStatus = document.getElementById('fileStatus');
        const generateBtn = document.getElementById('generateBtn');
        const progress = document.getElementById('progress');
        const progressBar = document.getElementById('progressBar');
    const progressInfo = document.getElementById('progressInfo');
        const status = document.getElementById('status');

        fileInput.addEventListener('change', function() {
            if (this.files.length > 0) {
                fileStatus.textContent = `Выбран файл: ${this.files[0].name}`;
                fileStatus.style.color = '#28a745';
            } else {
                fileStatus.textContent = 'Файл не выбран';
                fileStatus.style.color = '#dc3545';
            }
        });

        form.addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const formData = new FormData();
            const file = fileInput.files[0];
            const mode = document.querySelector('input[name="mode"]:checked').value;
            
            if (!file) {
                showStatus('Пожалуйста, выберите CSV файл', 'error');
                return;
            }
            
            formData.append('csv_file', file);
            formData.append('mode', mode);
            // Генерируем jobId для трекинга прогресса через SSE
            const jobId = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : ('job-' + Date.now() + '-' + Math.random().toString(16).slice(2));
            formData.append('job_id', jobId);
            
            generateBtn.disabled = true;
            generateBtn.textContent = 'Генерация...';
            progress.style.display = 'block';
            progressInfo.style.display = 'block';
            status.style.display = 'none';
            
            // Подключаемся к SSE для живого прогресса обработки
            const es = new EventSource(`/progress/${jobId}`);
            let sseStage = 'init';
            es.onmessage = (ev) => {
                try {
                    const data = JSON.parse(ev.data || '{}');
                    sseStage = data.stage || sseStage;
                    if (sseStage === 'processing' || sseStage === 'zipping') {
                        updateProgress(data.percent || 0);
                        setInfo(`${data.message || ''} (${data.processed || 0}/${data.total || 0})`);
                    } else if (sseStage === 'uploading') {
                        setInfo('Загрузка файла...');
                    } else if (sseStage === 'done') {
                        setInfo('Подготовка к скачиванию...');
                    } else if (sseStage === 'error') {
                        setInfo(data.message || 'Ошибка');
                    }
                    if (sseStage === 'done' || sseStage === 'error') {
                        es.close();
                    }
                } catch (e) {
                    // ignore
                }
            };
            es.onerror = () => {
                // Не роняем UX, просто закрываем, если что
                es.close();
            };

            // Отправляем XHR, чтобы показать реальную загрузку и скачивание
            try {
                const xhr = new XMLHttpRequest();
                xhr.open('POST', '/generate');
                xhr.responseType = 'blob';

                // Прогресс загрузки файла на сервер
                xhr.upload.onprogress = (e) => {
                    if (e.lengthComputable) {
                        const pct = Math.round((e.loaded / e.total) * 100);
                        updateProgress(Math.min(pct, 99));
                        setInfo(`Загрузка файла: ${pct}%`);
                    } else {
                        setInfo('Загрузка файла...');
                    }
                };

                // Прогресс скачивания ответа (ZIP)
                xhr.onprogress = (e) => {
                    if (sseStage === 'done' || sseStage === 'zipping') {
                        if (e.lengthComputable) {
                            const pct = Math.round((e.loaded / e.total) * 100);
                            updateProgress(pct);
                            setInfo(`Скачивание архива: ${pct}%`);
                        } else {
                            setInfo('Скачивание архива...');
                        }
                    }
                };

                xhr.onload = () => {
                    const contentType = xhr.getResponseHeader('Content-Type') || '';
                    if (xhr.status !== 200) {
                        const reader = new FileReader();
                        reader.onload = () => {
                            showStatus(`Ошибка: ${reader.result}`, 'error');
                        };
                        reader.readAsText(xhr.response);
                        cleanup();
                        return;
                    }
                    if (!contentType.includes('application/zip')) {
                        const reader = new FileReader();
                        reader.onload = () => {
                            showStatus(`Получен неожиданный ответ (не ZIP). Тип: ${contentType}. Текст: ${String(reader.result).slice(0,200)}...`, 'error');
                        };
                        reader.readAsText(xhr.response);
                        cleanup();
                        return;
                    }
                    const blob = xhr.response;
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = 'certificates.zip';
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                    updateProgress(100);
                    setInfo('Готово');
                    showStatus('Сертификаты успешно сгенерированы!', 'success');
                    cleanup();
                };
                xhr.onerror = () => {
                    showStatus('Сетевая ошибка при запросе', 'error');
                    cleanup();
                };
                xhr.send(formData);
            } catch (error) {
                showStatus(`Ошибка сети: ${error.message}`, 'error');
                cleanup();
            }

            function updateProgress(pct) {
                progressBar.style.width = (pct || 0) + '%';
            }
            function setInfo(text) {
                progressInfo.textContent = text || '';
            }
            function cleanup() {
                generateBtn.disabled = false;
                generateBtn.textContent = 'Сгенерировать';
                // Оставим прогресс видимым кратко, затем спрячем
                setTimeout(() => {
                    progress.style.display = 'none';
                    progressInfo.style.display = 'none';
                    updateProgress(0);
                    setInfo('');
                }, 800);
            }
        });

        function showStatus(message, type) {
            status.textContent = message;
            status.className = `status ${type}`;
            status.style.display = 'block';
        }

    // Убрали фейковую анимацию: прогресс теперь реальный (SSE + XHR)
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Используем существующую папку с шаблонами на уровне репозитория
TEMPLATES_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "Templates"))
PDF_TEMPLATES_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "TemplatesPDF"))

# Шрифты: пытаемся использовать кастомный EYInterstate из папок fonts/ или @fonts/, иначе DejaVuSans
FONTS_DIR_CANDIDATES = [
    os.path.abspath(os.path.join(BASE_DIR, "..", "fonts")),
    os.path.abspath(os.path.join(BASE_DIR, "..", "@fonts")),
]
FONTS_DIR = next((p for p in FONTS_DIR_CANDIDATES if os.path.isdir(p)), FONTS_DIR_CANDIDATES[0])
EY_FONT_CANDIDATES = [
    os.path.join(FONTS_DIR, "EYINTERSTATE-REGULAR.OTF"),
    os.path.join(FONTS_DIR, "EYINTERSTATE-LIGHT.OTF"),
    os.path.join(FONTS_DIR, "EYINTERSTATE-BOLD.OTF"),
]

FONT_NAME = "EYInterstate"
registered = False
try:
    # Регистрируем Regular. Дополнительно пробуем Bold/Light, если есть
    regular = os.path.join(FONTS_DIR, "EYINTERSTATE-REGULAR.OTF")
    if os.path.exists(regular):
        pdfmetrics.registerFont(TTFont(FONT_NAME, regular))
        registered = True
        # Не обязательно, но полезно, если захотим жирный/светлый стиль
        for extra_name in [
            ("EYInterstate-Bold", os.path.join(FONTS_DIR, "EYINTERSTATE-BOLD.OTF")),
            ("EYInterstate-Light", os.path.join(FONTS_DIR, "EYINTERSTATE-LIGHT.OTF")),
        ]:
            if os.path.exists(extra_name[1]):
                try:
                    pdfmetrics.registerFont(TTFont(extra_name[0], extra_name[1]))
                except Exception:
                    pass
except Exception:
    registered = False

if not registered:
    # Фоллбек на системный DejaVuSans (ставится в Docker)
    SYSTEM_FONT_CANDIDATES = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
    ]
    FONT_NAME = "DejaVuSans"
    for p in SYSTEM_FONT_CANDIDATES:
        if os.path.exists(p):
            try:
                pdfmetrics.registerFont(TTFont(FONT_NAME, p))
                registered = True
                break
            except Exception:
                continue

if not registered:
    # Последний фоллбек (без кириллицы)
    FONT_NAME = "Helvetica"


# Карта шаблонов DOCX (имена должны совпадать с файлами в Templates/)
DOCX_MAP: Dict[str, Dict[str, Dict[str, str]]] = {
    "print": {
        "duration_day": {
            "normal": "template_duration_day.docx",
            "small": "template_small_duration_day.docx",
        },
        "2day_2month": {
            "normal": "template2_2day_2month.docx",
            "small": "template2_small_2day_2month.docx",
        },
        "1day_1month": {
            "normal": "template3_1day_1month.docx",
            "small": "template3_small_1day_1month.docx",
        },
    },
    "online": {
        "duration_day": {
            "normal": "template-online_duration_day.docx",
            "small": "template-online_small_duration_day.docx",
        },
        "2day_2month": {
            "normal": "template-online2_2day_2month.docx",
            "small": "template-online2_small_2day_2month.docx",
        },
        "1day_1month": {
            "normal": "template-online3_1day_1month.docx",
            "small": "template-online3_small_1day_1month.docx",
        },
    },
}


# Кэш соответствий DOCX->PDF (генерация через LibreOffice при первом обращении)
DOCX_TO_PDF_CACHE: Dict[str, str] = {}


# Позиции и размеры текста в поинтах для A4 landscape (примерные, подстройте под макеты)
LAYOUT = {
    "normal": {
        "name": {"x": 421, "y": 330, "align": "center", "size": 28},
        "course": {"x": 421, "y": 380, "align": "center", "size": 16, "max_width": 620},
        "dates": {"x": 421, "y": 290, "align": "center", "size": 14},
        # Смещаем ID ближе к подписи внизу сертификата
        "id": {"x": 540, "y": 45, "align": "left", "size": 10},
    },
    "small": {
        "name": {"x": 421, "y": 320, "align": "center", "size": 22},
        "course": {"x": 421, "y": 380, "align": "center", "size": 15, "max_width": 700},
        "dates": {"x": 421, "y": 285, "align": "center", "size": 13},
        "id": {"x": 540, "y": 43, "align": "left", "size": 10},
    },
    "name_max_width": {"normal": 400, "small": 520},
}

# Отдельная разметка для online (портрет). Значения подобраны ориентировочно; подгоняются по скриншоту.
ONLINE_LAYOUT = {
    # Подгонка по вашему скриншоту (A4 portrait ~595x842):
    # - Имя: правый блок, по правому краю; жирный; чуть ниже середины
    # - Курс: над именем, небольшим кеглем
    # - Даты: правее и ниже имени
    # - ID: снизу слева у подписи ID Number
    "normal": {
        "name": {"x": 720, "y": 560, "align": "right", "size": 18, "font": "EYInterstate-Bold"},
        "course": {"x": 720, "y": 590, "align": "right", "size": 12, "max_width": 320},
        "dates": {"x": 720, "y": 540, "align": "right", "size": 11},
        "id": {"x": 270, "y": 105, "align": "left", "size": 9},
    },
    "small": {
        "name": {"x": 720, "y": 560, "align": "right", "size": 17, "font": "EYInterstate-Bold"},
        "course": {"x": 720, "y": 590, "align": "right", "size": 11, "max_width": 320},
        "dates": {"x": 720, "y": 540, "align": "right", "size": 10},
        "id": {"x": 270, "y": 103, "align": "left", "size": 9},
    },
}

# Базовые размеры для масштабирования координат
LAYOUT_BASE = {
    "print": {"w": 842.0, "h": 595.0},   # A4 landscape
    "online": {"w": 595.0, "h": 842.0},  # A4 portrait (ожидаем для online)
}

MONTH_GEN = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


# ------------------------------------------------------------
# Нормализация заголовков и поиск значений в строке
# Поддерживаем варианты вида "Имя/Name", "Фамилия/Surname", и т.д.
# ------------------------------------------------------------

def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("ё", "е")
    return s


# Карта алиасов: каноническое имя -> набор допустимых заголовков (нормализованных)
KEY_ALIASES = {
    "first_name": {
        "имя", "name", "first name", "first_name", "имя/name", "name/имя",
    },
    "last_name": {
        "фамилия", "surname", "last name", "last_name", "фамилия/surname", "surname/фамилия",
    },
    "course": {
        "название тренинга", "название", "course", "course name", "course_name",
        "название тренинга/название", "название/название тренинга",
    },
    "dates": {
        "даты", "дата", "dates", "date", "даты/дата", "date/dates",
    },
    "id": {
        "id", "id/id", "идентификатор", "certificate id", "сертификат id",
    },
    "city": {
        "город", "city", "город/city", "city/город",
    },
    "country": {
        "страна", "country", "страна/country", "country/страна",
    },
}


def _build_row_with_normalized_keys(row: Dict[str, str]) -> Dict[str, str]:
    """Возвращает копию строки с нормализованными ключами (и оригинальными тоже).
    Если встретили заголовок с косой чертой ("Имя/Name"), добавляем обе части как ключи.
    """
    out: Dict[str, str] = {}
    for k, v in (row or {}).items():
        if k is None:
            continue
        val = (v or "").strip()
        # Оригинальный ключ
        out[k] = val
        # Нормализованный ключ
        nk = _norm_key(str(k))
        out[nk] = val
        # Разбивка по "/" — часто встречается в шаблонах
        if "/" in str(k):
            parts = [p.strip() for p in str(k).split("/") if p.strip()]
            for p in parts:
                out[p] = val
                out[_norm_key(p)] = val
    return out


def _get_field(row: Dict[str, str], canonical: str) -> str:
    """Ищем значение по каноническому имени, используя KEY_ALIASES.
    Учитываем исходные, нормализованные и разрезанные по '/'
    заголовки.
    """
    r = _build_row_with_normalized_keys(row)
    aliases = KEY_ALIASES.get(canonical, set())
    for key in list(r.keys()):
        nk = _norm_key(str(key))
        if nk in aliases:
            return (r[key] or "").strip()
    # Последняя попытка: прямой поиск по каноническому имени
    return (r.get(canonical) or r.get(_norm_key(canonical)) or "").strip()


def normalize_online(v: str) -> bool:
    v = (v or "").strip().lower()
    if v in {"да", "yes", "true", "1", "y", "онлайн", "online"}:
        return True
    if v in {"нет", "no", "false", "0", "n", "оффлайн", "offline", "off"}:
        return False
    return False


def detect_delimiter(text: str) -> str:
    sample = text[:4096]
    # Простая эвристика по количеству разделителей
    candidates = [',', ';', '\t']
    counts = {c: sample.count(c) for c in candidates}
    delim = max(counts, key=counts.get)
    # Если все нули, оставим запятую
    return delim if counts[delim] > 0 else ','


def normalize_csv_and_get_delimiter(text: str) -> Tuple[str, str]:
    """Возвращает нормализованный CSV-текст (с корректной строкой заголовка)
    и детектированный разделитель. Поддерживает запятую, точку с запятой и табы.
    Убирает технические заголовки вида Column1, Column2.
    """
    # Унификация перевода строк
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    delim = detect_delimiter(text)

    reader = csv.reader(io.StringIO(text), delimiter=delim)
    rows = [list(map(lambda s: (s or '').strip(), r)) for r in reader]
    rows = [r for r in rows if any(cell != '' for cell in r)]
    if not rows:
        return '', delim

    def is_generic_header(cells: List[str]) -> bool:
        if not cells:
            return False
        return all(cell.lower().startswith('column') or cell.lower().startswith('unnamed') for cell in cells)

    # Если первая строка техническая, используем вторую как заголовок
    start_idx = 0
    header = rows[0]
    if is_generic_header(header) and len(rows) >= 2:
        start_idx = 1
        header = rows[1]
        start_idx += 1
    else:
        start_idx = 1

    # Удаляем BOM у первой ячейки заголовка
    if header:
        header[0] = header[0].lstrip('\ufeff')

    # Собираем обратно нормализованный текст
    norm_rows = [header] + rows[start_idx:]
    normalized_text = '\n'.join(delim.join(r) for r in norm_rows)
    return normalized_text, delim


def parse_dates(s: str) -> Dict[str, Optional[int]]:
    s = (s or "").strip()
    
    # Приоритет: числовой формат DD.MM.YY или DD.MM.YYYY
    # Паттерн для дат: DD.MM.YY или DD.MM.YYYY
    date_pattern = r"\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})\b"
    dates = list(re.finditer(date_pattern, s))
    
    if len(dates) >= 2:
        # Два дня: DD.MM.YY - DD.MM.YY
        d1, m1, y1 = int(dates[0].group(1)), int(dates[0].group(2)), int(dates[0].group(3))
        d2, m2, y2 = int(dates[1].group(1)), int(dates[1].group(2)), int(dates[1].group(3))
        
        # Нормализация года (YY -> YYYY)
        if y1 < 100:
            y1 += 2000
        if y2 < 100:
            y2 += 2000
        
        return {"d1": d1, "m1": m1, "d2": d2, "m2": m2, "y": y1}
    
    elif len(dates) == 1:
        # Один день: DD.MM.YY
        d1, m1, y1 = int(dates[0].group(1)), int(dates[0].group(2)), int(dates[0].group(3))
        
        # Нормализация года (YY -> YYYY)
        if y1 < 100:
            y1 += 2000
        
        return {"d1": d1, "m1": m1, "d2": None, "m2": None, "y": y1}
    
    # Фоллбек: старый парсер для текстовых дат
    s_lower = s.lower()
    m_year = re.search(r"\b(20\d{2})\b", s)
    if m_year:
        y = int(m_year.group(1))
    else:
        y = __import__("datetime").datetime.now().year

    months_tokens = [
        ("январ", 1), ("феврал", 2), ("март", 3), ("апрел", 4),
        ("ма", 5), ("июн", 6), ("июл", 7), ("август", 8),
        ("сентябр", 9), ("октябр", 10), ("ноябр", 11), ("декабр", 12),
    ]

    def detect_month(token: str) -> Optional[int]:
        if token.isdigit() and 1 <= int(token) <= 12:
            return int(token)
        for pref, m in months_tokens:
            if token.startswith(pref):
                return m
        return None

    tokens = re.sub(r"[,;]+", " ", s_lower)
    tokens = re.sub(r"\s+", " ", tokens).strip().split(" ")
    days = [int(t) for t in tokens if t.isdigit() and 1 <= int(t) <= 31]
    month_idx = [(i, detect_month(t)) for i, t in enumerate(tokens) if detect_month(t)]

    rng = re.search(r"\b(\d{1,2})\s*[-–]\s*(\d{1,2})\b", s_lower)
    if rng and month_idx:
        return {
            "d1": int(rng.group(1)), "m1": month_idx[0][1],
            "d2": int(rng.group(2)), "m2": month_idx[0][1], "y": y,
        }

    if len(days) >= 2 and len(month_idx) >= 2:
        return {"d1": days[0], "m1": month_idx[0][1], "d2": days[1], "m2": month_idx[1][1], "y": y}
    if len(days) >= 2 and len(month_idx) == 1:
        return {"d1": days[0], "m1": month_idx[0][1], "d2": days[1], "m2": month_idx[0][1], "y": y}
    if len(days) >= 1 and len(month_idx) >= 1:
        return {"d1": days[0], "m1": month_idx[0][1], "d2": None, "m2": None, "y": y}

    return {"d1": days[0] if days else 1, "m1": month_idx[0][1] if month_idx else 1, "d2": None, "m2": None, "y": y}


def format_dates_for_jinja(p: Dict[str, Optional[int]]) -> Dict[str, str]:
    """Форматирует даты для Jinja-шаблонов"""
    d1, m1, d2, m2, y = p["d1"], p["m1"], p["d2"], p["m2"], p["y"]
    
    # Базовые переменные
    result = {
        "Имя": "",  # Будет заполнено позже
        "Фамилия": "",  # Будет заполнено позже
        "Тренинг": "",  # Будет заполнено позже
        "Год": str(y),
        "Город": "Москва",  # По умолчанию, может быть перезаписан из CSV
    }
    
    if d1 and d2:
        if m1 == m2:
            # 2 дня в одном месяце: 10.01.25 - 17.01.25
            result.update({
                "Дата1": str(d1),
                "Дата2": str(d2),
                "Месяц1": MONTH_GEN[m1],
                "Месяц2": MONTH_GEN[m2],
            })
        else:
            # 2 дня в разных месяцах: 10.01.25 - 17.02.25
            result.update({
                "Дата1": str(d1),
                "Дата2": str(d2),
                "Месяц1": MONTH_GEN[m1],
                "Месяц2": MONTH_GEN[m2],
            })
    else:
        # 1 день: 01.06.25
        result.update({
            "Дата1": str(d1),
            "Месяц1": MONTH_GEN[m1],
        })
    
    return result


def pick_kind(p: Dict[str, Optional[int]]) -> str:
    d1, m1, d2, m2 = p["d1"], p["m1"], p["d2"], p["m2"]
    if d1 and d2:
        if m1 == m2:
            return "duration_day"  # 2 дня в одном месяце: 10.01.25 - 17.01.25
        return "2day_2month"       # 2 дня в разных месяцах: 10.01.25 - 17.02.25
    return "1day_1month"           # 1 день: 01.06.25


def string_width_pt(text: str, size: int) -> float:
    try:
        return pdfmetrics.stringWidth(text, FONT_NAME, size)
    except Exception:
        # если нет зарегистрированного шрифта — приблизительно
        return size * max(1, len(text)) * 0.5


def need_small_variant(full_name: str) -> bool:
    w = string_width_pt(full_name, LAYOUT["normal"]["name"]["size"])
    return w > LAYOUT["name_max_width"]["normal"]


def fit_font_size_to_width(text: str, base_size: int, max_width: Optional[int]) -> int:
    if not max_width:
        return base_size
    size = base_size
    # Гарантируем вменяемую нижнюю границу размера шрифта
    while size > 8 and string_width_pt(text, size) > max_width:
        size -= 1
    return max(size, 8)


def format_dates_line(p: Dict[str, Optional[int]], city: Optional[str] = None) -> str:
    d1, m1, d2, m2, y = p["d1"], p["m1"], p["d2"], p["m2"], p["y"]
    if d1 and d2:
        core = f"{d1} и {d2} {MONTH_GEN[m1]} {y}" if m1 == m2 else f"{d1} {MONTH_GEN[m1]} — {d2} {MONTH_GEN[m2]} {y}"
    else:
        core = f"{d1} {MONTH_GEN[m1]} {y}"
    city = (city or "").strip()
    return f"{core}, {city}" if city else core


def draw_aligned_text(
    cnv: canvas.Canvas,
    x: float,
    y: float,
    text: str,
    align: str,
    size: int,
    font_name: Optional[str] = None,
    page_w: Optional[float] = None,
    page_h: Optional[float] = None,
):
    try:
        cnv.setFont(font_name or FONT_NAME, size)
    except Exception:
        cnv.setFont(FONT_NAME, size)
    width = string_width_pt(text, size)
    # Границы страницы с отступом 20pt
    margin = 20.0
    max_x = (page_w or 1e9) - margin if page_w else None
    min_x = margin
    min_y = margin
    max_y = (page_h or 1e9) - margin if page_h else None

    # Анкерная точка x (в зависимости от выравнивания) и итоговая позиция текста
    if align == "center":
        x_draw = x - width / 2
    elif align == "right":
        x_draw = x - width
    else:
        x_draw = x

    # Клампы, чтобы текст не выходил за границы страницы
    if page_w:
        x_draw = max(min_x, min(x_draw, max_x - width))
    if page_h:
        y = max(min_y, min(y, max_y))

    cnv.drawString(x_draw, y, text)


def build_overlay_pdf_bytes(
    first_name: str,
    last_name: str,
    course: str,
    parsed_dates: Dict[str, Optional[int]],
    cert_id: str,
    variant: str,
    page_w: float,
    page_h: float,
    group: str,
    city: Optional[str] = None,
    offsets: Optional[Dict[str, float]] = None,
    debug: bool = False,
) -> bytes:
    layout = (ONLINE_LAYOUT if group == "online" else LAYOUT)[variant]
    base = LAYOUT_BASE.get(group, LAYOUT_BASE["print"])  # по умолчанию как print
    sx = page_w / float(base["w"]) if base["w"] else 1.0
    sy = page_h / float(base["h"]) if base["h"] else 1.0
    buffer = io.BytesIO()
    cnv = canvas.Canvas(buffer, pagesize=(page_w, page_h))

    # Имя
    full_name = f"{first_name} {last_name}".strip()
    name_cfg = layout["name"]
    name_dx = (offsets or {}).get("name_dx", 0.0)
    name_dy = (offsets or {}).get("name_dy", 0.0)
    draw_aligned_text(
        cnv,
        name_cfg["x"] * sx + name_dx,
        name_cfg["y"] * sy + name_dy,
        full_name,
        name_cfg.get("align", "center"),
        name_cfg.get("size", 24),
        font_name=name_cfg.get("font"),
        page_w=page_w,
        page_h=page_h,
    )

    # Тренинг (подбираем размер под ширину)
    course_cfg = layout["course"]
    base_size = int(course_cfg.get("size", 16))
    max_width = int(course_cfg.get("max_width", 600))
    fitted_size = fit_font_size_to_width(course, base_size, max_width)
    course_dx = (offsets or {}).get("course_dx", 0.0)
    course_dy = (offsets or {}).get("course_dy", 0.0)
    draw_aligned_text(
        cnv,
        course_cfg["x"] * sx + course_dx,
        course_cfg["y"] * sy + course_dy,
        course,
        course_cfg.get("align", "center"),
        fitted_size,
        page_w=page_w,
        page_h=page_h,
    )

    # Даты
    dates_line = format_dates_line(parsed_dates, city)
    dates_cfg = layout["dates"]
    dates_dx = (offsets or {}).get("dates_dx", 0.0)
    dates_dy = (offsets or {}).get("dates_dy", 0.0)
    draw_aligned_text(
        cnv,
        dates_cfg["x"] * sx + dates_dx,
        dates_cfg["y"] * sy + dates_dy,
        dates_line,
        dates_cfg.get("align", "center"),
        dates_cfg.get("size", 14),
        page_w=page_w,
        page_h=page_h,
    )

    # ID
    id_cfg = layout["id"]
    id_dx = (offsets or {}).get("id_dx", 0.0)
    id_dy = (offsets or {}).get("id_dy", 0.0)
    draw_aligned_text(
        cnv,
        id_cfg["x"] * sx + id_dx,
        id_cfg["y"] * sy + id_dy,
        cert_id,
        id_cfg.get("align", "right"),
        id_cfg.get("size", 10),
        page_w=page_w,
        page_h=page_h,
    )

    if debug:
        # Отладочные кресты и подписи
        cnv.setStrokeColor(colors.red)
        cnv.setFillColor(colors.red)
        def cross(x, y, label):
            cnv.line(x-5, y, x+5, y)
            cnv.line(x, y-5, x, y+5)
            cnv.setFont(FONT_NAME, 8)
            cnv.drawString(x+6, y+6, label)
        cross(name_cfg["x"] * sx + name_dx, name_cfg["y"] * sy + name_dy, "name")
        cross(course_cfg["x"] * sx + course_dx, course_cfg["y"] * sy + course_dy, "course")
        cross(dates_cfg["x"] * sx + dates_dx, dates_cfg["y"] * sy + dates_dy, "dates")
        cross(id_cfg["x"] * sx + id_dx, id_cfg["y"] * sy + id_dy, "id")

    cnv.showPage()
    cnv.save()
    buffer.seek(0)
    return buffer.read()


def sanitize_filename(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", s).replace(" ", "_")[:100]


def docx_to_pdf_cached(docx_path: str) -> str:
    abs_docx = os.path.abspath(docx_path)
    if abs_docx in DOCX_TO_PDF_CACHE:
        logger.info(f"Using cached PDF for {abs_docx}")
        return DOCX_TO_PDF_CACHE[abs_docx]

    logger.info(f"Converting DOCX to PDF: {abs_docx}")
    out_dir = tempfile.mkdtemp(prefix="docx2pdf_")
    logger.info(f"Output directory: {out_dir}")
    
    # Попробуем разные пути к LibreOffice
    libreoffice_paths = [
        "soffice",  # если в PATH
        r"C:\Program Files\LibreOffice\program\soffice.exe",
        r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
        r"C:\LibreOffice\program\soffice.exe",
    ]
    
    cmd = None
    for path in libreoffice_paths:
        try:
            logger.info(f"Trying LibreOffice path: {path}")
            # Проверяем, существует ли файл
            if path == "soffice":
                # Проверяем команду в PATH
                test_cmd = ["soffice", "--version"]
                subprocess.run(test_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
                cmd = [path, "--headless", "--convert-to", "pdf", "--outdir", out_dir, abs_docx]
                logger.info("LibreOffice found in PATH")
                break
            elif os.path.exists(path):
                cmd = [path, "--headless", "--convert-to", "pdf", "--outdir", out_dir, abs_docx]
                logger.info(f"LibreOffice found at: {path}")
                break
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning(f"LibreOffice not found at {path}: {e}")
            continue
    
    if cmd is None:
        error_msg = "LibreOffice not found. Please install LibreOffice and add it to PATH, or update the paths in the code."
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    # Для конкурентных запусков LibreOffice нужен уникальный профиль пользователя,
    # иначе случаются конфликты блокировок. Также отключим восстановление/проверки блокировок.
    profile_dir = os.path.join(out_dir, "lo_profile")
    try:
        os.makedirs(profile_dir, exist_ok=True)
    except Exception:
        pass
    profile_url = "file:///" + os.path.abspath(profile_dir).replace("\\", "/").lstrip("/")

    cmd_with_profile = [
        cmd[0],
        "--headless",
        "--norestore",
        "--nolockcheck",
        f"-env:UserInstallation={profile_url}",
        "--convert-to", "pdf",
        "--outdir", out_dir,
        abs_docx,
    ]

    logger.info(f"Running command: {' '.join(cmd_with_profile)}")
    proc = subprocess.run(cmd_with_profile, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    pdf_path = os.path.join(
        out_dir, os.path.splitext(os.path.basename(abs_docx))[0] + ".pdf"
    )
    
    # Некоторые версии LibreOffice пишут предупреждения в stderr и/или возвращают ненулевой код,
    # но при этом корректно создают PDF. Считаем успехом факт наличия файла.
    if not os.path.exists(pdf_path):
        stderr_txt = proc.stderr.decode(errors='ignore') if proc.stderr else ''
        stdout_txt = proc.stdout.decode(errors='ignore') if proc.stdout else ''
        error_msg = f"LibreOffice convert failed: {stderr_txt or stdout_txt or 'unknown error'}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info(f"PDF successfully created: {pdf_path}")
    DOCX_TO_PDF_CACHE[abs_docx] = pdf_path
    return pdf_path


def render_docx_template(docx_path: str, context: Dict[str, str]) -> bytes:
    """Рендерит DOCX шаблон с Jinja-переменными"""
    doc = DocxTemplate(docx_path)
    doc.render(context)
    temp_docx = tempfile.NamedTemporaryFile(suffix=".docx", delete=False)
    doc.save(temp_docx.name)
    temp_docx.close()
    pdf_path = docx_to_pdf_cached(temp_docx.name)
    with open(pdf_path, 'rb') as f:
        pdf_bytes = f.read()
    os.unlink(temp_docx.name)
    return pdf_bytes


def build_blank_pdf_from_docx_template(docx_path: str) -> str:
    """Строит и кэширует PDF-фон из DOCX-шаблона с пустым контекстом,
    чтобы убрать Jinja-плейсхолдеры на фоне. Возвращает путь к PDF.
    """
    cache_key = f"blank::{os.path.abspath(docx_path)}"
    if cache_key in DOCX_TO_PDF_CACHE:
        return DOCX_TO_PDF_CACHE[cache_key]

    doc = DocxTemplate(docx_path)
    try:
        doc.render({})
    except Exception:
        # Если шаблон требует обязательные поля — подставляем пустые строки
        doc.render({
            "Имя": "", "Фамилия": "", "Тренинг": "",
            "Дата1": "", "Дата2": "", "Месяц1": "", "Месяц2": "",
            "Год": "", "Город": "", "Идентификатор": "",
        })
    temp_docx = tempfile.NamedTemporaryFile(suffix=".docx", delete=False)
    doc.save(temp_docx.name)
    temp_docx.close()
    pdf_path = docx_to_pdf_cached(temp_docx.name)
    # Копируем построенный PDF в постоянную папку TemplatesPDF/
    pdf_name = os.path.splitext(os.path.basename(docx_path))[0] + ".pdf"
    dest_pdf = os.path.join(PDF_TEMPLATES_DIR, pdf_name)
    try:
        os.makedirs(PDF_TEMPLATES_DIR, exist_ok=True)
        shutil.copy(pdf_path, dest_pdf)
    except Exception:
        dest_pdf = pdf_path
    DOCX_TO_PDF_CACHE[cache_key] = dest_pdf
    return dest_pdf


def get_template_pdf_path(docx_name: str) -> str:
    """Возвращает путь к PDF-фону для данного DOCX-шаблона.
    Сначала пытается использовать заранее подготовленный PDF из `TemplatesPDF/`.
    Если его нет, строит пустой PDF на основе DOCX и кэширует.
    """
    os.makedirs(PDF_TEMPLATES_DIR, exist_ok=True)
    pdf_name = os.path.splitext(docx_name)[0] + ".pdf"
    prebuilt_pdf = os.path.join(PDF_TEMPLATES_DIR, pdf_name)
    if os.path.exists(prebuilt_pdf):
        return prebuilt_pdf
    # Fallback: построить чистый PDF из DOCX
    docx_path = os.path.join(TEMPLATES_DIR, docx_name)
    return build_blank_pdf_from_docx_template(docx_path)


# Прогрев: при старте предстроим фоны только для print-шаблонов, online рендерим напрямую из DOCX
try:
    for grp, kinds in DOCX_MAP.items():
        for kind, variants in kinds.items():
            for variant, docx_name in variants.items():
                if grp != "print":
                    continue
                docx_path = os.path.join(TEMPLATES_DIR, docx_name)
                if os.path.exists(docx_path):
                    try:
                        _ = get_template_pdf_path(docx_name)
                        logger.info(f"Warmup built PDF for: {docx_name}")
                    except Exception as e:
                        logger.warning(f"Warmup failed for {docx_name}: {e}")
except Exception:
    pass


def merge_overlay(template_pdf_path: str, overlay_pdf_bytes: bytes) -> bytes:
    base_reader = PdfReader(template_pdf_path)
    over_reader = PdfReader(io.BytesIO(overlay_pdf_bytes))
    writer = PdfWriter()

    base_page = base_reader.pages[0]
    overlay_page = over_reader.pages[0]
    # Наложение текста поверх фона
    base_page.merge_page(overlay_page)
    writer.add_page(base_page)

    out = io.BytesIO()
    writer.write(out)
    out.seek(0)
    return out.read()


# --------------------------- Реальный прогресс (SSE) ---------------------------

@dataclass
class ProgressState:
    total: int = 0
    processed: int = 0
    stage: str = "init"   # init | uploading | processing | zipping | done | error
    message: str = ""
    errors: int = 0
    created: float = field(default_factory=lambda: time.time())
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)


PROGRESS: Dict[str, ProgressState] = {}


def get_progress(job_id: str) -> ProgressState:
    if job_id not in PROGRESS:
        PROGRESS[job_id] = ProgressState()
    return PROGRESS[job_id]


def snapshot(state: ProgressState) -> Dict[str, object]:
    percent = 0
    if state.total > 0:
        percent = int(state.processed * 100 / max(1, state.total))
    return {
        "total": state.total,
        "processed": state.processed,
        "percent": percent,
        "stage": state.stage,
        "message": state.message,
        "errors": state.errors,
    }


async def emit(job_id: str):
    state = get_progress(job_id)
    await state.queue.put("update")


@app.get("/progress/{job_id}")
async def progress_stream(job_id: str):
    async def event_gen():
        state = get_progress(job_id)
        # Отправим мгновенный снимок при подключении
        await emit(job_id)
    # heartbeat таймер реализован через timeout; отдельная переменная не нужна
        while True:
            try:
                _ = await asyncio.wait_for(state.queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                # heartbeat
                yield "event: ping\ndata: {}\n\n"
                continue
            data = json.dumps(snapshot(state), ensure_ascii=False)
            yield f"data: {data}\n\n"
            if state.stage in ("done", "error"):
                break

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@app.get("/health")
def health() -> PlainTextResponse:
    return PlainTextResponse("ok")

@app.get("/sample-excel")
def sample_excel():
    """Отдаёт пример Excel-шаблона Template_Certificates.xlsx из корня проекта."""
    root = os.path.abspath(os.path.join(BASE_DIR, ".."))
    sample_path = os.path.join(root, "Template_Certificates.xlsx")
    if not os.path.exists(sample_path):
        return PlainTextResponse("Template_Certificates.xlsx not found", status_code=404)
    return FileResponse(
        sample_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="Template_Certificates.xlsx",
    )

@app.get("/check-templates")
def check_templates():
    """Проверяет доступность всех шаблонов"""
    missing_templates = []
    available_templates = []
    
    for group in ["print", "online"]:
        for kind in ["duration_day", "2day_2month", "1day_1month"]:
            for variant in ["normal", "small"]:
                try:
                    docx_name = DOCX_MAP[group][kind][variant]
                    docx_path = os.path.join(TEMPLATES_DIR, docx_name)
                    if os.path.exists(docx_path):
                        available_templates.append(f"{group}/{kind}/{variant}: {docx_name}")
                    else:
                        missing_templates.append(f"{group}/{kind}/{variant}: {docx_name}")
                except KeyError:
                    missing_templates.append(f"{group}/{kind}/{variant}: NOT_FOUND_IN_MAP")
    
    return {
        "available_templates": available_templates,
        "missing_templates": missing_templates,
        "templates_dir": TEMPLATES_DIR,
        "templates_dir_exists": os.path.exists(TEMPLATES_DIR)
    }


@app.post("/generate")
async def generate(
    csv_file: UploadFile = File(...),
    mode: str = Form(...),  # print | online
    job_id: Optional[str] = Form(None),
):
    """Быстрый генератор с реальным прогрессом через SSE.
    Рендерим DOCX в PDF, собираем ZIP, отдаем одним ответом, а прогресс — через /progress/{job_id}.
    """
    try:
        logger.info(f"Starting certificate generation for mode: {mode}")

        # Привяжем состояние прогресса
        state = None
        if job_id:
            state = get_progress(job_id)
            state.stage = "uploading"
            state.message = "Загрузка файла"
            await emit(job_id)

        # Читаем загруженный файл в память
        data = await csv_file.read()

        filename = (csv_file.filename or '').lower()
        incoming_fields: List[str] = []
        rows_list: List[Dict[str, str]] = []
        txt = ''
        delim = ','

        if filename.endswith('.xlsx') or filename.endswith('.xlsm'):
            if not HAS_XLSX:
                raise RuntimeError('Поддержка Excel не установлена на сервере')
            wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
            ws = wb.active
            headers: List[str] = []
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                cells = [(c if c is not None else '') for c in row]
                if not any(str(c).strip() for c in cells):
                    continue
                if not headers:
                    candidate = [str(c).strip() for c in cells]
                    tech = all(h.lower().startswith('column') or h.lower().startswith('unnamed') or h == '' for h in candidate)
                    non_empty = [h for h in candidate if h]
                    if tech or len(non_empty) < 3:
                        continue
                    headers = candidate
                    incoming_fields = headers[:]
                    continue
                d: Dict[str, str] = {}
                for idx, h in enumerate(headers):
                    val = '' if idx >= len(cells) or cells[idx] is None else str(cells[idx])
                    d[h] = val
                rows_list.append(d)
            if not headers or not rows_list:
                raise ValueError('Excel: нераспознан заголовок или нет данных')
        else:
            raw_txt = data.decode('utf-8-sig', errors='ignore')
            txt, delim = normalize_csv_and_get_delimiter(raw_txt)
            if not txt:
                raise ValueError('CSV пустой или нераспознанный формат')
            dict_reader = csv.DictReader(io.StringIO(txt), delimiter=delim)
            incoming_fields = list(dict_reader.fieldnames or [])
            rows_list = [row for row in dict_reader]

        # Инициализируем прогресс обработки
        total = len(rows_list)
        if state:
            state.total = total
            state.processed = 0
            state.stage = "processing"
            state.message = "Обработка строк"
            await emit(job_id)

        mem_zip = io.BytesIO()
        processed_count = 0
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=4)
        tasks = []
        with zipfile.ZipFile(mem_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for row_num, row in enumerate(rows_list, 1):
                try:
                    is_online = (mode == "online")

                    # Унифицированное извлечение значений с учетом комбинированных заголовков
                    course = _get_field(row, "course")
                    dates_raw = _get_field(row, "dates")
                    first_name = _get_field(row, "first_name")
                    last_name = _get_field(row, "last_name")
                    cert_id = _get_field(row, "id")
                    city = _get_field(row, "city")
                    country = _get_field(row, "country")

                    if not (course and dates_raw and first_name and last_name and cert_id):
                        logger.warning(f"Skipping row {row_num}: missing required fields")
                        continue

                    parsed = parse_dates(dates_raw)
                    kind = pick_kind(parsed)
                    use_small = need_small_variant(f"{first_name} {last_name}")
                    variant = "small" if use_small else "normal"

                    group = "online" if is_online else "print"
                    docx_name = DOCX_MAP[group][kind][variant]
                    docx_path = os.path.join(TEMPLATES_DIR, docx_name)
                    if not os.path.exists(docx_path):
                        raise FileNotFoundError(f"Template not found: {docx_path}")

                    if not is_online:
                        # Режим печати: используем готовый PDF-фон + наложение текста (без LibreOffice на каждую строку)
                        template_pdf_path = get_template_pdf_path(docx_name)

                        async def render_print_one(template_pdf_path=template_pdf_path, first_name=first_name, last_name=last_name,
                                                   course=course, parsed=parsed, cert_id=cert_id, variant=variant, city=city):
                            # Получаем размеры страницы из шаблона
                            base_reader = PdfReader(template_pdf_path)
                            page = base_reader.pages[0]
                            page_w = float(page.mediabox.width)
                            page_h = float(page.mediabox.height)
                            overlay_bytes = await loop.run_in_executor(
                                executor,
                                build_overlay_pdf_bytes,
                                first_name,
                                last_name,
                                course,
                                parsed,
                                cert_id,
                                variant,
                                page_w,
                                page_h,
                                "print",
                                city,
                                None,
                                False,
                            )
                            merged = await loop.run_in_executor(executor, merge_overlay, template_pdf_path, overlay_bytes)
                            fname = f"{sanitize_filename(cert_id)}_{sanitize_filename(last_name)}_{sanitize_filename(first_name)}.pdf"
                            return fname, merged

                        tasks.append(render_print_one())
                    else:
                        # Режим online: оставляем рендер через DocxTemplate + LibreOffice
                        context = format_dates_for_jinja(parsed)
                        context.update({
                            "Имя": first_name,
                            "Фамилия": last_name,
                            "Тренинг": course,
                            "Идентификатор": cert_id,
                            "Город": city or context.get("Город", "Москва"),
                            "Страна": country,
                        })

                        async def render_online_one(docx_path=docx_path, context=context, cert_id=cert_id, last_name=last_name, first_name=first_name):
                            pdf_bytes = await loop.run_in_executor(executor, render_docx_template, docx_path, context)
                            fname = f"{sanitize_filename(cert_id)}_{sanitize_filename(last_name)}_{sanitize_filename(first_name)}.pdf"
                            return fname, pdf_bytes

                        tasks.append(render_online_one())
                except Exception as e:
                    logger.error(f"Error preparing row {row_num}: {str(e)}")
                    if state:
                        state.errors += 1
                        state.message = f"Ошибка в строке {row_num}"
                        await emit(job_id)
                    continue

            # Сбор результатов (по мере готовности)
            async for fut in _as_completed_iter(tasks):
                fname, pdf_bytes = await fut
                zf.writestr(fname, pdf_bytes)
                processed_count += 1
                if state:
                    state.processed = processed_count
                    state.message = f"Готово {processed_count} из {total}"
                    await emit(job_id)

        logger.info(f"Generation completed. Processed {processed_count} certificates")

        if processed_count == 0:
            try:
                if not incoming_fields and txt:
                    reader2 = csv.DictReader(io.StringIO(txt), delimiter=delim)
                    incoming_fields = list(reader2.fieldnames or [])
            except Exception:
                incoming_fields = []
            required = [
                "Имя/Name", "Фамилия/Surname", "Название тренинга/Название",
                "Даты/Дата", "ID/Id", "(опц.) Город/City", "(опц.) Страна/Country"
            ]
            hint = (
                "CSV распознан, но ни одной корректной строки не найдено. "
                "Проверьте заголовки и обязательные поля. Требуемые колонки: "
                + ", ".join(required)
            )
            details = f"Полученные колонки: {incoming_fields}"
            if state:
                state.stage = "error"
                state.message = "Нет валидных строк"
                await emit(job_id)
            return PlainTextResponse(hint + "\n" + details, status_code=400)

        mem_zip.seek(0)
        zip_bytes = mem_zip.getvalue()
        if state:
            state.stage = "zipping"
            state.message = "Упаковка ZIP"
            await emit(job_id)

        # Завершаем прогресс перед отдачей
        if state:
            state.stage = "done"
            state.message = "Готово"
            await emit(job_id)

        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=certificates.zip"},
        )

    except Exception as e:
        logger.error(f"Generation failed: {str(e)}")
        if job_id:
            state = get_progress(job_id)
            state.stage = "error"
            state.message = str(e)
            await emit(job_id)
        raise


async def _as_completed_iter(coros):
    """Асинхронный итератор по завершению задач (обертка вокруг asyncio.as_completed)."""
    for fut in asyncio.as_completed(coros):
        yield fut


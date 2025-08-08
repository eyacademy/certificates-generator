import io
import os
import csv
import re
import zipfile
import tempfile
import subprocess
import logging
from fastapi.responses import FileResponse, HTMLResponse, Response
from typing import Dict, List, Optional

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse

from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
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
    </style>
</head>
<body>
    <div class="container">
        <h1>Генератор сертификатов</h1>
        
        <form id="certificateForm">
            <div class="form-group">
                <label for="csvFile">Выберите CSV файл:</label>
                <input type="file" id="csvFile" name="csv_file" accept=".csv" required>
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
        
        <div class="status" id="status"></div>
    </div>

    <script>
        const form = document.getElementById('certificateForm');
        const fileInput = document.getElementById('csvFile');
        const fileStatus = document.getElementById('fileStatus');
        const generateBtn = document.getElementById('generateBtn');
        const progress = document.getElementById('progress');
        const progressBar = document.getElementById('progressBar');
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
            
            generateBtn.disabled = true;
            generateBtn.textContent = 'Генерация...';
            progress.style.display = 'block';
            status.style.display = 'none';
            
            try {
                const response = await fetch('/generate', {
                    method: 'POST',
                    body: formData
                });
                
                if (response.ok) {
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = 'certificates.zip';
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                    
                    showStatus('Сертификаты успешно сгенерированы!', 'success');
                } else {
                    const errorText = await response.text();
                    showStatus(`Ошибка: ${errorText}`, 'error');
                }
            } catch (error) {
                showStatus(`Ошибка сети: ${error.message}`, 'error');
            } finally {
                generateBtn.disabled = false;
                generateBtn.textContent = 'Сгенерировать';
                progress.style.display = 'none';
            }
        });

        function showStatus(message, type) {
            status.textContent = message;
            status.className = `status ${type}`;
            status.style.display = 'block';
        }

        // Анимация прогресса
        let progressValue = 0;
        const progressInterval = setInterval(() => {
            if (progress.style.display === 'block') {
                progressValue += Math.random() * 15;
                if (progressValue > 90) progressValue = 90;
                progressBar.style.width = progressValue + '%';
            } else {
                progressValue = 0;
                progressBar.style.width = '0%';
            }
        }, 200);
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

# Шрифт: используем системный DejaVuSans (с кириллицей), ставится в Docker
SYSTEM_FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
]
FONT_NAME = "DejaVuSans"
FONT_PATH: Optional[str] = None
for p in SYSTEM_FONT_CANDIDATES:
    if os.path.exists(p):
        FONT_PATH = p
        break
if FONT_PATH is None:
    # как фоллбек попробуем Helvetica (но у неё нет кириллицы)
    FONT_NAME = "Helvetica"
else:
    pdfmetrics.registerFont(TTFont(FONT_NAME, FONT_PATH))


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
        "id": {"x": 800, "y": 40, "align": "right", "size": 10},
    },
    "small": {
        "name": {"x": 421, "y": 320, "align": "center", "size": 22},
        "course": {"x": 421, "y": 380, "align": "center", "size": 15, "max_width": 700},
        "dates": {"x": 421, "y": 285, "align": "center", "size": 13},
        "id": {"x": 800, "y": 40, "align": "right", "size": 10},
    },
    "name_max_width": {"normal": 400, "small": 520},
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


def normalize_online(v: str) -> bool:
    v = (v or "").strip().lower()
    if v in {"да", "yes", "true", "1", "y", "онлайн", "online"}:
        return True
    if v in {"нет", "no", "false", "0", "n", "оффлайн", "offline", "off"}:
        return False
    return False


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
        if y1 < 100: y1 += 2000
        if y2 < 100: y2 += 2000
        
        return {"d1": d1, "m1": m1, "d2": d2, "m2": m2, "y": y1}
    
    elif len(dates) == 1:
        # Один день: DD.MM.YY
        d1, m1, y1 = int(dates[0].group(1)), int(dates[0].group(2)), int(dates[0].group(3))
        
        # Нормализация года (YY -> YYYY)
        if y1 < 100: y1 += 2000
        
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
        "Город": "Москва",  # По умолчанию
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
                logger.info(f"LibreOffice found in PATH")
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
    
    logger.info(f"Running command: {' '.join(cmd)}")
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if proc.returncode != 0:
        error_msg = f"LibreOffice convert failed: {proc.stderr.decode(errors='ignore')}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    pdf_path = os.path.join(
        out_dir, os.path.splitext(os.path.basename(abs_docx))[0] + ".pdf"
    )
    
    if not os.path.exists(pdf_path):
        error_msg = "PDF not produced by LibreOffice"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info(f"PDF successfully created: {pdf_path}")
    DOCX_TO_PDF_CACHE[abs_docx] = pdf_path
    return pdf_path


def render_docx_template(docx_path: str, context: Dict[str, str]) -> bytes:
    """Рендерит DOCX шаблон с Jinja-переменными"""
    doc = DocxTemplate(docx_path)
    doc.render(context)
    
    # Сохраняем во временный файл
    temp_docx = tempfile.NamedTemporaryFile(suffix=".docx", delete=False)
    doc.save(temp_docx.name)
    temp_docx.close()
    
    # Конвертируем в PDF
    pdf_path = docx_to_pdf_cached(temp_docx.name)
    
    # Читаем PDF
    with open(pdf_path, 'rb') as f:
        pdf_bytes = f.read()
    
    # Удаляем временные файлы
    os.unlink(temp_docx.name)
    
    return pdf_bytes


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


@app.get("/health")
def health() -> PlainTextResponse:
    return PlainTextResponse("ok")

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
):
    try:
        logger.info(f"Starting certificate generation for mode: {mode}")
        
        data = await csv_file.read()
        txt = data.decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(io.StringIO(txt))
        
        logger.info(f"CSV file loaded, processing rows...")

        mem_zip = io.BytesIO()
        processed_count = 0
        
        with zipfile.ZipFile(mem_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for row_num, row in enumerate(reader, 1):
                try:
                    is_online = (mode == "online")

                    course = (row.get("Название тренинга") or row.get("название тренинга") or "").strip()
                    dates_raw = (row.get("даты") or row.get("Даты") or "").strip()
                    first_name = (row.get("Имя") or row.get("имя") or "").strip()
                    last_name = (row.get("Фамилия") or row.get("фамилия") or "").strip()
                    cert_id = (row.get("ID") or row.get("id") or "").strip()

                    logger.info(f"Processing row {row_num}: {first_name} {last_name}")

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
                    
                    logger.info(f"Using template: {docx_path}")
                    
                    if not os.path.exists(docx_path):
                        logger.error(f"Template not found: {docx_path}")
                        raise FileNotFoundError(f"Template not found: {docx_path}")

                    # Подготавливаем контекст для Jinja
                    context = format_dates_for_jinja(parsed)
                    context.update({
                        "Имя": first_name,
                        "Фамилия": last_name,
                        "Тренинг": course,
                    })

                    logger.info(f"Rendering certificate for {first_name} {last_name}")
                    
                    # Рендерим DOCX с Jinja и конвертируем в PDF
                    final_pdf = render_docx_template(docx_path, context)

                    filename = f"{sanitize_filename(cert_id)}_{sanitize_filename(last_name)}_{sanitize_filename(first_name)}.pdf"
                    zf.writestr(filename, final_pdf)
                    processed_count += 1
                    
                    logger.info(f"Successfully processed certificate {processed_count}")
                    
                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {str(e)}")
                    raise

        logger.info(f"Generation completed. Processed {processed_count} certificates")
        
        mem_zip.seek(0)
        return StreamingResponse(
            mem_zip,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=certificates.zip"},
        )
        
    except Exception as e:
        logger.error(f"Generation failed: {str(e)}")
        raise


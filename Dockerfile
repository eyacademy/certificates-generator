# certificates-generator/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      libreoffice-writer libreoffice-core libreoffice-java-common \
      default-jre-headless \
      fonts-dejavu-core fontconfig \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код и артефакты
COPY app ./app
COPY Templates ./Templates
COPY Template_Certificates.xlsx ./
# Кастомные шрифты (если нужны)
# COPY fonts /usr/local/share/fonts/ey
# RUN fc-cache -f -v

EXPOSE 0

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "${PORT}"]

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice-writer libreoffice-core libreoffice-java-common default-jre-headless \
    fonts-dejavu-core fontconfig \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY Templates ./Templates
COPY fonts /usr/local/share/fonts/ey/
RUN fc-cache -f -v | cat
COPY Template_Certificates.xlsx ./

ENV PORT=10000
EXPOSE $PORT
CMD python -m uvicorn app.main:app --host 0.0.0.0 --port $PORT


FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./
COPY readme.md ./
COPY datastructures.md ./

RUN addgroup --gid 10001 appuser \
    && adduser --disabled-password --gecos "" --uid 10001 --gid 10001 appuser \
    && chown -R 10001:10001 /app
USER 10001:10001

CMD ["python", "main.py"]

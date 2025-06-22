# Etapa 1: Instalar dependencias
FROM python:3.9-slim as builder
WORKDIR /usr/src/app
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /usr/src/app/wheels -r requirements.txt

# Etapa 2: Construir la imagen final
FROM python:3.9-slim
WORKDIR /usr/src/app
COPY --from=builder /usr/src/app/wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*
COPY ./app ./app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
FROM python:3.12-slim

WORKDIR /app

COPY requirements/ requirements/
RUN pip install --no-cache-dir -r requirements/base.txt -r requirements/prod.txt

COPY . .

ENV PYTHONPATH=/app/src
ENV FORECAST_DIR=/data/forecasts

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "postprocessing.inference.api:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.9-slim
WORKDIR /app
COPY api_gm.py requirements.txt* /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "api_gm.py"]

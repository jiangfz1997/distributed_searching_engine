FROM python:3.9-slim

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*


COPY serving/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

#COPY compute/ /app/compute/
#COPY serving/ /app/serving/
RUN python -m nltk.downloader -d /usr/local/share/nltk_data punkt stopwords punkt_tab

ENV PYTHONPATH=/app


EXPOSE 8000


CMD ["python", "-m", "uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000"]
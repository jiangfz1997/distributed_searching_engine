FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -m nltk.downloader punkt stopwords

ENV PYTHONPATH=/app

CMD ["tail", "-f", "/dev/null"]
FROM python:3.9-slim

WORKDIR /app

#COPY . /app/

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


RUN python -m nltk.downloader -d /usr/local/share/nltk_data punkt stopwords punkt_tab

#COPY . .

ENV PYTHONPATH=/app

CMD ["bash"]
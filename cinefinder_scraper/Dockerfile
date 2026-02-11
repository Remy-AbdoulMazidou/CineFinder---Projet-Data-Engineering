FROM python:3.12-slim

WORKDIR /app

# Dépendances système utiles (souvent nécessaires pour lxml/cryptography)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Installer les libs python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copier le projet Scrapy
COPY . /app

# Lancer le spider et exporter dans le volume partagé
# (Le dossier /data est monté via docker-compose)
CMD ["sh", "-c", "scrapy crawl senscritique -O /data/films.json"]

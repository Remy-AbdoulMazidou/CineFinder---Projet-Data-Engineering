# CineFinder - Projet Data Engineering ESIEE Paris

## Description du projet

CineFinder est une application web qui permet d’explorer des films à partir de données récupérées par web scraping.
Le projet met en place une chaîne complète :

- **Scraping** de pages de films (Scrapy)
- **Export** des résultats en JSON (`data/films.json`)
- **Chargement** en base **MongoDB**
- **Affichage** et **statistiques** via une webapp **Flask**

---

## Technologies utilisées

- Python
- Scrapy (scraping)
- MongoDB (stockage)
- Flask (webapp + templates)
- Docker & Docker Compose (conteneurisation)

---

## Structure du projet

```
CineFinder---Projet-Data-Engineering/
├── docker-compose.yml
├── .gitignore
├── data/
│   └── films.json
├── cinefinder_scraper/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── scrapy.cfg
│   └── cinefinder_scraper/
│       ├── items.py
│       ├── middlewares.py
│       ├── pipelines.py
│       ├── settings.py
│       └── spiders/
│           └── senscritique_spider.py
├── loader/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── load_to_mongo.py
└── webapp/
    ├── Dockerfile
    ├── requirements.txt
    ├── app.py
    ├── static/
    │   └── style.css
    └── templates/
        ├── base.html
        ├── index.html
        ├── detail.html
        └── stats.html
```

---

## Détails des composants

### 1) Scrapy Spider (scraper)

Le spider Scrapy récupère des URLs de films depuis des pages “top / listes”, puis visite chaque page film.
Les informations sont principalement extraites via JSON-LD quand c’est disponible.

Exemples de champs récupérés :
- `title`, `year`
- `genres`
- `directors`, `actors`
- `rating`, `rating_count`
- `description`, `poster_url`
- `duration_min`
- `url`

Le résultat est exporté dans : `data/films.json`.

### 2) Loader MongoDB

Le loader :
- attend que `data/films.json` existe et ne soit pas vide
- attend que MongoDB soit disponible
- crée un **index unique** sur `url`
- insère/met à jour les documents avec un **upsert** (pas de doublons)

### 3) Webapp Flask

Routes principales :
- `/` : page d’accueil avec filtres et tri (titre, réalisateur, genre, note minimale)
- `/film/<id>` : page détail d’un film
- `/stats` : statistiques (agrégations Mongo : top genres, top réalisateurs, histogramme de notes, films par décennie, etc.)

---

## Instructions de lancement

### Pré-requis
- Docker + Docker Compose installés

### Lancer l’application

1) **Cloner le repository**
```bash
git clone https://github.com/Remy-AbdoulMazidou/CineFinder---Projet-Data-Engineering.git
cd CineFinder---Projet-Data-Engineering
```

2) **Construire et lancer**
```bash
docker compose up --build
```

3) **Accéder à l’application**
- Webapp : http://localhost:5000

### Arrêter l’application
```bash
docker compose down
```

### Réinitialiser complètement la base (supprime le volume Mongo)
```bash
docker compose down -v
```

---

## Auteurs

- Rémy ABDOUL MAZIDOU
- Antoine LI

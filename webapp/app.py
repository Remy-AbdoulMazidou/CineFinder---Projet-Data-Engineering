import os
import re

from flask import Flask, render_template, request, abort
from pymongo import MongoClient
from bson.objectid import ObjectId


MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB_NAME = os.getenv("MONGO_DB", "cinefinder")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "films")

client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=3000)
db = client[MONGO_DB_NAME]
films_collection = db[MONGO_COLLECTION_NAME]

app = Flask(__name__)


def safe_float(x):
    """Convertit une string en float, accepte la virgule. Retourne None si invalide."""
    if x is None:
        return None
    try:
        s = str(x).strip().replace(",", ".")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def regex_i(text):
    """Regex insensible à la casse (texte échappé)."""
    return {"$regex": re.escape(text), "$options": "i"}


def reorder_genres(genres_list, selected_genre):
    """Met le genre sélectionné en premier (affichage)."""
    if not isinstance(genres_list, list) or not selected_genre:
        return genres_list
    if selected_genre in genres_list:
        return [selected_genre] + [g for g in genres_list if g != selected_genre]
    return genres_list


@app.route("/")
def index():
    title_query = request.args.get("title", "").strip()
    director_query = request.args.get("director", "").strip()
    genre = request.args.get("genre", "").strip()
    rating_min_raw = request.args.get("rating_min", "").strip()
    sort = request.args.get("sort", "year_desc").strip()

    filters = {}

    if title_query:
        filters["title"] = regex_i(title_query)

    if director_query:
        # directors est une liste, Mongo match si un élément matche la regex
        filters["directors"] = regex_i(director_query)

    if genre and genre.lower() not in ("toutes", "tous", "all"):
        filters["genres"] = genre

    rating_min = safe_float(rating_min_raw)
    if rating_min is not None:
        filters["rating"] = {"$gte": rating_min}

    sort_map = {
        "year_desc": ("year", -1),
        "year_asc": ("year", 1),
        "rating_asc": ("rating", 1),
        "rating_desc": ("rating", -1),
        "title_asc": ("title", 1),
        "title_desc": ("title", -1),
    }
    sort_field, sort_dir = sort_map.get(sort, ("year", -1))

    films = list(
        films_collection.find(filters).sort(sort_field, sort_dir).limit(120)
    )

    if genre and genre.lower() not in ("toutes", "tous", "all"):
        for f in films:
            f["genres"] = reorder_genres(f.get("genres", []), genre)

    genres = films_collection.distinct("genres")
    genres = [g for g in genres if isinstance(g, str) and g.strip()]
    genres = sorted(set(genres), key=lambda s: s.lower())

    return render_template(
        "index.html",
        films=films,
        genres=genres,
        title_query=title_query,
        director_query=director_query,
        genre=genre,
        rating_min=rating_min_raw,
        sort=sort,
        show_header=True,
    )


@app.route("/stats")
def stats():
    # KPIs
    total_films = films_collection.count_documents({})

    avg_rating_result = list(
        films_collection.aggregate([
            {"$match": {"rating": {"$type": "number"}}},
            {"$group": {"_id": None, "avg": {"$avg": "$rating"}, "count": {"$sum": 1}}},
        ])
    )
    if avg_rating_result:
        avg_rating = float(avg_rating_result[0].get("avg") or 0)
        rated_count = int(avg_rating_result[0].get("count") or 0)
    else:
        avg_rating = None
        rated_count = 0

    with_desc = films_collection.count_documents({"description": {"$type": "string", "$ne": ""}})
    with_poster = films_collection.count_documents({"poster_url": {"$type": "string", "$ne": ""}})

    pct_desc = round((with_desc / total_films) * 100, 1) if total_films else 0.0
    pct_poster = round((with_poster / total_films) * 100, 1) if total_films else 0.0

    # Top genres
    top_genres = list(
        films_collection.aggregate([
            {"$unwind": {"path": "$genres", "preserveNullAndEmptyArrays": False}},
            {"$match": {"genres": {"$type": "string", "$ne": ""}}},
            {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
            {"$sort": {"count": -1, "_id": 1}},
            {"$limit": 12},
        ])
    )

    # Top réalisateurs
    top_directors = list(
        films_collection.aggregate([
            {"$unwind": {"path": "$directors", "preserveNullAndEmptyArrays": False}},
            {"$match": {"directors": {"$type": "string", "$ne": ""}}},
            {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
            {"$sort": {"count": -1, "_id": 1}},
            {"$limit": 10},
        ])
    )

    # Histogramme des notes (buckets)
    rating_hist = list(
        films_collection.aggregate([
            {"$match": {"rating": {"$type": "number"}}},
            {"$bucket": {
                "groupBy": "$rating",
                "boundaries": [0, 2, 4, 6, 8, 10.1],
                "default": "Autre",
                "output": {"count": {"$sum": 1}},
            }},
            {"$sort": {"_id": 1}},
        ])
    )

    hist_labels = []
    hist_counts = []
    for b in rating_hist:
        bucket_id = b.get("_id")
        count = int(b.get("count") or 0)

        if bucket_id == "Autre":
            label = "Autre"
        else:
            low = float(bucket_id)
            if low == 0:
                label = "0 – 2"
            elif low == 2:
                label = "2 – 4"
            elif low == 4:
                label = "4 – 6"
            elif low == 6:
                label = "6 – 8"
            elif low == 8:
                label = "8 – 10"
            else:
                label = str(low)

        hist_labels.append(label)
        hist_counts.append(count)

    max_hist = max(hist_counts) if hist_counts else 0

    # Films par décennie
    films_by_decade = list(
        films_collection.aggregate([
            {"$match": {"year": {"$type": "number"}}},
            {"$addFields": {"decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}}},
            {"$group": {"_id": "$decade", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ])
    )

    return render_template(
        "stats.html",
        total_films=total_films,
        avg_rating=round(avg_rating, 2) if avg_rating is not None else None,
        rated_count=rated_count,
        with_desc=with_desc,
        with_poster=with_poster,
        pct_desc=pct_desc,
        pct_poster=pct_poster,
        top_genres=top_genres,
        top_directors=top_directors,
        hist_labels=hist_labels,
        hist_counts=hist_counts,
        max_hist=max_hist,
        films_by_decade=films_by_decade,
        show_header=True,
    )


@app.route("/film/<film_id>")
def film_detail(film_id):
    try:
        obj_id = ObjectId(film_id)
    except Exception:
        abort(404)

    film = films_collection.find_one({"_id": obj_id})
    if film is None:
        abort(404)

    return render_template("detail.html", film=film, show_header=False)


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "0").strip() in ("1", "true", "True")
    app.run(host="0.0.0.0", port=5000, debug=debug)

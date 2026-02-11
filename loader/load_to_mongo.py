import json
import os
import time
from pymongo import MongoClient, UpdateOne


MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB_NAME = os.getenv("MONGO_DB", "cinefinder")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "films")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = (
    os.getenv("JSON_PATH")
    or os.getenv("DATA_FILE")
    or os.path.join(BASE_DIR, "..", "data", "films.json")
)

WAIT_TIMEOUT = int(os.getenv("WAIT_TIMEOUT", "600"))  # secondes


def wait_for_file(path, timeout_sec=180):
    start = time.time()
    while time.time() - start < timeout_sec:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True
        print(f"[loader] En attente du fichier: {path}")
        time.sleep(2)
    return False


def load_films_from_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data

    # au cas où ce serait un dict (rare), on prend les valeurs
    if isinstance(data, dict):
        return list(data.values())

    return []


def get_mongo_collection(max_tries=15):
    for i in range(max_tries):
        try:
            client = MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            db = client[MONGO_DB_NAME]
            return client, db[MONGO_COLLECTION_NAME]
        except Exception:
            print(f"[loader] Mongo pas prêt ({i+1}/{max_tries})")
            time.sleep(2)

    raise RuntimeError("Impossible de se connecter à MongoDB.")


def normalize_film(doc):
    """Vérifie le minimum et ajoute un timestamp."""
    if not isinstance(doc, dict):
        return None

    url = doc.get("url")
    if not isinstance(url, str) or not url.strip():
        return None

    doc["url"] = url.strip()
    doc["scraped_at"] = int(time.time())
    return doc


def main():
    print(f"[loader] Fichier: {DATA_FILE}")
    print(f"[loader] Timeout attente: {WAIT_TIMEOUT}s")

    if not wait_for_file(DATA_FILE, timeout_sec=WAIT_TIMEOUT):
        print("[loader] Fichier introuvable ou vide.")
        return

    films = load_films_from_json(DATA_FILE)
    print(f"[loader] Films lus: {len(films)}")

    client, collection = get_mongo_collection()

    # index unique pour éviter les doublons
    try:
        collection.create_index("url", unique=True)
    except Exception as e:
        print(f"[loader] Index url déjà présent (ou non créé): {e}")

    ops = []
    kept = 0
    skipped = 0

    for film in films:
        doc = normalize_film(film)
        if not doc:
            skipped += 1
            continue

        ops.append(UpdateOne({"url": doc["url"]}, {"$set": doc}, upsert=True))
        kept += 1

    print(f"[loader] Valides: {kept} | Ignorés: {skipped}")

    if ops:
        result = collection.bulk_write(ops, ordered=False)
        print(
            f"[loader] bulk_write OK | matched={result.matched_count} "
            f"| modified={result.modified_count} | upserted={result.upserted_count}"
        )
    else:
        print("[loader] Rien à insérer (ops vide).")

    client.close()
    print("[loader] Terminé.")


if __name__ == "__main__":
    main()

import json
import os
import random
import re

import scrapy


def iso8601_duration_to_minutes(duration):
    """Convertit une durée ISO 8601 (ex: PT1H30M) en minutes."""
    if not duration or not isinstance(duration, str):
        return None

    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?$", duration.strip())
    if not m:
        return None

    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    total = hours * 60 + minutes
    return total if total > 0 else None


def as_list(x):
    """Force une valeur en liste."""
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def clean_title_and_year(text):
    """Nettoie un titre et essaie d'extraire une année en fin de chaîne."""
    if not text:
        return None, None

    s = " ".join(text.split()).strip()

    # suffixes qu'on voit souvent dans les titres de pages
    s = re.sub(r"\s*-\s*SensCritique\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*Film\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*Série\s*$", "", s, flags=re.IGNORECASE)

    year = None
    m = re.search(r"\((\d{4})\)\s*$", s)
    if m:
        year = int(m.group(1))
        s = re.sub(r"\s*\(\d{4}\)\s*$", "", s).strip()

    return s, year


def extract_year_from_date(date_str):
    """Récupère l'année au début d'une date (ex: 2019-03-01)."""
    if not date_str or not isinstance(date_str, str):
        return None
    m = re.match(r"^(\d{4})", date_str.strip())
    return int(m.group(1)) if m else None


def iter_jsonld_objects(data):
    """Parcourt récursivement des objets JSON-LD (list / dict / @graph)."""
    if isinstance(data, list):
        for x in data:
            yield from iter_jsonld_objects(x)
    elif isinstance(data, dict):
        if "@graph" in data:
            yield from iter_jsonld_objects(data.get("@graph"))
        else:
            yield data


class SensCritiqueSpider(scrapy.Spider):
    name = "senscritique"
    allowed_domains = ["senscritique.com"]

    default_seed_urls = [
        "https://www.senscritique.com/films/tops/top111",
        "https://www.senscritique.com/liste/les_200_films_les_plus_notes_sur_sens_critique/1499333",
        "https://www.senscritique.com/liste/les_100_meilleurs_films_de_tous_les_temps/93309",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "COOKIES_ENABLED": True,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        "FEED_EXPORT_ENCODING": "utf-8",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        seed_env = os.getenv("SEED_URLS", "").strip()
        if seed_env:
            self.seed_urls = [u.strip() for u in seed_env.split(",") if u.strip()]
        else:
            self.seed_urls = self.default_seed_urls[:]

        self.max_items = int(os.getenv("MAX_ITEMS", "250"))
        self.max_pages = int(os.getenv("MAX_PAGES", "12"))

        shuffle_env = os.getenv("SHUFFLE", "1").strip()
        self.shuffle = shuffle_env not in ("0", "false", "False")

        seed_value = os.getenv("RANDOM_SEED", "").strip()
        if seed_value:
            try:
                random.seed(int(seed_value))
            except Exception:
                random.seed(seed_value)

        self.sample_rate = float(os.getenv("SAMPLE_RATE", "1.0"))

        self.seen_film_urls = set()
        self.seen_list_pages = set()
        self.pages_crawled = 0
        self.items_scheduled = 0

        # stop automatique quand on atteint MAX_ITEMS
        self.custom_settings = dict(self.custom_settings)
        self.custom_settings["CLOSESPIDER_ITEMCOUNT"] = self.max_items

        self.logger.info(
            "Config: seeds=%d | MAX_ITEMS=%d | MAX_PAGES=%d | SHUFFLE=%s | SAMPLE_RATE=%.2f",
            len(self.seed_urls),
            self.max_items,
            self.max_pages,
            self.shuffle,
            self.sample_rate,
        )

    def start_requests(self):
        urls = self.seed_urls[:]
        if self.shuffle:
            random.shuffle(urls)

        for url in urls:
            yield scrapy.Request(
                url,
                callback=self.parse_list_page,
                dont_filter=True,
            )

    def parse_list_page(self, response):
        """Parse une page de liste/top et planifie des pages de films."""
        if response.url in self.seen_list_pages:
            return
        self.seen_list_pages.add(response.url)

        self.pages_crawled += 1
        if self.pages_crawled > self.max_pages:
            self.logger.info("MAX_PAGES atteint (%d).", self.max_pages)
            return

        html = response.text or ""

        # liens vers les films
        film_urls = set()

        for href in response.css('a[href^="/film/"]::attr(href)').getall():
            if href:
                film_urls.add(href.split("?")[0])

        # fallback si la structure est différente
        for p in set(re.findall(r'href="(/film/[^"#?]+)', html)):
            film_urls.add(p.split("?")[0])
        for m in re.findall(r'"/film/[^"]+"', html):
            p = m.strip('"')
            film_urls.add(p.split("?")[0])

        film_urls = [response.urljoin(p) for p in film_urls if p.startswith("/film/")]

        if self.shuffle:
            random.shuffle(film_urls)

        if 0.0 < self.sample_rate < 1.0:
            k = max(1, int(len(film_urls) * self.sample_rate))
            film_urls = film_urls[:k]

        scheduled_now = 0
        for url in film_urls:
            if self.items_scheduled >= self.max_items:
                break
            if url in self.seen_film_urls:
                continue

            self.seen_film_urls.add(url)
            self.items_scheduled += 1
            scheduled_now += 1

            yield scrapy.Request(url, callback=self.parse_film, meta={"item": {"url": url}})

        if not film_urls:
            self.logger.warning("Aucun lien film détecté sur %s", response.url)
            self.logger.warning("Extrait HTML: %s", (html[:300] or "").replace("\n", " "))
        else:
            self.logger.info(
                "List page: %s | films=%d | schedulés=%d | total=%d",
                response.url,
                len(film_urls),
                scheduled_now,
                self.items_scheduled,
            )

        if self.pages_crawled >= self.max_pages:
            return

        # pagination (heuristique)
        next_links = set()

        for href in response.css('a[rel="next"]::attr(href)').getall():
            if href:
                next_links.add(response.urljoin(href))

        for href in response.css("a::attr(href)").getall():
            if not href:
                continue
            if "/liste/" in href or "/films/" in href or "/top" in href:
                if re.search(r"/\d+(\?|$)", href) or "page=" in href:
                    next_links.add(response.urljoin(href))

        next_links = list(next_links)
        if self.shuffle:
            random.shuffle(next_links)

        for u in next_links[:8]:
            if self.pages_crawled >= self.max_pages:
                break
            if u in self.seen_list_pages:
                continue
            yield scrapy.Request(u, callback=self.parse_list_page, dont_filter=True)

    def parse_film(self, response):
        """Parse une page de film et renvoie un item."""
        item = response.meta.get("item", {})
        item["url"] = response.url

        # on essaie d'abord JSON-LD
        ld_json_list = response.css('script[type="application/ld+json"]::text').getall()
        movie = None

        for raw in ld_json_list:
            raw = (raw or "").strip()
            if not raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                continue

            for obj in iter_jsonld_objects(data):
                if not isinstance(obj, dict):
                    continue

                t = obj.get("@type")
                if isinstance(t, list):
                    is_movie = any(x in ("Movie", "Film") for x in t)
                else:
                    is_movie = t in ("Movie", "Film")

                if is_movie:
                    movie = obj
                    break

            if movie:
                break

        if movie:
            name = movie.get("name")
            if isinstance(name, str) and name.strip():
                clean_title, guessed_year = clean_title_and_year(name)
                item["title"] = clean_title
                item["full_title"] = name.strip()
                if guessed_year is not None:
                    item["year"] = guessed_year

            y = extract_year_from_date(movie.get("datePublished"))
            if y is not None:
                item["year"] = y

            img = movie.get("image")
            if isinstance(img, dict):
                img = img.get("url")
            if isinstance(img, list) and img:
                img = img[0]
            item["poster_url"] = img

            item["description"] = movie.get("description")

            genres = movie.get("genre")
            item["genres"] = [g.strip() for g in as_list(genres) if isinstance(g, str) and g.strip()]

            item["duration_min"] = iso8601_duration_to_minutes(movie.get("duration"))

            directors = []
            for d in as_list(movie.get("director")):
                if isinstance(d, dict) and d.get("name"):
                    directors.append(str(d["name"]).strip())
                elif isinstance(d, str):
                    directors.append(d.strip())
            item["directors"] = [x for x in directors if x]

            actors = []
            for a in as_list(movie.get("actor")):
                if isinstance(a, dict) and a.get("name"):
                    actors.append(str(a["name"]).strip())
                elif isinstance(a, str):
                    actors.append(a.strip())
            item["actors"] = [x for x in actors if x]

            rating = movie.get("aggregateRating") or {}
            if isinstance(rating, dict):
                rv = rating.get("ratingValue")
                rc = rating.get("ratingCount")

                try:
                    item["rating"] = float(rv) if rv is not None else None
                except Exception:
                    item["rating"] = None

                try:
                    item["rating_count"] = int(rc) if rc is not None else None
                except Exception:
                    item["rating_count"] = None

        # fallback titre/année via meta ou <title>
        if not item.get("title") or item.get("year") is None:
            ogt = response.css('meta[property="og:title"]::attr(content)').get()
            tit = response.css("title::text").get()

            for txt in (ogt, tit):
                if not txt:
                    continue
                clean_title, guessed_year = clean_title_and_year(txt)
                if not item.get("title") and clean_title:
                    item["title"] = clean_title
                if item.get("year") is None and guessed_year is not None:
                    item["year"] = guessed_year

        # fallback image
        if not item.get("poster_url"):
            item["poster_url"] = response.css('meta[property="og:image"]::attr(content)').get()

        yield item

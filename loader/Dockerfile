FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY load_to_mongo.py /app/load_to_mongo.py

CMD ["python", "load_to_mongo.py"]

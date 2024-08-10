from urllib.parse import urlparse, urlunparse
import dotenv
import os
import sqlalchemy
import time
import random

dotenv.load_dotenv()

database_url = os.getenv("DATABASE_URL")
database_url_parsed = urlparse(database_url)
database_url_new = urlunparse(database_url_parsed._replace(scheme="postgresql+psycopg2"))
engine = sqlalchemy.create_engine(database_url_new)

with engine.connect() as connection:
    while True:
        reps = random.randint(1, 10)
        updates = 0
        for i in range(reps):
            res = connection.execute(sqlalchemy.text(f"UPDATE \"Market\" SET \"CzkEur\" = random() WHERE \"Time\" = '2024-01-02 {random.randint(1, 24)}:00:00+01:00'"))
            updates += res.rowcount
        connection.commit()
        print(f"Updated {updates} rows in {reps} queries")
        time.sleep(1)


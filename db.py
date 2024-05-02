import os
from datetime import datetime
import re
from random import choice

import ydb

from logger import logger

YD_ENDPOINT = os.getenv(key="YD_ENDPOINT")
YD_PATH = os.getenv(key="YD_PATH")
YD_PATH_TOKEN = os.getenv(key="YD_PATH_TOKEN")


class DbHandler:
    def __init__(self):
        driver_config = ydb.DriverConfig(
            endpoint=YD_ENDPOINT,
            database=YD_PATH,
            credentials=ydb.credentials_from_env_variables(),
            root_certificates=ydb.load_ydb_root_certificate(),
        )

        self.driver = ydb.Driver(driver_config)
        try:
            self.driver.wait(timeout=15)
        except TimeoutError:
            logger.warn(f"Connect failed to YDB: {self.driver.discovery_debug_details()}")

    def add_user(self,user_id, username, first_name):
        session = self.driver.table_client.session().create()
        if self.user_exists(user_id):
            return

        query = f"""
            INSERT INTO Users (user_id, username, first_name, total_albums, last_listened)
            VALUES ({user_id}, '{username}', '{first_name}', 0, CAST(0 AS Timestamp))
            """

        session.transaction().execute(
            query,
            commit_tx=True
        )
        logger.info(f"User {user_id} successfully added to DB")

    def add_album(self, image_url, title, description):
        session = self.driver.table_client.session().create()
        if self.album_exists(title):
            return

        last_id = self.get_album_with_max_id()
        last_id += 1
        description = description.replace("\n", " ")
        description = description.replace("\'", ' ')

        print(last_id, image_url, title, description)
        query = f"""
                INSERT INTO Albums (album_id, image_url, title, description)
                VALUES ({last_id}, '{image_url}', '{title}', '{description}')
                """
        print(query)
        session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            commit_tx=True
        )

    def get_album_with_max_id(self):
        session = self.driver.table_client.session().create()

        query_result = session.transaction(ydb.SerializableReadWrite()).execute(
            """
            SELECT album_id, image_url, title, description
            FROM Albums
            ORDER BY album_id DESC
            LIMIT 1
            """
        )

        if not query_result:
            print("No albums found.")
            return None

        min_album = query_result[0].rows[0]
        return min_album['album_id']


    def album_exists(self, title):
        session = self.driver.table_client.session().create()
        result = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            SELECT 1 FROM Albums WHERE title = '{title}' LIMIT 1;
            """,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
        )
        return len(result[0].rows) == 1

    def user_exists(self, user_id):
        session = self.driver.table_client.session().create()
        result = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            SELECT 1 FROM Users WHERE user_id = {user_id} LIMIT 1;
            """,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
        )
        return len(result[0].rows) == 1

    def get_random_album(self, user_id):
        session = self.driver.table_client.session().create()

        result = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            $in_collection =
            SELECT album_id
            FROM UserAlbumTies 
            WHERE user_id = {user_id};

            SELECT albums.album_id AS album_id
            FROM Albums as albums
            LEFT ONLY JOIN $in_collection as collection
            USING (album_id);
            """,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
        )

        chosen_card_id = choice(result[0].rows)["album_id"]

        result = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            INSERT INTO UserAlbumTies (user_id, album_id, rate) VALUES 
            ({user_id}, {chosen_card_id}, {-1});

            SELECT *
            FROM Albums
            WHERE album_id == {chosen_card_id};
            """,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
        )

        session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            UPDATE Users
            SET total_albums = total_albums + 1
            WHERE user_id = {user_id};
            """,
            commit_tx=True,
        )

        datetime_now = f"Datetime('{datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}')"
        session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            UPDATE Users
            SET last_listened = {datetime_now}
            WHERE user_id = {user_id};
            """,
            commit_tx=True,
        )

        image = result[0].rows[0]['image_url'].decode()
        album_id = result[0].rows[0]['album_id']
        title = result[0].rows[0]['title'].decode()
        description = result[0].rows[0]['description'].decode()

        return image, album_id, title, description

    def update_rate(self, user_id, album_id, rate):
        session = self.driver.table_client.session().create()
        session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            UPDATE UserAlbumTies
            SET rate = {rate}
            WHERE user_id = {user_id} AND album_id = {album_id};
            """,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
        )


    def get_all_albums(self, user_id):
        session = self.driver.table_client.session().create()
        result = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            SELECT * FROM UserAlbumTies
            WHERE user_id = {user_id};
            """,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
        )

        all_albums = 'Прослушанные альбомы:\n\n'
        skipped_albums = []
        not_rated_yet = []
        for row in result[0].rows:
            album = session.transaction(ydb.SerializableReadWrite()).execute(
                    f"""
                    SELECT * FROM Albums WHERE album_id = {row['album_id']};
                    """,
                    commit_tx=True,
                )

            album_id = row['album_id']
            title = album[0].rows[0]['title'].decode()
            rate = row['rate']
            if rate == -1:
                not_rated_yet.append(f'{album_id}. {title}\n')
            elif rate == 0:
                skipped_albums.append(f'{album_id}. {title}\n')
            else:
                all_albums += f'{album_id}. {title} - {rate} {rate * '⭐'}\n'

        if not_rated_yet:
            all_albums += '\nНеоцённые альбомы:\n\n'
            for album in not_rated_yet:
                all_albums += album

        if skipped_albums:
            all_albums += '\nПропущенные альбомы:\n\n'
            for album in skipped_albums:
                all_albums += album

        return all_albums

import requests as re
import mysql.connector as conn

import time
import random

connect = conn.connect(
    host='103.127.97.31',
    user='spotify',
    passwd='#Spotify2023',
    database='spotify',
)

class DB():
    def connection(self):
        connect = conn.connect(
            host='103.127.97.31',
            user='spotify',
            passwd='#Spotify2023',
            database='spotify',
            auth_plugin='mysql_native_password'
        )

        return connect

class Worker(DB):
    def __init__(self):
        self.token = ''
        self.connect = self.connection()

    def main(self):
        print('Starting...')

        collection = self.collection()

        self.token = self.get_token()

        for row in collection:
            time.sleep(random.randint(3,10))

            response, status_code = self.get_artist(self.token, row['track_id'])

            print(response)

            if status_code == 400:
                self.token = self.get_token()

            list_artist = []

            for arts in response['artists']:
                list_artist.append(arts['name'])

            self.update_data(row['track_id'])
            self.insert_data(list_artist=list_artist, response=response)

            list_artist = []

    def update_data(self, track_id):
            query = '''UPDATE raw_data SET `update` = "1" WHERE `track_id` = %s'''

            cursor = self.connect.cursor(dictionary=True)

            cursor.execute(query, [track_id])

            self.connect.commit()
        
    def insert_data(self, list_artist, response):
        query = '''INSERT INTO artist (`artist_id`, `track_id`, `artist`, `artist_image`, `artist_url`) VALUES (%s, %s, %s, %s, %s)'''

        cursor = self.connect.cursor(dictionary=True)

        artist = ", ".join(list_artist)

        cursor.execute(query, [
            response['album']['artists'][0]['id'], response['id'], artist, response['album']['images'][0]['url'], 
            response['album']['artists'][0]['external_urls']['spotify']
        ])

        self.connect.commit()

    def get_artist(self, token, track_id):
        Headers = {
            "Authorization": token,
        }

        url = "https://api.spotify.com/v1/tracks/" + track_id

        response = re.get(url, headers=Headers)

        return response.json(), response.status_code
    
    def collection(self):
        query = '''SELECT * FROM raw_data WHERE `update` = "0" LIMIT 100'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query)

        return cursor.fetchall()

    def get_token(self) -> str:
        header = {
            "Authorization": "Basic ZDQxNTI1YmJhZDJlNDE0N2I2YmYyMDBmNjI0MzYxYWM6YTBhMWFmMzdiNGIzNDJhNjgxMDVkN2RlNmRhMzY1Yzk=",
            "Content-Type": "application/x-www-form-urlencoded", 
        }

        body = {
            "grant_type": "client_credentials"
        }

        request = re.post("https://accounts.spotify.com/api/token", headers=header, data=body)

        return "Bearer " + str(request.json()['access_token'])

def main():
    print('Hello')

if __name__ == '__main__':
    worker = Worker()
    
    worker.main()
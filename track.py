import requests as re
import mysql.connector as conn

import time
import random

connect = conn.connect(
    host='103.127.97.31',
    user='spotify',
    passwd='#Spotify2023',
    database='spotify',
    auth_plugin='mysql_native_password'
)

class DB():
    def connection(self):
        connect = conn.connect(
            host='103.127.97.31',
            user='spotify',
            passwd='#Spotify2023',
            database='spotify',
        )

        return connect

class Worker(DB):
    def __init__(self):
        self.token = ''
        self.connect = self.connection()

    def main(self):
        print('Starting...')

        counter = 0

        collection = self.collection()

        self.token = self.get_token()

        for i, row in enumerate(collection):
            time.sleep(random.randint(3,10))

            if counter == 25:
                time.sleep(random.randint(100, 600))

            response, status_code = self.get_album(self.token, row['artist_id'])

            print(response)

            if status_code == 400:
                self.token = self.get_token()
            
            self.insert_track(row['album_id'])

            self.update_data(row['album_id'])

        counter += 1

    def collection(self):
        query = '''SELECT * FROM album WHERE `update` = "0" ORDER BY id DESC'''

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
    
    def insert_track(self, album_id, artist_id, response):
        query = '''INSERT INTO album (`album_id`, `artist_id`, `track_id`, `track_name`, `track_link`, `duration`, `update`) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query, [
            album_id, artist_id, response['id'],  response['name'], response['external_urls']['spotify'], response['duration_ms'], "0"
        ])

        self.connect.commit()

    def update_data(self, album_id):
        query = '''UPDATE album SET `update` = "1" WHERE `album_id` = %s'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query, [album_id])

        self.connect.commit()
    
    def get_track(self, token, album_id):
        Headers = {
            "Authorization": token,
        }

        url = "https://api.spotify.com/v1/albums/" + str(album_id) + "/tracks?market=ID&limit=50"

        response = re.get(url, headers=Headers)

        return response.json(), response.status_code
        
def main():
    print('Hello')

if __name__ == '__main__':
    worker = Worker()
    
    worker.main()
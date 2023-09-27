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

        for row in collection:
            time.sleep(random.randint(3,10))

            if counter == 25:
                time.sleep(random.randint(100, 600))
            
            response, status_code = self.get_album(self.token, row['artist_id'])

            print(response)

            if status_code == 400:
                self.token = self.get_token()

            counter += 1

            for item in response:
                self.insert_album(row['artist_id'], response=item)
                
                self.update_data(row['artist_id'])

    def collection(self):
        query = '''SELECT * FROM artist WHERE `update` = "0" ORDER BY id DESC'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query)

        return cursor.fetchall()
    
    def insert_album(self, artist_id, response):
        query = '''INSERT INTO album (`artist_id`, `album`, `album_id`, `album_url`, `album_image`, `release_date`, `total_track`, `update`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query, [
            artist_id, response['name'], response['id'],  response['external_urls']['spotify'], response['images'][0]['url'], response['release_date'], response['total_tracks'], "0"
        ])

        self.connect.commit()

    def update_data(self, artist_id):
            query = '''UPDATE artist SET `update` = "1" WHERE `artist_id` = %s'''

            cursor = self.connect.cursor(dictionary=True)

            cursor.execute(query, [artist_id])

            self.connect.commit()

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

    def get_album(self, token, artist_id):
        Headers = {
            "Authorization": token,
        }

        url = "https://api.spotify.com/v1/artists/" + str(artist_id) + "/albums?include_groups=album,single,compilation,appears_on&offset=0&limit=30"

        response = re.get(url, headers=Headers)

        return response.json()['items'], response.status_code

def main():
    print('Hello')

if __name__ == '__main__':
    worker = Worker()
    
    worker.main()
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

            response, status_code = self.get_track_feature(self.token, row['track_id'])

            if status_code == 404:
                self.update_data(row['track_id'], 2)
            else:
                self.insert_track_feature(row['track_id'], response=response)

                self.update_data(row['track_id'], 1) 

        counter += 1

    def collection(self):
        query = '''SELECT * FROM track WHERE `update` = "0" ORDER BY id DESC'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query)

        return cursor.fetchall()

    def get_token(self) -> str:
        header = {
            "Authorization": "Basic MmMxOTcxMDVlYjZiNDBhZDgzNmRlZDY3MzhmZTAxNjg6MzhlNTFlZGFmNDZlNGJmMTkxOTI3NTE2YjM5MDJmNzE=",
            "Content-Type": "application/x-www-form-urlencoded", 
        }

        body = {
            "grant_type": "client_credentials"
        }

        request = re.post("https://accounts.spotify.com/api/token", headers=header, data=body)

        return "Bearer " + str(request.json()['access_token'])
    
    def insert_track_feature(self, track_id, response):
        query = '''INSERT INTO feature (`danceability`,`energy`,`key`,
                    `loudness`,`mode`,`speechiness`,
                    `acousticness`,`instrumentalness`,`liveness`,
                    `tempo`,`valence`,`track_id`) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query, [
            response['danceability'], response['energy'], response['key'], response['loudness'],
            response['mode'], response['speechiness'], response['acousticness'], response['instrumentalness'],
            response['liveness'], response['tempo'], response['valence'], track_id
        ])

        self.connect.commit()

    def update_data(self, track_id, status):
        if status == 2:
            query = '''UPDATE track SET `update` = "2" WHERE `track_id` = %s'''
        else:
            query = '''UPDATE track SET `update` = "1" WHERE `track_id` = %s'''

        cursor = self.connect.cursor(dictionary=True)

        cursor.execute(query, [track_id])

        self.connect.commit()
    
    def get_track_feature(self, token, track_id):
        Headers = {
            "Authorization": token,
        }

        url = "https://api.spotify.com/v1/audio-features/" + str(track_id)

        response = re.get(url, headers=Headers)

        return response.json(), response.status_code
        
def main():
    print('Hello')

if __name__ == '__main__':
    worker = Worker()
    
    worker.main()
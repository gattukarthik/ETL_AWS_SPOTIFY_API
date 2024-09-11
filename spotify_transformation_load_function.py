import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    #create list of dictinaries
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id, 'album_name':album_name, 'album_release_date': album_release_date, 'album_total_tracks': album_total_tracks,
                            'album_url':album_url}
        album_list.append(album_element)
    return album_list 
        
def artist(data):
    artist_list = []
    for row in data['items']:
        for key,value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_id = artist['id']
                    artist_name = artist['name']
                    artist_type = artist['type']
                    artist_url = artist['external_urls']['spotify']
                    artist_element = {'artist_id': artist_id, 'artist_name': artist_name, 'artist_type': artist_type,
                                     'artist_url': artist_url}
                    artist_list.append(artist_element)
    return artist_list
    
def songs(data):
    song_list = []
    for row in data['items']:
        song_name = row['track']['name']
        song_id = row['track']['id']
        song_popularity = row['track']['popularity']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        album_id = row['track']['album']['id']
        artist_id = row['track']['artists'][0]['id'] #taking only one artist
        artist_name = row['track']['artists'][0]['name']
        song_elements = {'song_name': song_name, 'song_id': song_id, 'song_popularity': song_popularity, 'song_duration':song_duration,
                        'song_url':song_url, 'album_id':album_id, 'artist_id': artist_id, 'artist_name': artist_name}
        song_list.append(song_elements)
    return song_list
        

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-mumbai-karthik"
    Key = "raw_data/to_processed/"
    
    #print(s3.list_objects(Bucket = Bucket, Prefix = Key))
    #print(s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents'])
    
    spotify_data = []
    spotify_keys = []
    for file in (s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']):
        # print(file['Key']) #raw_data/to_processed/spotify_raw_2024-09-11 11:35:15.052966.json
        file_key = file['Key'] 
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            context = response['Body']
            jsonObject = json.loads(context.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            # print(spotify_data) #Json Data
            # print(spotify_keys) #['raw_data/to_processed/spotify_raw_2024-09-11 11:35:15.052966.json']
        
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        songs_list = songs(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset = ['album_id'])
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset = ['artist_id'])
        song_df = pd.DataFrame.from_dict(songs_list)
        song_df = song_df.drop_duplicates(subset = ['song_id'])
        
        song_key = "transformed_data/songs_data/song_transformed" + str(datetime.now()) + ".csv" # file path (key) in your S3 bucket where the CSV will be uploaded.
        song_Buffer = StringIO()  #StringIO() creates an in-memory buffer that acts like a file. You can write text (CSV data in this case) to it, and it will behave like a file object.
        song_df.to_csv(song_Buffer, index = False) #Convert the DataFrame to CSV and Store in Buffer:(writes the DataFrame content as a CSV format into the song_Buffer instead of saving it to a physical file.)
        song_content = song_Buffer.getvalue() #getvalue() retrieves the entire content of the song_Buffer as a string.This string will be the body of your CSV file, and it's now ready to be uploaded.
        #print(song_content)- total CSV data
        s3.put_object (Bucket = Bucket, Key = song_key, Body = song_content) #uploads the content to the specified S3 bucket
        #Bucket=Bucket: Refers to the name of the S3 bucket where the file will be uploaded. The Bucket variable should be defined elsewhere.
        #Key=song_key: Refers to the location (key) in the S3 bucket, which is the file path constructed in step 1.
        #Body=song_content: The actual content to be stored in the file, which is the CSV data in string format.
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_Buffer = StringIO()
        artist_df.to_csv(artist_Buffer, index = False)
        artist_content = artist_Buffer.getvalue()
        s3.put_object (Bucket = Bucket, Key = artist_key, Body = artist_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_Buffer = StringIO()
        album_df.to_csv(album_Buffer, index = False)
        album_content = album_Buffer.getvalue()
        s3.put_object (Bucket = Bucket, Key = album_key, Body = album_content)
        
    #Moving Data from to_processed to Processed
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        #print(copy_source) #{'Bucket': 'spotify-etl-mumbai-karthik', 'Key': 'raw_data/to_processed/spotify_raw_2024-09-11 11:35:15.052966.json'}
        #print(key) #raw_data/to_processed/spotify_raw_2024-09-11 11:35:15.052966.json
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])    
        s3_resource.Object(Bucket, key).delete()

    
        
    #boto3.resource('s3') creates a high-level S3 resource object, which allows you to interact with S3.s3_resource can be used to copy and delete objects in your S3 bucket.
    #copy_source is a dictionary that defines the source bucket and object key (file path) from where the file will be copied.
    #s3_resource.meta.client.copy() copies an object from one location to another within the S3 bucket.
    #copy_source: The source file information (bucket and key) created in the previous step. #
    #Bucket: The same destination bucket where the file will be copied (same as the source in this case). #Bucket = "spotify-etl-mumbai-karthik"
    #After the object is copied, the original object in the bucket is deleted using s3_resource.Object().delete().
    
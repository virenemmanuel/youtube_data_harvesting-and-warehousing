from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from ipykernel import kernelapp as app
from sqlalchemy import Column, Integer, String, TIMESTAMP
import re
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from ipykernel import kernelapp as app
from cryptography.x509 import load_der_x509_certificate



# Api key details 
# creating a function for Api_details

def Api_connect():
    api_key = "AIzaSyA4Xkqss0doi6-eIrH6DGhhticzIQPV51w"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=api_key)

    return youtube

youtube = Api_connect()

# get channels information
def get_channel_info(channel_id):

    request=youtube.channels().list(
                     part = "snippet,ContentDetails,statistics",
                    id = channel_id
    )
    response=request.execute()

    channel_data = []

    for i in response['items']:
        data = {'Channel_Name' : i['snippet']['title'],
                'Channel_Id': i['id'],
                'Subscribers':i['statistics']['subscriberCount'],
                'Views' :i['statistics']['viewCount'],
                'Total_videos': i['statistics']['videoCount'],
                'Channel_Discription':i['snippet'].get('description', ''),
                'Playlist_Id ':i['contentDetails']['relatedPlaylists']['uploads']}
        channel_data.append(data)
    
        return data


# getting videos ids

def get_videos_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id=channel_id,
                                       part = 'contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                            part = 'snippet',
                                            playlistId = Playlist_Id,
                                            maxResults=50,
                                            pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
    

# getting video information

def get_video_info(Video_ids):
        video_data = []
        for video_id in Video_ids:
            request = youtube.videos().list(
                    part = "snippet,ContentDetails,statistics",
                    id = video_id
            )
            response = request.execute()
        
            for item in response['items']:
                data = dict(Channel_Name = item['snippet']['channelTitle'],
                            Channel_Id = item['snippet']['channelId'],
                            Video_Id = item['id'],
                            Title = item['snippet']['title'],
                            Description = item['snippet']['description'],
                            Published_date = item['snippet']['publishedAt'],
                            Views = item['statistics']['viewCount'],
                            likes = item['statistics']['likeCount'],
                            Comments = item.get('CommentCount'),                                          
                            Duration = item['contentDetails']['duration']
                                )
                video_data.append(data)
        return video_data
                            
    
    
# creat function for get the comment information
def get_comment_info(Video_ids):
    comment_data = []
    try:
        for video_id in Video_ids:    
            request = youtube.commentThreads().list(
                        part = 'snippet',
                        videoId =video_id ,                            
                        maxResults = 50)
            
            response = request.execute()
            
            for i in range(len(response['items'])):
                    data =dict(
                                Comment_Id = response['items'][0]['snippet']['topLevelComment']['id'],
                                Video_Id = response['items'][0]['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_text = response['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_author =response['items'][0]['snippet']['topLevelComment']['snippet'],
                                Comment_published = response['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt'])
                    comment_data.append(data)
                
         
    except:
        pass        
    return comment_data



#get playlist details
def get_playlist_info(channel_id):
        next_page_token = None
        All_data = []
        while True:
                request = youtube.playlists().list(
                        part = "snippet,contentDetails",
                        channelId = channel_id,
                        maxResults = 50,
                        pageToken = next_page_token
                    )

                response = request.execute()

                for item in response['items']:
                        data = dict(Playlist_Id = item['id'],
                                    Title = item['snippet']['title'],
                                    Channel_Id = item['snippet']['channelId'],
                                    Channel_Name = item['snippet']['channelTitle'],
                                    PublishedAt = item['snippet']['publishedAt'],
                                    Video_Count = item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token = response.get('nextPageToken')

                if next_page_token is None:
                    break
        return All_data
                        

# creat a clinent instance for MongoDB connection 
conn_str = "mongodb+srv://virenemmanuel:roomno13@cluster0.e6ecnpv.mongodb.net/project1?retryWrites=true&w=majority"
client = pymongo.MongoClient(conn_str)

#create a database or use existing one
db = client["Youtube_data"]



#create function to upload to MongoDB
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details =get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"



# Table creation for channels,playlists,videos,comments
def channels_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "roomno13",
                            database = "youtube_data",
                            port = "5432")

    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers_Count bigint,
                                                            Views bigint,                     
                                                            Total_videos int,
                                                            Channel_Discription text,
                                                            Playlist_Id varchar(80))'''
        
        cursor.execute(create_query)
        mydb.commit()

    except:
        st.write("Channels table already created")


    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Views,
                                            Total_Videos,
                                            Channel_Discription,
                                            Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        
        values = (
                row['Channels_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("Channels values are already inserted")


def playlist_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="roomno13",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()
    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                                                                Title varchar(80),
                                                                ChannelId varchar(100),
                                                                ChannelName varchar(100),
                                                                PublishedAt timestamp,
                                                                VideoCount int) '''
        cursor.execute(create_query)
        mydb.commit()

    except:
        st.write("playlists Table already ccreated")

    db = client["youtube_data"]
    coll1 = db["channels_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)

    for index,row in df.iterrows():
        insert_query = '''INSERT into playlists(PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                            VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['published_at'],
                row['VideoCount'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            st.write("playlists values are already inserted")
            

            

# def videos_table():
#     mydb = psycopg2.connect(
#         host="localhost",
#         user="postgres",
#         password="roomno13",
#         database="youtube_data",
#         port="5432")
    
#     cursor = mydb.cursor()

#     drop_query = "DROP TABLE IF EXISTS videos"
#     cursor.execute(drop_query)
#     mydb.commit()

#     create_query = ''' CREATE TABLE IF NOT EXISTS Videos ( Channel_Name VARCHAR(150),
#                                                            Channel_Id VARCHAR(100),
#                                                            Video_Id VARCHAR(80) PRIMARY KEY,
#                                                            Title VARCHAR(150),
#                                                            Thumbnail VARCHAR(250),
#                                                            Description VARCHAR(250),
#                                                            Published_Date TIMESTAMP,
#                                                            Duration INTERVAL,
#                                                            Views BIGINT,
#                                                            Likes BIGINT,
#                                                            Comments INT,
#                                                            Favorite_Count INT,
#                                                                 )'''
#     cursor.execute(create_query)
#     mydb.commit()

#     vi_list = []
#     db = client["Youtube_data"]
#     coll1 = db["channel_details"]
#     for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
#         for i in range(len(vi_data["video_information"])):
#             vi_list.append(vi_data["video_information"][i])
#         df2 = pd.DataFrame(vi_list)

        

#         for index, row in df2.iterrows():
#             insert_query = '''
#                 INSERT INTO videos (
#                     Channel_Name,
#                     Channel_Id,
#                     Video_Id,
#                     Title,
#                     Thumbnail,
#                     Description,
#                     Published_Date,
#                     Duration,
#                     Views,
#                     Likes,
#                     Comments,
#                     Favorite_Count,
                   
                  
#                 ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             '''
#             values = (
#                 row['Channel_Name'],
#                 row['Channel_Id'],
#                 row['Video_Id'],
#                 row['Title'],
#                 row['Thumbnail'],
#                 row['Description'],
#                 row['Published_Date'],
#                 row['Duration'],
#                 row['Views'],
#                 row['Likes'],
#                 row['Comments'],
#                 row['Favorite_Count'],
                
                
#             )
#             try:
#                 cursor.execute(insert_query, values)
#                 mydb.commit()
#             except psycopg2.Error as e:
#                 print(f"Error inserting row: {e}")
#                 mydb.rollback()
#                 break
#         else:
#             print("Videos values already inserted in the table")

def videos_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="roomno13",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''
    CREATE TABLE IF NOT EXISTS videos (
        Channel_Name VARCHAR(150),
        Channel_id VARCHAR(100),
        Video_id VARCHAR(80) PRIMARY KEY,
        Title VARCHAR(150),
        Thumbnail VARCHAR(250),    
        Description_Date TIMESTAMP,
        Published_Date  TIMESTAMP,
        Views BIGINT,
        Likes BIGINT,
        Favorite_Count INT,
        Comments INT,
        Duration INTERVAL
       
    )'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for video_info in vi_data["video_information"]:
            vi_list.append(video_info)

    for video in vi_list:
        insert_query = '''INSERT INTO videos (Channel_Name,
                                              Channel_Id,
                                              Video_Id,
                                              Title,
                                              Thumbnail,
                                              Description,
                                              Published_Date,            
                                              Views,
                                              Likes,            
                                              Favorite_Count,
                                              Comments,
                                              Duration,)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        values = (
            video['Channel_Name'],
            video['Channel_Id'],
            video['Video_Id'],
            video['Title'],
            video.get('Thumbnail', ''),
            video['Description'],
            video.get('Published_Date', ''),           
            video['Views'],
            video.get('Likes', ''),           
            video.get('Favorite_Count', ''),
            video['Comments'],
            video['Duration'],
           
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.Error as e:
            st.write(f"Error inserting row: {e}")

    st.write("Videos values inserted in the table")




def comment_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="roomno13",
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments( Comment_Id VARCHAR(100) PRIMARY KEY,
                                                               Video_Id VARCHAR(80),
                                                               Comment_Text TEXT,
                                                               Comment_Author VARCHAR(150),
                                                               Comment_Published timestamp
                                                                )'''
        cursor.execute(create_query)
        
        mydb.commit()

    except Exception as e:
        st.write("Error creating table:", e)
        return

    com_list = []  # Assuming this is defined elsewhere
    # Assuming `client` is defined elsewhere for MongoDB
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        insert_query = '''
            INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
            VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            row.get('Comment_Id', ''),
            row.get('Video_Id', ''),
            row.get('Comment_Text', ''),
            row.get('Comment_Author', ''),
            row.get('Comment_Published', '')
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            st.write("Error inserting data:", e)
            continue

    cursor.close()
    mydb.close()

# Call the function to create the table and insert data





# create function for creating tables

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comment_table()
    return "Tables Created successfully"

def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"]
    coll1 = db["channels_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    playlist_table = st.dataframe(pl_list)
    return playlist_table

def show_videos_table():
      vi_list = []
      db = client["Youtube_data"]
      coll2 = db["channel_details"]
      for vi_data in coll2.find({},{"_id":0,"video_information":1}):
          for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

      df = pd.DataFrame(vi_list)
      videos_table = st.dataframe(df)
      return videos_table

def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df = pd.DataFrame(com_list)
    comment_table = st.dataframe(df)
    return comment_table



with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL USED ")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQL")

channel_id = st.text_input("Enter the Channel Id here")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")

        else:
            output = channel_details(channel)
            st.success(output)

if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)

show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))


if show_table == ":green[channels]":
    show_channels_table()

elif show_table == ":orange[playlists]":
    show_playlists_table()

elif show_table == ":red[videos]":
    show_videos_table()

elif show_table == ":blue[comments]":
    show_comments_table()




# FOR SQL CONNECTION

mydb = psycopg2.connect(host="localhost",
                            user = "postgres",
                            password = "roomno13",
                            database = "youtube_data",
                            port = "5432")
cursor = mydb.cursor()

question = st.selectbox(
    'Please Select your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are theircorresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what aretheir corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))



if question == '1. What are the names of all the videos and their corresponding channels':
    query1 = '''SELECT title AS Videos , channel_Name AS 'ChannelName' FROM videos;'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Videos", "ChannelName"]))


elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3.  What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Which videos have the highest number of likes, and what are theircorresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. What is the total number of likes and dislikes for each video, and what aretheir corresponding video names?':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7.  What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Channel_Name, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10.  Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))


cursor.close()
mydb.close()






#!/usr/bin/env python
# coding: utf-8

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pymongo
import psycopg2
import isodate
import pandas as pd
import streamlit as st

## api connection
api_key="enter your api key here"
api_service_name="youtube"
api_version="v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

## postgresql connection
ytproject=psycopg2.connect(host='localhost',user='postgres',password='password',database='youtube')   #enter your connection details
cursor=ytproject.cursor()

## mongodb atlas connection
project=pymongo.MongoClient("mongodb://waran:<password>@ac-bdx0zvp-shard-00-00.0teuoco.mongodb.net:27017,ac-bdx0zvp-shard-00-01.0teuoco.mongodb.net:27017,ac-bdx0zvp-shard-00-02.0teuoco.mongodb.net:27017/?ssl=true&replicaSet=atlas-10m62a-shard-0&authSource=admin&retryWrites=true&w=majority")


# # get channel details

def youtube_channel(youtube,channel_id):
    request= youtube.channels().list(
        id=channel_id,
        part="snippet,contentDetails,statistics"
       )
    response=request.execute()

    for item in response['items']:                                        # iterate through every item in "items" element in responce
        data={'channelName':item['snippet']['title'],                       #displaying only reqired items eg.nested in snippet-->title
              'channel_id':item['id'],                                      #the date acqiured from api is convert into dict 
              'subscribers':item['statistics']['subscriberCount'],
              'views':item['statistics']['viewCount'],
              'totalVideos':item['statistics']['videoCount'],
              'playlistId':item['contentDetails']['relatedPlaylists']['uploads'],
              'channel_description':item['snippet']['description']
            }
   
    return(data)


# # get play list details

def get_playlists(youtube,channel_id):
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25
        )
    response = request.execute()
    All_data=[]
    for item in response['items']: #these are the details we are getting about the playlist
        data={'PlaylistId':item['id'],
              'Title':item['snippet']['title'],
              'ChannelId':item['snippet']['channelId'],
              'ChannelName':item['snippet']['channelTitle'],
              'PublishedAt':item['snippet']['publishedAt'],
              'VideoCount':item['contentDetails']['itemCount']
              }
        All_data.append(data)

        next_page_token = response.get('nextPageToken')

        while next_page_token is not None:
            request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId="UCmXkiw-1x9ZhNOPz0X73tTA",
                    maxResults=25)
            response = request.execute()  
                      
            for item in response['items']: 
                data={'PlaylistId':item['id'],
                      'Title':item['snippet']['title'],
                      'ChannelId':item['snippet']['channelId'],
                      'PublishedAt':item['snippet']['publishedAt'],
                      'VideoCount':item['contentDetails']['itemCount']
                      }
                All_data.append(data)
            next_page_token = response.get('nextPageToken')
                
    return All_data


# # get video id

def channel_videoId(youtube,playlist_Id):
    video_Ids=[]
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_Id,
        maxResults=50
    )
    response = request.execute()

    for i in range(len(response['items'])):
        video_Ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while  more_pages:
        if next_page_token is None:
            more_pages= False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_Id,
                maxResults = 50,
                pageToken = next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                video_Ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')
    return video_Ids


# # time conversion

def format_duration(duration):
    duration_obj = isodate.parse_duration(duration)
    hours = duration_obj.total_seconds() // 3600
    minutes = (duration_obj.total_seconds() % 3600) // 60
    seconds = duration_obj.total_seconds() % 60

    formatted_duration = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return formatted_duration

# # get individual video details

def video_details(youtube, video_Id):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_Id,
    )
    response = request.execute()
    video_info = {}
    for video in response['items']:
        stats_to_keep = {
            'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt', 'channelId'],
            'statistics': ['viewCount', 'likeCount', 'favoriteCount', 'commentCount'],
            'contentDetails': ['duration', 'definition', 'caption']
        }
        
        video_info['video_id'] = video['id']
        for key in stats_to_keep.keys():
            for value in stats_to_keep[key]:
                try:
                    if key=='contentDetails' and value=='duration':
                        video_info[value]=format_duration(video[key][value])
                    else:
                        video_info[value] = video[key][value]              
                except KeyError:
                    video_info[value] = None

    return video_info


# # get comment details

def get_comments_in_videos(youtube, video_id):
    all_comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id
        )
        response = request.execute()

        for item in response['items']:
            data={'comment_id':item['snippet']['topLevelComment']['id'],
                  'comment_txt':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                  'videoId':item['snippet']['topLevelComment']["snippet"]['videoId'],
                  'author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  'published_at':item['snippet']['topLevelComment']['snippet']['publishedAt'],
            }

            all_comments.append(data)

    except:
        
        return 'Could not get comments for video '

    return all_comments


# # import data to MongoDb
db=project["youtube_project"]
col = db["Channels"]

@st.cache_data

def channel_Details(channel_id):
    det = youtube_channel(youtube, channel_id)
    col = db["Channels"]
    col.insert_one(det)

    playlist = get_playlists(youtube, channel_id)
    col = db["playlists"]
    for i in playlist:
        col.insert_one(i)

    upload = det.get('playlistId')  
    videos = channel_videoId(youtube, upload)
    for video in videos:
        v = video_details(youtube, video)
        col = db["videos"]
        col.insert_one(v)
        com = get_comments_in_videos(youtube, video)
        if com != 'Could not get comments for video ':
            for comment in com:
                col = db["comments"]
                col.insert_one(comment)
    
    return "Process for " + channel_id + " is completed"


def display_channels():
    db=project["youtube_project"]
    col=db["Channels"]
    channeltb=list(col.find())
    channeltb=st.dataframe(channeltb)
    return channeltb

def display_playlists():
    db=project["youtube_project"]
    col1=db["playlists"]
    playlisttb=list(col1.find())
    playlisttb=st.dataframe(playlisttb)
    return playlisttb
            
def display_videos():
    db=project["youtube_project"]
    col2=db["videos"]
    videotb=list(col2.find())
    videotb=st.dataframe(videotb)
    return videotb     
              
def display_comments():
    db=project["youtube_project"] 
    col3=db["comments"]
    commentstb=list(col3.find())
    commentstb=st.dataframe(commentstb)
    return commentstb


# # channel table creation

def channel_table():
    try:
        cursor.execute("""create table if not exists channel 
                        (channelName varchar(50),
                        channelId varchar(100) primary key,
                        subscribers bigint,
                        views bigint, 
                        totalVideos int,
                        playlistId varchar(80),
                        channel_description text)"""
                      )
        ytproject.commit()
    except:
        ytproject.rollback()

    db=project["youtube_project"]
    col=db["Channels"]
    data=col.find()
    doc=list(data)
    df=pd.DataFrame(doc)
    try:
        for _, row in df.iterrows():                     #iterate through each records
            insert_query = '''
                INSERT INTO channel (channelName, channelId, subscribers, views, totalVideos, playlistId, channel_description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['channelName'],
                row['channel_id'],
                row['subscribers'],
                row['views'],
                row['totalVideos'],
                row['playlistId'],
                row['channel_description']
            )
            try:
                cursor.execute(insert_query, values)
                ytproject.commit()
            except:
                ytproject.rollback()

    except:
        st.write("values already exist in channels table")


# # table for playlist
def playlist_table():
    try:
        cursor.execute("""create table if not exists playlists
               (PlaylistId varchar(80) primary key,
                Title varchar(100),
                ChannelId varchar(100),
                ChannelName varchar(50),
                PublishedAt timestamp,
                VideoCount int)""")
        ytproject.commit()
    except:
        project.rollback()
        
    col1=db["playlists"]
    data1=col1.find()
    doc1=list(data1)
    df1=pd.DataFrame(doc1)
    try:
        for _, row in df1.iterrows():
            insert_query = '''
                            INSERT INTO playlists (PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
                            VALUES (%s, %s, %s, %s, %s, %s)  '''       
                           
            values = (
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount']
                )
            try:
                cursor.execute(insert_query,values)
                ytproject.commit()
            except:
                ytproject.rollback()
    except:
        st.write("values already exist in playlist table") 


# # table for videos
def videos_table():
    try:
        cursor.execute("""create table if not exists videos
                        (video_id varchar(50)  primary key,
                        channelTitle varchar(150),
                        title varchar(150),
                        description text,
                        tags text,
                        publishedAt timestamp,
                        viewCount bigint,
                        likeCount bigint,
                        favoriteCount bigint,
                        commentCount int,
                        duration interval,
                        definition varchar(10),
                        caption varchar(10),
                        ChannelId varchar(100))""")
        ytproject.commit()
    except:
        ytproject.rollback()
        
    col2=db["videos"]
    data2=col2.find()
    doc2=list(data2)
    df2=pd.DataFrame(doc2)
    try:
        for _, row in df2.iterrows():
            insert_query = '''
                INSERT INTO videos (video_id, channelTitle,  title, description, tags, publishedAt, 
                viewCount, likeCount, favoriteCount, commentCount, duration, definition, caption, channelId)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['video_id'],
                row['channelTitle'],
                row['title'],
                row['description'],
                row['tags'],
                row['publishedAt'],
                row['viewCount'],
                row['likeCount'],
                row['favoriteCount'],
                row['commentCount'],
                row['duration'],
                row['definition'],
                row['caption'],
                row['channelId']
            )
            try:
                cursor.execute(insert_query,values)
                ytproject.commit()
            except:
                ytproject.rollback()
    except:
        st.write("values aready exists in the videos table")


# # table for comments
def comments_table():
    try:
        cursor.execute("""create table if not exists comments
               (comment_id varchar(100)  primary key,
               comment_txt text,
               videoId varchar(80),
               author_name varchar(150),
               published_at timestamp)""")
        ytproject.commit()
    except:
        project.rollback()
        
    col3=db["comments"]
    data3=col3.find()
    doc3=list(data3)
    df3=pd.DataFrame(doc3)
    try:
        for _, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (comment_id, comment_txt, videoId, author_name, published_at)
                VALUES (%s, %s, %s, %s, %s)
                '''
            values = (
            row['comment_id'],
            row['comment_txt'],
            row['videoId'],
            row['author_name'],
            row['published_at']
            )
            try:
                cursor.execute(insert_query,values)
                ytproject.commit()
            except:
                ytproject.rollback() 
    except:
           st.write("values already exist in comments table")        


# # wrapper function
def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()
    return ("done")   


#SQL queries    
    
def q1():
    cursor.execute("select channeltitle as channel_name, title as videos from videos;")
    ytproject.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1,columns=["channel_name","videos"]))
    
def q2():
    cursor.execute("select channelName as ChannelName,totalvideos as No_of_Videos from channel order by totalvideos desc;")
    ytproject.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2,columns=["ChannelName","No_of_Videos"]))

def q3():
    cursor.execute('''select  channeltitle as ChannelName,title as video_title ,viewcount as views from videos 
                    where viewcount is not null order by viewcount desc limit 10;''')
    ytproject.commit()
    t3=cursor.fetchall()
    st.write(pd.DataFrame(t3,columns=["ChannelName","video_title","views"]))

def q4():
    cursor.execute("select title as Name , commentcount as No_of_comments  from videos where commentcount is not null;") 
    ytproject.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4,columns=["Name","No_of_comments"]))
def q5():
    cursor.execute('''select channeltitle as ChannelName, title as Video_name, likecount as Likes from videos 
                   where likecount is not null order by likecount desc;''')
    ytproject.commit()
    t5=cursor.fetchall()
    st.write(pd.DataFrame(t5,columns=["ChannelName","Video_name","Likes"]))

def q6():
    cursor.execute('''select title as Video, likeCount as likes from videos;''')
    ytproject.commit()
    t6=cursor.fetchall()
    st.write(pd.DataFrame(t6,columns=["Video","likes"]))

def q7():
    cursor.execute("select channelName as ChannelName, views as Channelviews from channel;")
    ytproject.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7,columns=["ChannelName","Channelviews"]))

def q8():
    cursor.execute('''select channeltitle as ChannelName , title as Video_name, publishedat as Posted_On from videos 
                   where extract(year from publishedat) = 2022;''')
    ytproject.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["ChannelName","Video_name","Posted_On"]))
    
def q9():
    cursor.execute("""SELECT channeltitle as ChannelName, AVG(duration) AS average_duration FROM videos GROUP BY channeltitle;""")
    ytproject.commit()
    t9=cursor.fetchall()
    t9=pd.DataFrame(t9,columns=["ChannelName","average_duration"])
    tb9=[]
    for _, row in t9.iterrows():
        Channel_Name=row["ChannelName"]
        avg_duration=row["average_duration"]
        avg_str=str(avg_duration)
        tb9.append({"ChannelName":Channel_Name, "average_duration":avg_str})
    st.write(pd.DataFrame(tb9))

def q10():
    cursor.execute('''select channelTitle as ChannelName, title as Video_name, commentCount as Comments from videos 
                   where commentCount is not null order by commentCount desc;''')
    ytproject.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10,columns=["ChannelName","Video_name","Comments"]))

#streamlit
st.set_page_config(layout="wide")
st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

st.markdown(":red[Enter multiple channel IDs (comma-separated)]")
channel_ids=st.text_input("")
channel_ids= channel_ids.split(',')
channel_ids= [ch.strip() for ch in channel_ids if ch]

section1, section2 = st.columns(2)
with section1:
    if st.button("collect and store data in MongoDb"):
        for channel in channel_ids:
            query = {'channel_id': channel}
            document = col.find_one(query)
            if document:
                st.write("channel details already exists")
            else:
                output = channel_Details(channel)
                st.write(output)

with section2:
    if st.button("migrate data from MongoDb to Sql"):
        display=tables()
        st.write(display)
        
section1, section2 = st.columns([0.4,0.6])
with section1:
    st.markdown(":red[select the table you want to view]")
    frames=st.radio("", 
    ("none","Channel" ,"Playlist", "video", "Comment"))

    st.write("You selected:", frames)

    if frames=="none":
        st.write( "select an option")
    elif frames=="Channel":
        display_channels()
    elif frames=="Playlist":
         display_playlists()
    elif frames=="video":
         display_videos()
    elif frames=="Comment":
         display_comments()
 
with section2:
    st.markdown(":red[To view queried data]")
    query=st.selectbox("",
    ("none","channels and all it's videos","channels with highest no. of videos",
     "top 10 most viewed videos","No.of comments in each video","videos with most no. of likes",
     "no. of likes of all videos","Total views of each channel","videos published in year 2022",
     "average duration of videos in channels","videos with highest no. of comments"))


    if query=="none":
        st.write("select an option")
    if query=="channels and all it's videos":
        q1()
    elif query=="channels with highest no. of videos":
        q2()
    elif query=="top 10 most viewed videos":
        q3()
    elif query=="No.of comments in each video":
        q4()
    elif query=="videos with most no. of likes":
        q5()
    elif query=="no. of likes of all videos":
        q6()
    elif query=="Total views of each channel":
        q7()
    elif query=="videos published in year 2022":
        q8()
    elif query=="average duration of videos in channels":
        q9()
    elif query=="videos with highest no. of comments":
        q10()







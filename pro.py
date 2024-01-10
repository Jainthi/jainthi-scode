from googleapiclient.discovery import build
import pymongo 
from pymongo import MongoClient
import psycopg2 
import pandas as pd 
import streamlit as st

#api key connection

def api_connect():
    api_Id = "AIzaSyCrjEnWXlmDw-HyyYXVBdfUh_MG5m5ORlM"

    api_service_name="youtube"
    api_version="v3"

    youtube = build(api_service_name,api_version, developerKey = api_Id)
    return youtube

youtube = api_connect()

# get channel information
def get_channel_info(channel_Id):
    request=youtube.channels().list(

                part="snippet, contentDetails, statistics",
                id=channel_Id

    )
    response= request.execute()

    for i in response['items']:
        data=dict(channel_Name=i["snippet"]["title"],
                channel_Id=i["id"],
                subscribers=i['statistics']['subscriberCount'],
                views=i['statistics']['viewCount'],
                total_videos=i['statistics']['videoCount'],
                description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
        return data 

#get video id's
def get_video_id(channel_Id):
    video_ids=[]
    response=youtube.channels().list(
        id=channel_Id,
        part='contentDetails'
    ).execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# get each and every video information on a specific channel
def get_video_details(videos_ids):
    video_data=[]
    for videoid in videos_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videoid
        )
        response=request.execute()

        for item in response['items']:
            data=dict(channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_id=item['id'],
                    video_title=item['snippet']['title'],
                    description=item['snippet'].get('description'),
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    publish_date=item['snippet']['publishedAt'],
                    video_duration=item['contentDetails']['duration'],
                    views=item['statistics'].get('viewCount'),
                    likes=item['statistics'].get('likeCount'),
                    comments=item['statistics'].get('commentCount'),
                    fav_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    status=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data

#get comment details for every videos on the above channel
def get_comment_info(videos_ids):
    comment_data=[]
    try:
        for v_id in videos_ids:
            request = youtube.commentThreads().list(
                    part='snippet',
                    videoId= v_id,
                    maxResults=50
                )
            response = request.execute()
            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        video_id=item['snippet'][ 'topLevelComment']['snippet'][ 'videoId'],
                        comment_text=item['snippet'][ 'topLevelComment']['snippet']['textDisplay'],
                        comment_author=item['snippet'][ 'topLevelComment']['snippet']['authorDisplayName'],
                        comment_date=item['snippet'][ 'topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    return comment_data

#get video playlist details
def get_playlist_details(channel_Id):
    next_page_token=None
    all_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_Id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(playlistid=item['id'],
                    title=item['snippet']['title'],
                    channael_id=item['snippet']['channelId'],
                    channel_name=item['snippet']['channelTitle'],
                    publish_date=item['snippet']['publishedAt'],
                    videocount=item[ 'contentDetails']['itemCount'])
            all_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_data


#connect to Mongo DB
client= pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]

def channel_details(channel_Id):
    channel_details=get_channel_info(channel_Id)
    playlist_detail=get_playlist_details(channel_Id)
    video_details=get_video_id(channel_Id)
    video_info=get_video_details(video_details)
    comment_detail=get_comment_info(video_details)


    collection1=db["channel_details"]
    collection1.insert_one({"channel_information":channel_details,
                            "playlist_information":playlist_detail,
                            "video_information":video_info,
                            "comment_info":comment_detail})
    return "uploaded completed successfully"

# to get channel details

def channels_table():
    mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="jainthiyuva",
                    database= "youtube_data",
                    port=5432
                        )
    mycursor = mydb.cursor()

    # to insert multiple channels into sql

    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()


    try:
        create_query='''create table if not exists channels(channel_Name varchar(200),
                                                        channel_Id varchar(80) primary key,
                                                        subscribers bigint,
                                                        views bigint,
                                                        total_videos int,
                                                        description text,
                                                        playlist_id varchar(80))'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        print('channel table created')

    ch_mongo_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_mongo_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_mongo_list)

    for index,row in df.iterrows():
        
        insert_query= '''insert into channels(channel_Name,
                                                channel_Id,
                                                subscribers,
                                                views,
                                                total_videos,
                                                description,
                                                playlist_id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['channel_Id'],
                row['subscribers'],
                row['views'],
                row['total_videos'],
                row['description'],
                row['playlist_id'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('channel uploaded')

def playlists_table():

        mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="jainthiyuva",
                                database= "youtube_data",
                                port=5432
                                )
        mycursor = mydb.cursor()


        drop_query='''drop table if exists playlists'''
        mycursor.execute(drop_query)
        mydb.commit()

        # create table to insert playlist details

        create_query='''create table if not exists playlists(playlistid varchar(100) primary key,
                                                                title varchar(100),
                                                                channael_id varchar(100),
                                                                channel_name varchar(100),
                                                                publish_date timestamp,
                                                                videocount int
                                                                )'''
        mycursor.execute(create_query)
        mydb.commit()

        pl_mongo_list=[]
        db=client["youtube_data"]
        collection1=db["channel_details"]
        for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_mongo_list.append(pl_data["playlist_information"][i])
        df1=pd.DataFrame(pl_mongo_list)

        for index,row in df1.iterrows():
                insert_query= '''insert into playlists(playlistid,
                                                        title,
                                                        channael_id,
                                                        channel_name,
                                                        publish_date,
                                                        videocount)
                                                                
                                                        values(%s,%s,%s,%s,%s,%s)'''
                values=(row['playlistid'],
                        row['title'],
                        row['channael_id'],
                        row['channel_name'],
                        row['publish_date'],
                        row['videocount'])
                
                mycursor.execute(insert_query,values)
                mydb.commit()

#get video details
def videos_table():
        mydb = psycopg2.connect(host="localhost",
                                        user="postgres",
                                        password="jainthiyuva",
                                        database= "youtube_data",
                                        port=5432
                                        )
        mycursor = mydb.cursor()

        drop_query='''drop table if exists videos'''
        mycursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists videos(channel_name varchar(100),
                                                        channel_id varchar(100),
                                                        video_id varchar(20) primary key,
                                                        video_title varchar(100),
                                                        description text,
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        publish_date timestamp,
                                                        video_duration interval,
                                                        views bigint,
                                                        likes bigint,
                                                        comments int,
                                                        fav_count int,
                                                        Definition varchar(100),
                                                        status varchar(50))'''
        mycursor.execute(create_query)
        mydb.commit()

        vi_mongo_list=[]
        db=client["youtube_data"]
        collection1=db["channel_details"]
        for vi_data in collection1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_mongo_list.append(vi_data["video_information"][i])
        df2=pd.DataFrame(vi_mongo_list)

        for index , row in df2.iterrows():
                        insert_query= '''insert into videos(channel_name,
                                                                channel_id,
                                                                video_id,
                                                                video_title,
                                                                description,
                                                                Tags,
                                                                Thumbnail,
                                                                publish_date,
                                                                video_duration,
                                                                views,
                                                                likes,
                                                                comments,
                                                                fav_count,
                                                                Definition,
                                                                status
                                                                )
                                                                        
                                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                        values=(row['channel_name'],
                                row['channel_id'],
                                row['video_id'],
                                row['video_title'],
                                row['description'],
                                row['Tags'],
                                row['Thumbnail'],
                                row['publish_date'],
                                row['video_duration'],
                                row['views'],
                                row['likes'],
                                row['comments'],
                                row['fav_count'],
                                row['Definition'],
                                row['status'])
                        mycursor.execute(insert_query,values)
                        mydb.commit()

def comments_table():
        mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="jainthiyuva",
                                database= "youtube_data",
                                port=5432
                                )
        mycursor = mydb.cursor()


        drop_query='''drop table if exists comments'''
        mycursor.execute(drop_query)
        mydb.commit()

        # create table to insert playlist details

        create_query='''create table if not exists comments(comment_id varchar(100) primary key,
                                                                video_id varchar(100),
                                                                comment_text text,
                                                                comment_author varchar(150),
                                                                comment_date timestamp
                                                                )'''
        mycursor.execute(create_query)
        mydb.commit()

        com_mongo_list=[]
        db=client["youtube_data"]
        collection1=db["channel_details"]
        for com_data in collection1.find({},{"_id":0,"comment_info":1}):
                for i in range(len(com_data["comment_info"])):
                        com_mongo_list.append(com_data["comment_info"][i])
        df3=pd.DataFrame(com_mongo_list)

        for index,row in df3.iterrows():
                insert_query= '''insert into comments(comment_id,
                                                        video_id,
                                                        comment_text,
                                                        comment_author,
                                                        comment_date)
                                                                        
                                                        values(%s,%s,%s,%s,%s)'''
                values=(row['comment_id'],
                                row['video_id'],
                                row['comment_text'],
                                row['comment_author'],
                                row['comment_date'])
                mycursor.execute(insert_query,values)
                mydb.commit()


def sql_table():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return "tables created successfully"

# streamlit connection
def ch_show_table():
    ch_mongo_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_mongo_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_mongo_list)
    return df

def pl_show_table():
    pl_mongo_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(pl_data["playlist_information"])):
                    pl_mongo_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_mongo_list) 
    return df1

def vi_show_table():
    vi_mongo_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                    vi_mongo_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_mongo_list)
    return df2

def com_show_table(): 
        com_mongo_list=[]
        db=client["youtube_data"]
        collection1=db["channel_details"]
        for com_data in collection1.find({},{"_id":0,"comment_info":1}):
                for i in range(len(com_data["comment_info"])):
                        com_mongo_list.append(com_data["comment_info"][i])
        df3=st.dataframe(com_mongo_list)
        return df3

#streamlit 
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("PROCEDURE")
    st.caption("Get channel details from YOUTUBE")
    st.caption("Insert into MONGO DB")
    st.caption("Transfer to SQL")
    st.caption("Show through STREAMLIT")

channel_ID= st.text_input("ENTER THE CHANNEL ID ")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]['channel_Id'])

        if channel_ID in ch_ids:
            st.success("channel details of the given channel already exists")
        else:
            insert=channel_details(channel_ID)
            st.success(insert)

if st.button("MIGRATE TO SQL"):
    Table=sql_table()
    st.success(Table)

show_table= st.radio("select the table",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    ch_show_table()

elif show_table=="Playlists":
    pl_show_table()

elif show_table =="Videos":
    vi_show_table()

elif show_table =="Comments":
    com_show_table()

# sql connection
mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="jainthiyuva",
                        database= "youtube_data",
                        port=5432
                        )
mycursor = mydb.cursor()

questions=st.selectbox("Select Your Question",("1. All the videos and channel name",
                                               "2. channels with most number of videos",
                                               "3. 10 most viewed videos",
                                               "4. comments in each video",
                                               "5. Videos with highest likes",
                                               "6. Likes of all videos",
                                               "7. views of each channel",
                                               "8. videos published in the year of 2022",
                                               "9. Average duration of all videos in each channel"
                                               "10. Videos with highest number of comments"))

if questions =="1. All the videos and channel name":
    query1='''select video_title as videos,channel_name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["Video Title","Channel Name"])
    st.write(df)

elif questions =="2. channels with most number of videos":
    query2='''select channel_Name as channelname,total_videos as no_videos from channels
                order by total_videos desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel name","No. of videos"])
    df2
    st.write(df2)

elif questions =="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,video_title as title from videos
                where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","Channel name","video title"])
    st.write(df3)

elif questions=="4. comments in each video":
    query4='''select comments as no_comments,video_title as title from videos
                where comments is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No. of comments","video title"])
    st.write(df4)

elif questions=="5. Videos with highest likes":
    query5='''select video_title as title,channel_name as channelname ,likes as total_likes from videos
                where likes is not null order by likes desc'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Video Title","channel name","Total Likes"])
    st.write(df5)

elif questions=="6. Likes of all videos":
    query6='''select video_title as title,channel_name as channelname ,likes as total_likes from videos'''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Video Title","channel name","Total Likes"])
    st.write(df6)

elif questions=="7. views of each channel":
    query7='''select channel_name as channelname, views as view_count from channels'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","Total Views"])
    st.write(df7)

elif questions=="8. videos published in the year of 2022":
    query8='''select channel_name as channelname, video_title as title, publish_date as video_released from videos
                where extract(year from publish_date) = 2022'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["channel name","Video Title","Published Date"])
    st.write(df8)

elif questions=="9. Average duration of all videos in each channel":
    query9='''select channel_name as channelname, avg(video_duration) as video_time from videos
                group by channel_name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","video_time"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["video_time"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif questions=="10. Videos with highest number of comments":
    query10='''select channel_name as channelname, video_title as title, comments as comments from videos
                where comments is not null order by comments desc'''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["channelname","video_title","highest comments"])
    st.write(df10)


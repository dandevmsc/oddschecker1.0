import os
import pandas as pd

def twitterfollower(file_location):
        if not os.path.exists(file_location):
            sys.exit("file not found")

        else:
            df=pd.read_csv(file_location,nrows=20)
            df.columns=['follower_ID']
            df.insert(0, 'following', os.path.basename(file_location).split("_")[3].split(".")[0])
            df.insert(0, 'timestamp', pd.datetime.utcnow().replace(hour=0,minute=0,second=0,microsecond=0))
            from sqlalchemy import create_engine
            engine = sqlalchemy.create_engine("mssql+pyodbc://localhost/MSSQLSERVER01/Politics")
            df.to_sql('twitter_follower_id', con=engine,if_exists="append",index=False)
            
            #insert_odds= "'),('".join(df['follower_ID'].map(str)+"','"+df['following'].map(str)+"','"+df['timestamp'].map(str))
            #odds_table="Merge Into Politics.dbo.odds as Target using (values('"+insert_odds+"')) as source(follower_ID,following,timestamp) on Target.follower_ID=Source.follower_ID and Target.following=Source.following and Target.timestamp=Source.timestamp when not matched by target then Insert(follower_ID,following,timestamp) values (source.follower_ID,source.following,source.timestamp);"
            #print(odds_table)
            #conn=pyodbc.connect('Driver={SQL Server};Server=localhost\MSSQLSERVER01;Datebase=Politics;Trusted_Connection=yes;')
            #cursor=conn.cursor()
            
twitterfollower("C:\\Users\\daniele\\Documents\\Twitter followers\\twitter_followers_09022020_labour.txt")


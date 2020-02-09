def odds(file_location):
        if not os.path.exists(file_location):
            sys.exit("file not found")

        else:
            df=pd.read_csv(file_location)
            conn=pyodbc.connect('Driver={SQL Server};Server=localhost\MSSQLSERVER01;Datebase=Politics;Trusted_Connection=yes;')
            cursor=conn.cursor()
            unique={}

            print(df.columns)
            for i in ["sport","tournament","match","bettype","name","broker"]:
                df1=df[i].drop_duplicates()
                df1=df1.dropna()
                df1="'),('".join(df1.map(str))
                #print(df1)
                unique[i]=df1

                brokers_table="Merge Into Politics.dbo."+str(i)+" as Target using (values('"+str(unique[i])+"')) as source("+str(i)+") on Target."+str(i)+"=Source."+str(i)+" when not matched by target then Insert("+str(i)+") values (source."+str(i)+");"
                #print(brokers_table)
                cursor.execute(brokers_table)
                cursor.commit()
               
                cursor.execute("select ["+i+"Id],"+i+" from Politics.dbo."+str(i))
                namedict=dict(cursor.fetchall())
                
                namedict={v:k for k, v in namedict.items()}
                print(namedict)
                df[i]=df[i].map(namedict,na_action='ignore')
                print(df[i])
                



           
            #print(df.columns)
            df_odds=df.drop_duplicates()
            df_odds=df.dropna(subset=['broker'])
            df_odds['broker']=df_odds['broker'].astype(int)
#             #tournament table
            insert_odds= "'),('".join(df_odds['sport'].map(str)+"','"+df_odds['tournament'].map(str)+"','"+df_odds['match'].map(str)+"','"+df_odds['bettype'].map(str)+"','"+df_odds['name'].map(str)+"','"+df_odds['broker'].map(str)+"','"+df_odds['bestdig'].map(str)+"','"+df_odds['link'].map(str)+"','"+df_odds['timestamp'].map(str))
            odds_table="Merge Into Politics.dbo.odds as Target using (values('"+insert_odds+"')) as source(sport,tournament,match,bettype,name,broker,bestdig,link,timestamp) on Target.sport=Source.sport and Target.tournament=Source.tournament and Target.match=Source.match and Target.bettype=Source.bettype and Target.name=Source.name and Target.timestamp=Source.timestamp when not matched by target then Insert(sport,tournament,match,bettype,name,broker,bestdig,link,timestamp) values (source.sport,source.tournament,source.match,source.bettype,source.name,source.broker,source.bestdig,source.link,source.timestamp);"
            #print(odds_table)
            cursor.execute(odds_table)
            cursor.commit()

            cursor.close()
            print("upload Complete")



odds("C:\\Users\\user1\\Documents\\Odds\\odds"+datetime.now().strftime('%Y-%m-%d')+".csv")

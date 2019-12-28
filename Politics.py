


import requests
from bs4 import BeautifulSoup as soup
import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta, date
import csv
import re
import os
import time
import itertools
import pyodbc
import argparse
from random import randint


def make_soup(url):
    thepage=requests.get(url,headers={'User-Agent': 'Mozilla/5.0'}).text
    soupdata=soup(thepage,"html.parser")
    time.sleep(randint(15,23))
    return soupdata

def eventcategory(my_url,sport):
    politicsurls= []
    politics=make_soup(my_url+sport)
    for tlink in politics.find_all('ul',{'class':'all-events standard-list'}):
        for tlink1 in (tlink.find_all('a')):
            politicsurls.append(my_url+tlink1.get('href'))
    return politicsurls

def eventsubcategory(url,findotherurl):
    politicsurls2=[]
    for i in url:
        politicstype=make_soup(i)
        for t in politicstype.find_all('a',{'class':findotherurl}):
            #t.get('href')
            politicsurls2.append(t.get('href'))
    return politicsurls2


def getodds(my_url,urls2,brokermap):
    tbestbks=[]
    tbestdig=[]
    tdatabname=[]
    bettype=[]
    sport1=[]
    match=[]
    tournament=[]
    link=[]
    for i in urls2:
        scores=make_soup(my_url+i)
        for tdatabest in (scores.findAll("tr",{"class":"diff-row evTabRow bc"})):
                
                print(tdatabest)
                for j in tdatabest.findAll("td",{"class":"bc bs o"}):
                    link.append(my_url+i)
                    tbestbks.append(j.get('data-bk'))
                    tbestdig.append(1/float(j.get('data-odig')))
                    tdatabname.append(tdatabest.get('data-bname'))
                    match.append(i.split("/")[2])
                    sport1.append(i.split("/")[1])
                    #tournament.append(i.split("/")[5])
                    if len(i.split("/")) == 5 :
                        bettype.append(i.split("/")[4])
                        tournament.append(i.split("/")[3])
                    elif len(i.split("/")) == 4 :
                        bettype.append(i.split("/")[3])
                        tournament.append("Null")

   
    tbestbks1=[",".join(x) for x in replacebroker(tbestbks,brokermap)]
    return pd.DataFrame(list(itertools.zip_longest(link,tbestbks1 ,tbestdig,tdatabname,sport1,match,bettype,tournament)),columns=['link','broker', 'bestdig', 'name','sport','match','bettype','tournament'])
   
   
def replacebroker(list, dictionary):
    new_list1 = []
    for i1, inner_1 in enumerate(list):
        new_list = []
        inner_1=(inner_1.split(","))
        for i2,item in enumerate(inner_1):
            if item in dictionary:
                new_list.append(dictionary[item])
            else:
                new_list.append(item)
        new_list1.append(new_list)
    return new_list1


my_url = 'https://www.oddschecker.com/'

brokermap={'B3':'Bet365','SK':'SkyBet','LD':'LadBrokes','WH':'Williamhill','MR':'Marathonbet','FB':'BetFair','VC':'Betvictor','PP':'PaddyPower','UN':'Unibet',
           'CE':'Coral','FR':'Betfred','WA':'Betway','BL':'Blacktype','RZ':'Redzone','BY':'Boylesports','PE':'Sportspesa','OE':'10bet','SO':'Sportingbet','EB':'Sportingbet','EE':'188bet','BB':'Betbright','SX':'spreadex','RY':'royalpanda','SA':'Sportnation','BF':'Betfairexchange','BD':'BetDaq','MA':'Matchbook','MK':'SmartMarkets'}

firsturls=eventcategory(my_url,'politics')
secondurls=eventsubcategory(firsturls,'minitable-header beta-h3')

otherurls=eventsubcategory(firsturls,'outright-text no-odds')

odds=getodds(my_url,secondurls,brokermap)
#print(odds)
oddssupplement = getodds(my_url,otherurls,brokermap)
print(oddssupplement)
odds=odds.append(oddssupplement)
odds=odds.rename(columns={"broker": "brokerlist"})
odds['brokerlist']=odds['brokerlist'].str.split(",").tolist()
odds=odds.assign(broker=odds['brokerlist'].values.tolist()).explode('broker').reset_index(drop=True)
odds=odds.drop(columns='brokerlist')
odds.insert(0, 'timestamp', pd.datetime.utcnow().replace(microsecond=0))
odds.to_csv("C:\\Users\\user1\\Documents\\Odds\\odds"+datetime.utcnow().strftime('%Y-%m-%d')+".csv",index=False)


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



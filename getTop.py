# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 01:03:25 2014

@author: Greg
"""

from bs4 import BeautifulSoup
import urllib
import pandas as pd
from pandas import Series
import numpy as np
import string
import re
import os
import json

def getNewTop100():
    # Get the current top 100 trails from singletracks.com. 
    # Returns a list of dicts containing rank (#), trail name (Trail),
    # city (Location), URL

    link = "http://www.singletracks.com/mountain-bike/best_trails.php"
    f = urllib.request.urlopen(link)
    topHTML = f.read()
    
    soup = BeautifulSoup(topHTML)
    table = soup.find(id="myTable")
    rows = table.findAll('tr')
    links=["http://www.singletracks.com/" + row.findAll('td')[1].find('a').attrs['href'] for row in rows[1:]]
    
    # read in html table as df
    top = pd.read_html(str(table),infer_types=False)[0]
    top['URL'] = links
    top['#'] = top['#'].astype(int)
    del top['Difficulty']
    del top['Rating']
    top = top.to_dict("records") # to a list of dicts
    
    return top

def updateDB(coll, top):
    # Determine which trails are not in the database (we need to get latlng for)
    # and then add to DB.
    # If already in DB, update ranking
    for trail in top:
        # set the rank of the trail that used to be # top['#'][rowIdx] to NaN
        coll.update({'#':trail['#']},{"$set": {"#":"NaN"}})
        
        if coll.find({'URL':trail['URL']}).count() == 0: #not in db
            #get detailed stats
            print('Adding: ' + trail['Trail'])
            trail = getTrailInfo(trail)
            # insert new trail
            coll.update({'URL':trail['URL']}, trail, upsert = True)
        else:
            print('Already in DB: ' + trail['Trail'])
            #update ranking only
            coll.update({'URL':trail['URL']}, {"$set": {"#":trail['#']}})

def getTrailInfo(trail):
    # url = list(coll.find({'#':1}))[0]['URL']
    f = urllib.request.urlopen(trail['URL'])

    trail['html'] = str(f.read())

    soup = BeautifulSoup(trail['html'])
    stat = soup.findAll('div',attrs = {"class":"\\\'st_stat1\\\'"})
    trail['distance'] = int(stat[0].getText().strip(string.ascii_letters))
    try:
        trail['ascent'], trail['descent'] = [int(x.replace(',','')) for x in re.findall("[-,0-9]+",stat[3].getText())]
    except:
        trail['ascent'], trail['descent'] = [0, 0]
    trail['rating'] = float(soup.find('span',attrs = {"class":"\\\'average\\\'"}).getText())
    stat = soup.findAll('div',attrs = {"class":"span8"})[2].findAll("div")[0]
    trail['picURL'] = re.findall("(http://\S*.jpg)", str(stat))[0]

    latlng = re.findall("google.maps.LatLng\(\S\S(-?\d+[.]\d+)\S\S[,]\s\S{2}(-?\d+[.]\d+)",trail['html'])
    try:    
        latlng = [float(x) for x in latlng[0]]
    except:
        latlng = [0.,0.]
    
    (trail['Lat'], trail['Lng']) = latlng
    
    des = soup.find('div', attrs = {'id':"\\\'st_description\\\'"}).getText()  
    trail['description'] = des.replace('\\n','').replace('\\t','').replace("\\'","'")
    return trail
    
    #trailHTML[83832:83832+50]
    #"google.maps.LatLng(\\'39.400500\\', \\'-105.167880\\')"
    #google.maps.LatLng('39.310090', '-108.705730')

def getMongo():
    # Return the top100 collection     
    import pymongo
    from pymongo import MongoClient
    import json
    client = MongoClient()
    db = client.singletracks
    coll = db.top100
    return coll
    
def showTopX(coll, x):
    print(pd.DataFrame(list(coll.find({'#':{"$lt":x+1}}))))
    
def getNumEntries(coll):
    return coll.find().count()

def addDescription(coll):
    #added new field. use this to update db without redoing redownloading everything.
    for trail in coll.find():
        soup = BeautifulSoup(trail['html'])
        des = soup.find('div', attrs={'id':"\\\'st_description\\\'"}).getText()  
        trail['description'] = des.replace('\\n','').replace('\\t','').replace("\\'","'")
        coll.update({'URL':trail['URL']}, {"$set": {"description":trail['description']}})

def updateLastUpdated(pathToMaps):
    #input: path to maps.html
    import datetime

    with open(pathToMaps, 'r') as f:
        mapsHTML = f.read()
    
    soup = BeautifulSoup(mapsHTML)
    soup.find('span',attrs={'id':'last_updated'}).string  = str(datetime.date.today())
    
    html = soup.prettify("utf-8")
    with open(pathToMaps, "wb") as file:
        file.write(html)

def uploadChanges(dirPath):
    import ftplib
    #Get password from config file so we don't share it with the world
    with open('password.txt', 'r') as f:
        password = f.read()
    session = ftplib.FTP('stuppie.com','stuppie', password)
    file = open(dirPath + os.path.sep + 'map.html','rb')                  # file to send
    session.storbinary('STOR map.html', file)     # send the file
    file.close()                                    # close file and FTP
    file = open(dirPath + os.path.sep + 'markers.js','rb')                  # file to send
    session.storbinary('STOR markers.js', file)     # send the file
    file.close()  
    
    print(session.quit())
    
if False:
    # insert a bunch of records at once    
    records = json.loads(top.to_json(orient="records"))
    coll.insert(records)
    # test
    for d in test:
        coll.update({'URL':d['URL']}, d, upsert = True)

if __name__ == "__main__":
    coll = getMongo()
    top = getNewTop100()
    updateDB(coll, top)
    
    dirPath = r'C:\Users\Greg\Documents\stuppie.com'
    with open(dirPath + os.path.sep + "markers.js", "w") as text_file:
        print("var markers = " + json.dumps(list(coll.find({'#':{'$lte':100}},{'html':0, "_id":0}).sort('#',1))), file = text_file)
    
    updateLastUpdated(dirPath + os.path.sep + 'map.html')
    uploadChanges(dirPath)
    

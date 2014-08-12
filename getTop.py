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

def getNewTop100():
    # Get the current top 100 trails from singletracks.com. 
    # Returns a pandas dataframe containing rank (#), trail name (Trail),
    # city (Location), URL, Lat and Lng initialized to zero

    link = "http://www.singletracks.com/mountain-bike/best_trails.php"
    f = urllib.request.urlopen(link)
    topHTML = f.read()
    
    soup = BeautifulSoup(topHTML)
    table = soup.find(id="myTable")
    rows = table.findAll('tr')
    links=["http://www.singletracks.com/" + row.findAll('td')[1].find('a').attrs['href'] for row in rows[1:]]
    
    # read in html table as df
    top = pd.read_html(str(table),infer_types=False)[0]
    top['URL']=links
    del top['Difficulty']
    del top['Rating']
  
    top['Lat'] = Series(np.zeros(len(top['URL'])))
    top['Lng'] = Series(np.zeros(len(top['URL'])))
    
    return top

def updateDB(coll, top):
    # Determine which trails are not in the database (we need to get latlng for)
    # and then add to DB.
    # If already in DB, update ranking
    import json
    for rowIdx, trailURL in top['URL'].T.iteritems():
        
        # set the rank of the trail that used to be # top['#'][rowIdx] to NaN
        coll.update({'#':int(top['#'][rowIdx])},{"$set": {"#":"NaN"}})
        
        if coll.find({'URL':trailURL}).count() == 0: #not in db
            #get lat & lng
            print('Adding: ' + top['Trail'][rowIdx])
            trail = getTrailInfo(trailURL)
            top['Lat'][rowIdx]=trail['Lat']
            top['Lng'][rowIdx]=trail['Lng']
            
            # insert new trail
            trail = json.loads(top.loc[rowIdx].to_json())
            trail['#']=int(trail['#'])
            coll.update({'URL':trail['URL']}, trail, upsert = True)
        else:
            print('Already in DB: ' + top['Trail'][rowIdx])
            trail = json.loads(top.loc[rowIdx].to_json())
            trail['#']=int(trail['#'])
            #update ranking
            coll.update({'URL':trail['URL']}, {"$set": {"#":trail['#']}})

def getTrailInfo(url):
    # url = list(coll.find({'#':1}))[0]['URL']
    import re
    import urllib
    trail = dict()    
    f = urllib.request.urlopen(url)
    trailHTML = str(f.read())
    
    (trail['Lat'], trail['Lng']) = getLatLng(trailHTML)
    trail['html'] = trailHTML
    
    return trail
    

def getLatLng(trailHTML):
    latlng = re.findall("google.maps.LatLng\(\S\S(-?\d+[.]\d+)\S\S[,]\s\S{2}(-?\d+[.]\d+)",trailHTML)
    try:    
        latlng = [float(x) for x in latlng[0]]
    except:
        latlng = [0.,0.]
    
    return latlng
    
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

if False:
    # insert a bunch of records at once    
    records = json.loads(top.to_json(orient="records"))
    coll.insert(records)
    # test
    for d in test:
        coll.update({'URL':d['URL']}, d, upsert = True)

if __name__ == "X__main__":
    coll = getMongo()
    top = getNewTop100()
    updateDB(coll, top)

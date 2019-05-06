
# coding: utf-8

# In[ ]:


from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import operator 
import fiona
import fiona.crs
import shapely
import rtree


def processTweets(pid, records):

    
    import geopandas as gpd
    import sys
    import shapely.geometry as geo
    import rtree

    cities = gpd.read_file(cityData)
    count = {}
    index1 = rtree.Rtree()
    for idx,geometry in enumerate(cities.geometry):
        index1.insert(idx, geometry.bounds)

        
    for row in records:
        
        pdt = row.split('|')
        try:
           
            p1 = geo.Point(float(pdt[1]), float(pdt[2]))
            line =pdt[-1]
            line = separator.split(line.lower())
            
            tract = 0    
            match1 =  list(index1.intersection((p1.x, p1.y)))
            population = 1
            found = None

            for drug in drugs:
                if len(set(drug.split()).intersection(line))==len(drug.split()): 
                    for idx in match1:
                        shape = cities.geometry[idx]
                            if shape.contains(p1):
                                found = idx
                                tract = cities.plctract10[idx]
                                population = cities.plctrpop10[idx]
                                break
                    if found:
                         count[tract] = count.get(tract, 0) + (1/population)                           
        except:
            pass
        
    return list((count.items()))

            
       

if __name__ == "__main__":
        
    
    sc = SparkContext()
    tweets = sys.argv[1]
    drugs1 = sys.argv[2]
    drugs2 = sys.argv[3]
    cityData = sys.argv[4]

    drugs = (open(drugs1, 'r').readlines() + open(drugs2, 'r').readlines())
    drugs = set(map(lambda x: x.strip(),drugs))
    
    counts = sc.textFile(tweets, use_unicode=True).cache()            .mapPartitionsWithIndex(processTweets)             .reduceByKey(lambda x: x+y)             .sortBy(lambda x: -x[1])                  
    counts.saveAsTextFile('counts')


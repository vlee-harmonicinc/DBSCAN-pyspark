
# coding: utf-8

# ## to import GraphFrame
# ##### download and unzip tar spark-1.6.3-bin-hadoop2.6
# ##### export SPARK_HOME="/usr/local/bin/spark-1.6.3-bin-hadoop2.6"
# ##### export PATH=/home/vagrant/hadoop

# In[ ]:

import random, operator, subprocess
from pyspark.sql.types import *
from graphframes import *
import numpy as np
from datetime import datetime, timedelta


# In[ ]:

from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
sqlContext=SQLContext(sc)


# In[ ]:

k = 10
dimension = 3
input_filename = 'data.csv'
output_file = 'output.csv'
eps_record_filename = 'eps_record.csv'
eps_range = np.arange(6,7, 1)


# In[ ]:

input_filename = 's3n://spark-data-dbscan/data10k_6attr.csv'
output_filename = 's3n://spark-data-dbscan/output.csv'
dimension = 6
eps_range = np.arange(10,20, 1)


# In[ ]:

minPts = k
headers = ['age', 'height', 'weight', 'blood_sugar_level', 'child', 'exercise_hours']


# In[ ]:

rdd = sc.textFile(input_filename)         .map(lambda line: line.split(','))         .map(lambda elements: tuple([int(elements[i]) for i in range(len(elements))]))         .cache()


# In[ ]:

def dist(x, y):
    return sum([abs(x[i]-y[i]) for i in range(dimension)])

def get_nearest_centroid_idx(x, centroids):
    dists = {}
    for cluster in centroids:
        dists[cluster] = dist(x, centroids[cluster])
        
    cluster = min(dists, key=dists.get)
    return cluster

def assign_to_cluster(pt, available_centroids):
    nearest_centroid = get_nearest_centroid_idx(pt, available_centroids)
    return (nearest_centroid, ([pt], [dist(pt, available_centroids[nearest_centroid])]))

def calculate_pts_sum(pts):
    pts_sum = [0 for _ in range(dimension)]
    for pt in pts:
        for i in range(dimension):
            pts_sum[i] += pt[i]
    return pts_sum

def write_to_output(outputRDD):
    '''
    outputRDD = (pt, anonymized pt)
    '''
    print outputRDD.take(3)
    sqlContext.createDataFrame(outputRDD.map(lambda (pt, an_pt):(pt, an_pt+tuple(pt[dimension]))), ["pt","an_pt"]).write.format('json').save(output_filename, mode='overwrite')
    
def calc_error(cluster_data):
    '''
    cluster_data : (cluster_id, [list of row of pts])
    '''
    #print cluster_data
    pts=cluster_data[1]
    pts_sum= [0 for _ in range(dimension)]
    for pt in pts:
        for i in range(dimension):
            pts_sum[i]=pts_sum[i]+pt[i]
    avg_di = [pts_sum[i]/float(len(pts)) for i in range(dimension)]
    error = 0
    for pt in pts:
        error = error + dist(pt,avg_di)
    return (tuple(avg_di), error)


def anonymize(cluster_data):
    '''
    cluster_data : (cluster_id, [list of row of pts])
    '''
    #print cluster_data
    pts=cluster_data[1]
    pts_sum= [0 for _ in range(dimension)]
    for pt in pts:
        for i in range(dimension):
            pts_sum[i]=pts_sum[i]+pt[i]
    avg_di = [pts_sum[i]/float(len(pts)) for i in range(dimension)]
    result_list=list()
    for pt in pts:
        result_list.append([tuple(pt), tuple(avg_di)])
    return result_list

def flattenPair(pt,pts):
    # print pts
    pairs=[]
    for neighbor in pts:
        pairs += [(pt,neighbor)]
    return pairs

def assign_nearest(pt):
    nearest_cluster = tuple([0 for _ in range(dimension)])
    min_error = float('inf')
    for centroid in centroidsBC.value:
        if dist(pt,centroid)<min_error:
            min_error=dist(pt,centroid)
            nearest_cluster=centroid
    return (pt, nearest_cluster, min_error)

def outputRecord(eps_records):
    f = open(eps_record_filename, 'w')
    f.write('eps,number of cluster,number of noise,error within clusters,error of noise,total error\n')
    for record in eps_records:
        line = ""
        for number in record:
            line =line+ str(number) + ","
        f.write(line+"\n")
    f.close()


# In[ ]:

min_cost_rdd = None
min_cost = float('inf')
min_eps = 0

eps_records=[] # [eps, number of cluster, number of noise point, error within cluster, error of noise, total error]


# In[ ]:

vertics = sqlContext.createDataFrame(rdd.map(lambda pt: (pt, "pt")),['id','name'])
for eps in eps_range:
    start_loop_time = datetime.now()
    print "for eps=", eps
    ptsFullNeighborRDD=rdd.cartesian(rdd)                            .filter(lambda (pt1,pt2): dist(pt1,pt2)<eps)                            .map(lambda (pt1,pt2):(pt1,[pt2]))                            .reduceByKey(lambda pts1,pts2: pts1+pts2)                            .filter(lambda (pt, pts): len(pts)>=minPts)
    edgeRDD=ptsFullNeighborRDD.flatMap(lambda (pt,pts):flattenPair(pt,pts)).cache()
    if (edgeRDD.count()==0):
        print "cannot form cluster for this density"
        time_delta = datetime.now() - start_loop_time    
        eps_records.append([eps, 0, rdd.count(), 0, float('inf'), float('inf'), time_delta])
        outputRecord(eps_records)
        continue
    edges = sqlContext.createDataFrame(edgeRDD,['src','dst'])
    graph = GraphFrame(vertics, edges)
    sc.setCheckpointDir("checkpoint") # required for connectedComponents version > 0.3
    result = graph.connectedComponents()
    # result = graph.stronglyConnectedComponents(maxIter=10)
    resultRDD = result.rdd.map(tuple).map(lambda (row_pt, name, component):(tuple(row_pt),component))
    groupRDD= resultRDD.map(lambda (id_pt,component):(component,[id_pt])).reduceByKey(lambda pt1,pt2:pt1+pt2).cache()
    noiseRDD= groupRDD.filter(lambda (component, pts):len(pts)<k or component is None).flatMap(lambda (component, pts):pts).cache()
    number_of_noise = noiseRDD.count()
    print "noise: ",number_of_noise
    clusterRDD = groupRDD.filter(lambda (component, pts):len(pts)>=k and not component is None).cache()
    number_of_cluster = clusterRDD.count()
    print "number of cluster:", number_of_cluster
    if (number_of_cluster==0):
        cluster_error = 0
    else:
        cluster_error = clusterRDD.map(calc_error).map(lambda (c,e):e).reduce(lambda e1,e2:e1+e2)
    print "error within cluster (without noise)", cluster_error
    centroids = clusterRDD.map(calc_error).map(lambda (c,e):c).collect()
    centroidsBC = sc.broadcast(centroids)
    if (number_of_noise == 0):
        noise_error = 0
    else:
        noise_error =  noiseRDD.map(assign_nearest).map(lambda (pt,nc,e):e).reduce(lambda e1,e2:e1+e2)
    print "error of noises: ", noise_error
    total_error = noise_error + cluster_error
    print "total error: ", total_error
    
    #record time
    time_delta = datetime.now() - start_loop_time    
    eps_records.append([eps, number_of_cluster, number_of_noise, cluster_error, noise_error, total_error, time_delta])
    outputRecord(eps_records)
    if (total_error<min_cost):
        min_eps = eps
        min_cost=total_error
        cluster_anonRDD = clusterRDD.flatMap(anonymize).cache()
        outputRDD=noiseRDD.map(assign_nearest).map(lambda (pt,nc,e):(pt,nc)).union(cluster_anonRDD)
        write_to_output(outputRDD)


# In[ ]:

print "eps\tno. of cluster\tno. of noise point\terror within cluster\terror of noise\ttotal error"
for record in eps_records:
    line = ""
    for number in record:
        line =line+ str(number) + "\t\t"
    print line
print min_eps


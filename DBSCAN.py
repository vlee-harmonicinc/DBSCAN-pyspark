import random, operator, subprocess
from pyspark.sql.types import *
from graphframes import *
import numpy as np
from datetime import datetime, timedelta

from pyspark import SparkContext
sc =SparkContext()

rdd = sc.textFile('data.csv') \
        .map(lambda line: line.split(',')) \
        .map(lambda elements: tuple([int(elements[i]) for i in range(len(elements))])) \
        .cache()

k = 10
dimension = 3
minPts = k
headers = ['age', 'height', 'weight', 'blood_sugar_level', 'child', 'exercise_hours']
# max_cluster = rdd.count() / k
# min_cluster = rdd.count() / (2*k-1)
# loop_for_converge = 20
# different_combination = 30
eps_range = np.arange(4,10, 0.5)
# eps_range = [10]

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

def write_to_output(assignment, centroids):
    tmp = assignment.flatMap(lambda (cluster, pts): [centroids[cluster] for _ in range(len(pts))])
    sqlContext.createDataFrame(tmp, headers[:dimension]).save('output.txt', mode='overwrite')
    
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
    avg_di = [pts_sum[i]/float(len(cluster_data)) for i in range(dimension)]
    error = 0
    for pt in pts:
        error = error + dist(pt,avg_di)
    return (tuple(avg_di), error)

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

min_cost_rdd = None
min_cost = float('inf')
min_eps = 0

eps_records=[] # [eps, number of cluster, number of noise point, error within cluster, error of noise, total error]

for eps in eps_range:
    start_loop_time = datetime.now()
    print "for eps=", eps
    ptsFullNeighborRDD=rdd.cartesian(rdd)\
                            .filter(lambda (pt1,pt2): dist(pt1,pt2)<eps)\
                            .map(lambda (pt1,pt2):(pt1,[pt2]))\
                            .reduceByKey(lambda pts1,pts2: pts1+pts2)\
                            .filter(lambda (pt, pts): len(pts)>=minPts)
    edgeRDD=ptsFullNeighborRDD.flatMap(lambda (pt,pts):flattenPair(pt,pts))
    vertics = sqlContext.createDataFrame(rdd.map(lambda pt: (pt, "pt")),['id','name'])
    if (edgeRDD.count()==0):
        print "cannot form cluster for this density"
        continue
    edges = sqlContext.createDataFrame(edgeRDD,['src','dst'])
    graph = GraphFrame(vertics, edges)
    sc.setCheckpointDir("checkpoint") # required for connectedComponents version > 0.3
    result = graph.connectedComponents()
    resultRDD = result.rdd.map(tuple).map(lambda (row_pt, name, component):(tuple(row_pt),component))
    # TODO left outer join the original vertic point so that preserve 2 point with same location
    # FullResult = rdd.leftOuterJoin(resultRDD)
    groupRDD= resultRDD.map(lambda (id_pt,component):(component,[id_pt])).reduceByKey(lambda pt1,pt2:pt1+pt2)
    noiseRDD= groupRDD.filter(lambda (component, pts):len(pts)<k).flatMap(lambda (component, pts):pts).cache()
    print "noise: ",noiseRDD.count()
    clusterRDD = groupRDD.filter(lambda (component, pts):len(pts)>=k)
    print "number of cluster:", clusterRDD.count()
    if (clusterRDD.count()==0):
        cluster_error = 0
    else:
        cluster_error = clusterRDD.map(calc_error).map(lambda (c,e):e).reduce(lambda e1,e2:e1+e2)
    print "error within cluster (without noise)", cluster_error
    centroids = clusterRDD.map(calc_error).map(lambda (c,e):c).collect()
    centroidsBC = sc.broadcast(centroids)
    if (noiseRDD.count() == 0):
        noise_error = 0
    else:
        noise_error =  noiseRDD.map(assign_nearest).map(lambda (pt,nc,e):e).reduce(lambda e1,e2:e1+e2)
    print "error of noises: ", noise_error
    total_error = noise_error + cluster_error
    print "total error: ", total_error
    
    #record time
    time_delta = datetime.now() - start_loop_time    
    eps_records.append([eps, clusterRDD.count(), noiseRDD.count(), cluster_error, noise_error, total_error, time_delta])
    if (total_error<min_cost):
        min_eps = eps
        min_cost=total_error

print "eps\tno. of cluster\tno. of noise point\terror within cluster\terror of noise\ttotal error"
for record in eps_records:
    line = ""
    for number in record:
        line =line+ str(number) + "\t\t"
    print line
print min_eps

f = open('eps_record.csv', 'w')
for record in eps_records:
    line = ""
    for number in record:
        line =line+ str(number) + ","
    f.write(line+"\n")
f.close()

#coding=utf-8
from pyspark import SparkConf,SparkContext
from operator import *
import datetime
import math
from pyspark import StorageLevel
import time

start = datetime.datetime.now()
conf = SparkConf().setMaster("local[8]").setAppName("my_ubuntu_spark").set("spark.driver.memory","6g").set("spark.executor.memory","2g")
sc=SparkContext(conf=conf)

print sc.appName

ratingDataRaw = sc.textFile("/home/zy/spark_movie/datasets/ml-1m/ratings.dat")
#ratingDataRaw = sc.textFile("/home/zy/spark_movie/datasets/ml-latest-small/ratings.csv")
#ratingDataRaw.persist(StorageLevel.MEMORY_AND_DISK_SER)

#ratingDataSplited = ratingDataRaw.map(lambda line:line.split(','))
ratingDataSplited = ratingDataRaw.map(lambda line:line.split('::'))
#ratingDataSplited.persist(StorageLevel.MEMORY_AND_DISK_SER)

ratingDataUV = ratingDataSplited.map(lambda fields:(fields[0],fields[1]))

ratingDataVU = ratingDataSplited.map(lambda fields:(fields[1],fields[0]))

num_users = ratingDataSplited.map(lambda fields:fields[0]).distinct().count()
print "num_users" , num_users

movie2user = ratingDataVU.combineByKey(lambda v:[v], lambda x,y:x+[y], lambda x,y:x+y)

movie2userValues = movie2user.values()

def convertTuple2uvList(line):
    count = len(line)
    list = []
    for i in range(0,count-1):
        for j in range(i+1,count):
            list.append((line[i],line[j]))
    return list

usersim_mat_uvlist = movie2userValues.map(convertTuple2uvList)

usersim_mat_flat = usersim_mat_uvlist.flatMap(lambda x:x)
#usersim_mat_flat.persist(StorageLevel.MEMORY_AND_DISK_SER)

usersim_mat_tuple = usersim_mat_flat.map(lambda (x):((x[0],x[1]),1))
#usersim_mat_tuple.persist(StorageLevel.MEMORY_AND_DISK_SER)

usersim_mat_reduceByKey = usersim_mat_tuple.reduceByKey(add)
print "usersim_mat_reduceByKey is ok now"

user_item_count = ratingDataUV.map(lambda (x):(x[0],1)).reduceByKey(add)
#user_item_count.persist(StorageLevel.MEMORY_AND_DISK_SER)

usersim_mat_big = usersim_mat_reduceByKey.map(lambda (x):(x[0][0],(x[0][1],x[1]))).join(user_item_count).map(lambda (x):(x[0],(x[1][0][0],x[1][0][1],x[1][1]))).join(user_item_count).map(lambda (x):(x[1][0][0],x[0],x[1][0][1],x[1][0][2],x[1][1]))
usersim_mat_big.persist(StorageLevel.MEMORY_AND_DISK_SER)

usersim_mat_compute_sim = usersim_mat_big.map(lambda (x):(x[0],x[1],x[2]/math.sqrt(x[3]*x[4])))
usersim_mat_compute_sim.persist(StorageLevel.MEMORY_AND_DISK_SER)
usersim_mat_compute_sim.repartition(12)
end_compute = datetime.datetime.now()
print "end_compute=", end_compute
print usersim_mat_compute_sim.collect()
end_print = datetime.datetime.now()
usersim_mat_sorted = usersim_mat_compute_sim.sortBy(lambda x:(x[0],-x[2]))
usersim_mat_sorted.persist(StorageLevel.MEMORY_AND_DISK_SER)

end_sort = datetime.datetime.now()

print "compute cost =",end_compute-start
print "print cost=",end_print-end_compute
print "sort cost=",end_sort - end_print
print "total cost =",end_sort -start
print usersim_mat_sorted.take(50)
end = datetime.datetime.now()
print "end take=",end-end_sort
#compute cost = 0:00:26.430047
#sort cost= 2:17:00.031209 #dayin
#total cost = 2:17:26.461256

def sleeptime(hour,min,sec):
    return hour*3600+min*60+sec

second=sleeptime(0,0,30)
while 1==1:
    time.sleep(second)
    print "wait for 30 second"

sc.stop()


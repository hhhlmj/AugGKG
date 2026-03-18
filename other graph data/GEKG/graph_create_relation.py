#encoding:utf-8
##去除过部分出境条目后的数据的基本情况
##简单加了距离、方位等信息并删除了部分无效数据，同时进行了保存
import pandas as pd
import calendar
from geopy.distance import geodesic
from math import radians,sin,cos,degrees,atan2
import time
import math
import execjs




#此函数用于将时间限制为2019年6月4日0时至24时，2h为一采样阶段，只选取每阶段内的第一条航迹点数据
filename = 'result_grid_timeselect.csv'
data = pd.read_csv(filename,sep=',' )    #skiprows从文件开始处跳过的行数,  nrows取的行数
df_entity_locgrid = pd.DataFrame()
df_entity_time = pd.DataFrame()
df_time_entity = pd.DataFrame()
df_locgrid_time = pd.DataFrame()
df_time_locgrid = pd.DataFrame()
df_heading_entity=pd.DataFrame()
df_locgrid_heading=pd.DataFrame()
data['Time'] = pd.to_datetime(data['Time'])
hour=data.Time.dt.hour//2
for i in range(0,len(data)):
	df_entity_time=df_entity_time.append([{"entity_ID":"entity"+str(i),"name":"entity2time","time_ID":"time"+str(hour.iloc[i])}], ignore_index=True)
	df_time_entity=df_time_entity.append([{"time_ID":"time"+str(hour.iloc[i]),"name":"time2entity","entity_ID":"entity"+str(i)}], ignore_index=True)
	df_locgrid_time=df_locgrid_time.append([{"locgrid_ID":"locgrid"+str(i),"name":"loc2time","time_ID":"time"+str(hour.iloc[i])}], ignore_index=True)
	df_time_locgrid=df_time_locgrid.append([{"time_ID":"time"+str(hour.iloc[i]),"name":"time2loc","locgrid_ID":"locgrid"+str(i)}], ignore_index=True)
	df_entity_locgrid = df_entity_locgrid.append([{"entity_ID":"entity"+str(i),"name":"locatedin","locgrid_ID":"locgrid"+str(i)}], ignore_index=True)
	df_heading_entity = df_heading_entity.append([{"heading_ID":"heading"+str(i),"name":"leads","entity_ID":"entity"+str(i)}], ignore_index=True)
	df_locgrid_heading = df_locgrid_heading.append([{"locgrid_ID":"locgrid"+str(i),"name":"calculates","heading_ID":"heading"+str(i)}], ignore_index=True)

df_entity_time.to_csv('graph_new\\relation\\entity-time.csv',sep=',',header=0,index=False)
df_time_entity.to_csv('graph_new\\relation\\time-entity.csv',sep=',',header=0,index=False)
df_locgrid_time.to_csv('graph_new\\relation\\locgrid-time.csv',sep=',',header=0,index=False)
df_time_locgrid.to_csv('graph_new\\relation\\time-locgrid.csv',sep=',',header=0,index=False)
df_entity_locgrid.to_csv('graph_new\\relation\\entity-locgrid.csv',sep=',',header=0,index=False)
df_heading_entity.to_csv('graph_new\\relation\\heading-entity.csv',sep=',',header=0,index=False)
df_locgrid_heading.to_csv('graph_new\\relation\\locgrid-heading.csv',sep=',',header=0,index=False)
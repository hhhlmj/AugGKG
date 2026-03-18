#encoding:utf-8
##去除过部分出境条目后的数据的基本情况
##简单加了距离、方位等信息并删除了部分无效数据，同时进行了保存
import pandas as pd
import calendar
from math import radians,sin,cos,degrees,atan2
import time
import math




#此函数用于将时间限制为2019年6月4日0时至24时，2h为一采样阶段，只选取每阶段内的第一条航迹点数据
filename = 'result_grid_timeselect.csv'
data = pd.read_csv(filename,sep=',' )    #skiprows从文件开始处跳过的行数,  nrows取的行数
df_entity_location = pd.DataFrame()
df_entity_time = pd.DataFrame()

data['Time'] = pd.to_datetime(data['Time'])
hour=data.Time.dt.hour
Minute=data.Time.dt.minute
for i in range(0,len(data)):
	df_entity_time=df_entity_time.append([{"entity_ID":"entity"+str(i),"name":"entity2time","time_ID":"time"+str(i)}], ignore_index=True)
	df_entity_location = df_entity_location.append([{"entity_ID":"entity"+str(i),"name":"locatedin","location_ID":"location"+str(i)}], ignore_index=True)

df_entity_time.to_csv('Relations\\entity-time.csv',sep=',',header=0,index=False)
df_entity_location.to_csv('Relations\\entity-attribute.csv',sep=',',header=0,index=False)

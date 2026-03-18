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
df_entity = pd.DataFrame()
df_location = pd.DataFrame()
df_heading=pd.DataFrame()
df_time=pd.DataFrame()
data['Time'] = pd.to_datetime(data['Time'])
hour=data.Time.dt.hour
Minute=data.Time.dt.minute
for i in range(0,len(data)):
	df_entity=df_entity.append([{"entity_ID":"entity"+str(i),"name":data.iloc[i,2],"label":"entity"}], ignore_index=True)
	df_time=df_time.append([{"time_ID":"time"+str(i),"name":str(hour.iloc[i])+':'+str(Minute.iloc[i]),"label":"time"}], ignore_index=True)
	df_location=df_location.append([{"location_ID":"location"+str(i),"name":str(data.iloc[i,3])+','+str(data.iloc[i,4]),"label":"location"}], ignore_index=True)

df_entity.to_csv('Nodes\\entity.csv',sep=',',header=0,index=False)
df_time.to_csv('Nodes\\time.csv',sep=',',header=0,index=False)
df_location.to_csv('Nodes\\attribute.csv',sep=',',header=0,index=False)
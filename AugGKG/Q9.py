# coding:utf-8
from math import inf
from py2neo import Graph,Node,NodeMatcher,RelationshipMatcher,walk
import pandas as pd
# from config import *
from rdb_neo4jcsv import *
import execjs
import time


ctx = execjs.compile('''    
function get_grid_shift(code1,code2){
	console.assert(code1.length % 2 === 0 && code1.length > 2);
	console.assert(code1.length === code2.length);
    let level = code1.length / 2;
	let code_origin_lat1 = '';
    let code_origin_lon1 = '';
	let code_origin_lat2 = '';
    let code_origin_lon2 = '';
    for (var i = 0; i < code1.length; i++) {
		if(i%2==0){
		code_origin_lat1 = code_origin_lat1.concat(code1[i]);
		}
		if(i%2==1){
		code_origin_lon1 = code_origin_lon1.concat(code1[i]);
		}
	}
    for (var i = 0; i < code2.length; i++) {
		if(i%2==0){
		code_origin_lat2 = code_origin_lat2.concat(code2[i]);
		}
		if(i%2==1){
		code_origin_lon2 = code_origin_lon2.concat(code2[i]);
		}
	}
	let code_origin_lat_Int1 = parseInt(code_origin_lat1, 2);
	let code_origin_lon_Int1 = parseInt(code_origin_lon1, 2);
	let code_origin_lat_Int2 = parseInt(code_origin_lat2, 2);
	let code_origin_lon_Int2 = parseInt(code_origin_lon2, 2);
	return([code_origin_lat_Int2-code_origin_lat_Int1,code_origin_lon_Int2-code_origin_lon_Int1]);
}
	''')   #javascript



# GeoSOT level 16    1km
def distance_cal(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	return entity_distance



def mintime_entity(graph,nodes,matcher,select_entityname1,select_entityname2,visual=False):
    entity_list1=[]
    select_time1=[]
    entity_list2=[]
    select_time2=[]
    min_distance=float("inf")
    entity_distance=pd.DataFrame()
    result=nodes.match('entity',name=select_entityname1)   #Query the presence or absence of entity in each time slice
    for x in result:
        i=0
        for y in walk(x):
            entity_list1.append(y["ID"])
            i+=1    
    for k in range(0,len(entity_list1)):
        result = matcher.match(list(nodes.match('entity',name=select_entityname1,ID=entity_list1[k])),r_type='entity2time')   #entity query all time slice
        for x in result:
            i=0
            for y in walk(x):
                if type(y) is Node and i % 3 ==2:
                    select_time1.append(y["ID"])
                i+=1   
    result=nodes.match('entity',name=select_entityname2)
    for x in result:
        i=0
        for y in walk(x):
            entity_list2.append(y["ID"])
            i+=1    
    for k in range(0,len(entity_list2)):
        result = matcher.match(list(nodes.match('entity',name=select_entityname2,ID=entity_list2[k])),r_type='entity2time')   #time slice query locgrid
        for x in result:
            i=0
            for y in walk(x):
                if type(y) is Node and i % 3 ==2:
                    select_time2.append(y["ID"])
                i+=1   
    
    for k in range(0,12):
        if (("time"+str(k)) in select_time1) and (("time"+str(k)) in select_time2):
            entity_1=select_time1.index("time"+str(k))
            entity_2=select_time2.index("time"+str(k))
            result = matcher.match(list(nodes.match('entity',ID=entity_list1[entity_1])),r_type='locatedin')  #entity query locgrid
            for x in result:
                i=0
                for y in walk(x):
                    if type(y) is Node and i % 3 ==2:
                        select_locgrid1=y["name"]
                    i+=1   
            result = matcher.match(list(nodes.match('entity',ID=entity_list2[entity_2])),r_type='locatedin')  #entity query locgrid
            for x in result:
                i=0
                for y in walk(x):
                    if type(y) is Node and i % 3 ==2:
                        select_locgrid2=y["name"]
                    i+=1   
            distance=float(distance_cal(select_locgrid1,select_locgrid2))
            if distance<=min_distance:
                min_distance=distance
                select_entityID1=entity_list1[entity_1]
                select_entityID2=entity_list2[entity_2]
                min_time="time"+str(k)
        else:
                distance=float("inf")
        entity_distance=entity_distance.append([{"Time_ID":"time"+str(k),"entity_name1":select_entityname1,"entity_ID1":entity_list1[entity_1],"distance":distance,"relationship":"spatial location","entity_name2":select_entityname2,"entity_ID2":entity_list2[entity_2]}], ignore_index=True)
        min_entity_distance={"select_entityname1":select_entityname1,"select_entityID1":select_entityID1,"select_entityname2":select_entityname2,"select_entityID2":select_entityID2,"min_distance":min_distance,"entity_time":min_time}
        entity_distance.to_csv('query_results\\Q9.csv',columns=['entity_name1','entity_name2',"distance",'Time_ID'],sep=',',header=1,index=False)  #Do not save column names
    if visual==True:
        entity_distance.to_csv('new_relations\\entity-entity.csv',columns=['entity_ID1','distance','entity_ID2',"relationship"],sep=',',header=0,index=False)  #Do not save column names
        relation_files = get_FileList("new_relations")
        for rf in relation_files:
            relation_import_new(rf, graph, nodes,relationship='distance')
    print(entity_distance)
    print(min_entity_distance)
    return(entity_distance,min_entity_distance)



if __name__ == "__main__":
    # Connect to neo4j database, enter address, username, password
    graph = Graph("http://localhost:7474",auth =("***", "***"))  
    nodes = NodeMatcher(graph)
    matcher = RelationshipMatcher(graph) 
    visual=False

    select_entityname1='477194400'
    select_entityname2='477077600'
    time_start=time.time()
    mintime_entity(graph,nodes,matcher,select_entityname1,select_entityname2,visual)   #Select two entities and query them for the time when they are the shortest distance apart.
    time_end=time.time()
    print('time all cost',time_end-time_start,'s')
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



#GeoSOT 16TH
def distance_cal(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	return entity_distance

def distance_ne(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	if grid_shift[0]>=0 and grid_shift[1]>=0:    #The difference in latitude is greater than 0, and the difference in longitude is greater than 0
		entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	else:
		entity_distance=float("inf")
	return entity_distance

def distance_nw(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	if grid_shift[0]>=0 and grid_shift[1]<=0:   
		entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	else:
		entity_distance=1e10
	return entity_distance

def distance_sw(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	if grid_shift[0]<=0 and grid_shift[1]<=0:   
		entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	else:
		entity_distance=1e10
	return entity_distance

def distance_se(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	if grid_shift[0]<=0 and grid_shift[1]>=0:   
		entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	else:
		entity_distance=1e10
	return entity_distance




def min_entity(graph,nodes,matcher,select_entity,select_time,direction='all',visual=False):
    locgrid_name=[]
    entity_name=[]
    entity_ID=[]
    entity_distance=[]
    min_distance=float("inf")
    select_locgrid=[]
    df_distance=pd.DataFrame()
    result = matcher.match(list(nodes.match('time',ID=select_time)),r_type='time2loc')   #time slice query locgrid
    for x in result:
        i=0
        for y in walk(x):
            if type(y) is Node and i!=0 and i % 3 ==2:
                locgrid_name.append(y["name"])
            i+=1       
    result = matcher.match(list(nodes.match('time',ID=select_time)),r_type='time2entity')   #time slice query entity
    for x in result:
        i=0
        for y in walk(x):
            if type(y) is Node and i % 3 ==2:
                entity_name.append(y["name"])
                entity_ID.append(y["ID"])
            i+=1   
    result = matcher.match(list(nodes.match('entity',ID=select_entity)),r_type='locatedin').limit(1)   # the entity query Locgrid of select_entity
    for x in result:
        i=0
        for y in walk(x):
            if type(y) is Node and i % 3 ==2:
                select_locgrid.append(y["name"])
            i+=1   
    for i in range(0,len(locgrid_name)):   #Calculation of spatio-temporal distance  and direction in  geo-hidden layer
        if direction == 'all':
            distance=float(distance_cal(select_locgrid[0],locgrid_name[i]))
        elif direction == 'northeast':
            distance=float(distance_ne(select_locgrid[0],locgrid_name[i]))
        elif direction == 'northwest':
            distance=float(distance_nw(select_locgrid[0],locgrid_name[i]))
        elif direction == 'sorthwest':
            distance=float(distance_sw(select_locgrid[0],locgrid_name[i]))
        elif direction == 'sortheast':
            distance=float(distance_se(select_locgrid[0],locgrid_name[i]))
        entity_distance.append(distance)
        if distance<=min_distance and distance!=0:
            min_distance=distance
            min_entityname=entity_name[i]
            min_entityID=entity_ID[i]
            min_locgrid=locgrid_name[i]
        df_distance=df_distance.append([{"select_entity":select_entity,"distance":entity_distance[i],"relationship":"spatial location","entity_name":entity_name[i],"entity_ID":entity_ID[i]}], ignore_index=True)
    min_conncetion={"min_entityname":min_entityname,"min_entityID":min_entityID,"min_locgrid":min_locgrid,"min_distance":min_distance}
    df_distance.to_csv('query_results\\Q7.csv',columns=['entity_name',"entity_ID",'distance'],sep=',',header=1,index=False)  #don't save column names
    if visual==True:
        df_distance.to_csv('new_relations\\entity-entity.csv',columns=['select_entity','distance','entity_ID',"relationship"],sep=',',header=0,index=False)  #don't save column names
        relation_files = get_FileList("new_relations")
        for rf in relation_files:
            relation_import_new(rf, graph, nodes,relationship='distance')
    print(df_distance)
    print(min_conncetion)
    return(df_distance,min_conncetion)



if __name__ == "__main__":
    # Connect to neo4j database, enter address, username, password
    graph = Graph("http://localhost:7474",auth =("***", "***"))  
    nodes = NodeMatcher(graph)
    matcher = RelationshipMatcher(graph) 
    select_entity="entity2394"
    select_time="time8"
    direction='northeast'
    visual=False
    time_start=time.time()
    min_entity(graph,nodes,matcher,select_entity,select_time,direction,visual)  #Select the entity and time slice to query the nearest entity with the specified entity at the current time; direction represents the search direction
    time_end=time.time()
    print('time all cost',time_end-time_start,'s')


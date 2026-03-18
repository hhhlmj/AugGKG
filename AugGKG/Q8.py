# coding:utf-8
from math import inf
import pandas as pd
from py2neo import Graph,Node,NodeMatcher,RelationshipMatcher,walk
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



#GeoSOT 16level
def distance_cal(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	return entity_distance



def event_warning(graph,nodes,matcher,event_type,event_threshold,event_entity_relation,visual=False):
    event_time_list=[]
    event_node=nodes.match('event').where(name=event_type).first()
    result = matcher.match(list(nodes.match('event',name=event_type)),r_type='locatedin') 
    event_locgrid = list(matcher.match([event_node], r_type='locatedin'))
    event_locgrid=event_locgrid[0].end_node['name']
    event_time = list(matcher.match([event_node], r_type='eve2time'))
    for i in range(0,len(event_time)):
        event_time_list.append(event_time[i].end_node['ID'])
    

    df_event_true=pd.DataFrame()
    df_event_false=pd.DataFrame()

    for k in range(0,len(event_time_list)):      
        locgrid_name=[]
        entity_name=[]
        entity_ID=[]
        event_entity_distance=[]
        result = matcher.match(list(nodes.match('time',ID=event_time_list[k])),r_type='time2loc')   #time slice query entity
        for x in result:
            i=0
            for y in walk(x):
                if type(y) is Node and i!=0 and i % 3 ==2:
                    locgrid_name.append(y["name"])
                i+=1       
        result = matcher.match(list(nodes.match('time',ID=event_time_list[k])),r_type='time2entity')   #time slice query entity  
        for x in result:
            i=0
            for y in walk(x):
                if type(y) is Node and i % 3 ==2:
                    entity_name.append(y["name"])
                    entity_ID.append(y["ID"])
                i+=1     
        for i in range(0,len(locgrid_name)):   #Calculation of spatio-temporal distance in  geo-hidden layer
            distance=float(distance_cal(event_locgrid,locgrid_name[i]))
            event_entity_distance.append(distance)
            if distance<=event_threshold:
                df_event_true=df_event_true.append([{"event_node":event_node['ID'],"select_event":event_type,"event_locgrid":event_locgrid,"event_time":event_time_list[k],"distance":event_entity_distance[i],"influence":event_entity_relation,"entity_name":entity_name[i],"entity_ID":entity_ID[i]}], ignore_index=True)
            else:
                df_event_false=df_event_false.append([{"event_node":event_node['ID'],"select_event":event_type,"event_locgrid":event_locgrid,"event_time":event_time_list[k],"distance":event_entity_distance[i],"influence":'None',"entity_name":entity_name[i],"entity_ID":entity_ID[i]}], ignore_index=True)
    print(df_event_true)    
    df_event_true.to_csv('query_results\\Q8_1.csv',columns=['select_event','event_locgrid','event_time',"distance",'influence','entity_name'],sep=',',header=1,index=False)  #save column names
    print(df_event_false)  
    df_event_false.to_csv('query_results\\Q8_2.csv',columns=['select_event','event_locgrid','event_time',"distance",'influence','entity_name'],sep=',',header=1,index=False)  #save column names
    if visual==True:
        df_event_true.to_csv('wait_evevt_relation\\event-entity.csv',columns=['event_node','distance','entity_ID',"influence"],sep=',',header=0,index=False)  #Do not save column names
        relation_files = get_FileList("wait_evevt_relation")
        for rf in relation_files:
            relation_import_eventnew(rf, graph, nodes,relationship='distance')



if __name__ == "__main__":
    # Connect to neo4j database, enter address, username, password
    graph = Graph("http://localhost:7474",auth =("***", "***"))  
    nodes = NodeMatcher(graph)
    matcher = RelationshipMatcher(graph) 
    visual=False

    event_type='storm'
    event_threshold=500
    event_entity_relation='escape'
    time_start=time.time()
    event_warning(graph,nodes,matcher,event_type,event_threshold,event_entity_relation,visual)
    time_end=time.time()
    print('time all cost',time_end-time_start,'s')


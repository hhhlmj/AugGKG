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



#16层级
def distance_cal(code1,code2):
	grid_shift=ctx.call("get_grid_shift",code1,code2)
	entity_distance=(grid_shift[0]**2+grid_shift[1]**2)**0.5
	return entity_distance

def entity_detectevent(graph,nodes,matcher,select_entityname,visual=False):

    entity_list=[]
    select_time=[]
    df_event_true=pd.DataFrame()
    df_event_false=pd.DataFrame()

    result=nodes.match('entity',name=select_entityname)   #Query the presence or absence of entity in each time slice
    for x in result:
        i=0
        for y in walk(x):
            entity_list.append(y["ID"])
            i+=1    
    for k in range(0,len(entity_list)):
        result = matcher.match(list(nodes.match('entity',name=select_entityname,ID=entity_list[k])),r_type='entity2time')   #entity查询全部time
        for x in result:
            i=0
            for y in walk(x):
                if type(y) is Node and i % 3 ==2:
                    select_time.append(y["ID"])
                i+=1
    print(entity_list) 
    for k in range(0,12):
        event_name=[]
        event_ID=[]
        event_locgrid=[]
        if (("time"+str(k)) in select_time):
            entity_1=select_time.index("time"+str(k))
            result = matcher.match(list(nodes.match('entity',ID=entity_list[entity_1])),r_type='locatedin')  #  entity query locgrid
            for x in result:
                i=0
                for y in walk(x):
                    if type(y) is Node and i % 3 ==2:
                        select_locgrid1=y["name"]
                    i+=1        
            result = matcher.match(list(nodes.match('time',ID="time"+str(k))),r_type='time2eve')   # time slice query event
            for x in result:
                i=0
                for y in walk(x):
                    if type(y) is Node and i % 3 ==2:
                        event_name.append(y["name"])
                        event_ID.append(y["ID"])
                    i+=1   
            for j in range(0,len(event_ID)):
                result = matcher.match(list(nodes.match('event',ID=event_ID[j])),r_type='locatedin').limit(1)   #event query event_locgrid
                for x in result:
                    i=0
                    for y in walk(x):
                        if type(y) is Node and i % 3 ==2:
                            event_locgrid.append(y["name"])
                        i+=1   
            for i in range(0,len(event_ID)):   #时空计算距离
                if event_name[i]=='Tsunami' or event_name[i]=='earthquake' or event_name[i]=='storm':
                    event_entity_relation='escape'
                    event_threshold=5000
                else:
                    event_entity_relation='motivate'
                    event_threshold=3000
                distance=float(distance_cal(select_locgrid1,event_locgrid[i]))
                if distance<=event_threshold:
                    df_event_true=df_event_true.append([{"event_node":event_ID[i],"event_name":event_name[i],"event_time":"time"+str(k),"distance":distance,"influence":event_entity_relation,"entity_name":select_entityname,"entity_ID":entity_list[entity_1],"entity_locgrid":select_locgrid1}], ignore_index=True)
                else:
                    df_event_false=df_event_false.append([{"event_node":event_ID[i],"event_name":event_name[i],"event_time":"time"+str(k),"distance":distance,"influence":'None',"entity_name":select_entityname,"entity_ID":entity_list[entity_1],"entity_locgrid":select_locgrid1}], ignore_index=True)
    print(df_event_true)
    df_event_true.to_csv('query_results\\Q11_1.csv',columns=['entity_name','entity_locgrid','event_time',"distance",'influence','event_name'],sep=',',header=1,index=False)  #save column names
    print(df_event_false)
    df_event_false.to_csv('query_results\\Q11_2.csv',columns=['entity_name','entity_locgrid','event_time',"distance",'influence','event_name'],sep=',',header=1,index=False)  #save column names
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
    select_entityname='477771500'
    time_start=time.time()
    entity_detectevent(graph,nodes,matcher,select_entityname,visual)
    time_end=time.time()
    print('time all cost',time_end-time_start,'s')
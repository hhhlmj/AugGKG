# coding:utf-8
from math import inf
import pandas as pd
from py2neo import Graph,Node,NodeMatcher,RelationshipMatcher,walk
# from config import *
from rdb_neo4jcsv import *
import execjs
import time



if __name__ == "__main__":
    # Connect to neo4j database, enter address, username, password
    graph = Graph("http://localhost:7474",auth =("***", "***"))  
    nodes = NodeMatcher(graph)
    matcher = RelationshipMatcher(graph) 
    visual=False
    select_entityname='477218900'
    select_entityid='entity1247'
    select_node = nodes.match("entity").where(ID=select_entityid).first()
    relationship = list(matcher.match([select_node], r_type='locatedin'))
    
    select_locgrid=relationship[0].end_node['name']


    select_time=nodes.match("time").where(ID='time5').first()
    relationship = list(matcher.match([select_time], r_type='time2loc'))
    for i in range(0,len(relationship)):
        if relationship[i].end_node['name']==select_locgrid:
            select_locgrid=relationship[i].end_node
    time3_relationship = list(matcher.match([select_locgrid], r_type='loc2entity'))
    grid_entity=time3_relationship[0].end_node
    print(grid_entity)
    heading_relationship=list(matcher.match([grid_entity], r_type='leads'))
    # print(grid_entity,heading_relationship[0].endnode['name'])
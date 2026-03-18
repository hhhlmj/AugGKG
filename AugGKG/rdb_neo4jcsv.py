# coding:utf-8
from py2neo import Graph,Node,Relationship
# from config import *
import csv
import os
from py2neo import Graph,Node,NodeMatcher,RelationshipMatcher,walk


# Import different types of nodes from files
def node_import(file, test_graph):
    node_list = {}
    with open(file, "r", encoding='utf-8-sig') as csvfile:
        file_cont = csv.reader(csvfile) # Reads a csv file and returns the iteration type
        for item in file_cont:
            node = Node( item[2], ID=item[0], name = item[1])   # name is the node name, displayed in neo4j; item[1] is the node name e.g.: entity,time, event.
            test_graph.create(node)                 
            node_list[item[0]] = node
    return node_list



def relation_import(file, test_graph, node_lists):
    with open(file, "r", encoding='utf-8-sig') as csvfile:
        file_cont = csv.reader(csvfile) 
        for item in file_cont:
            node1 = node_lists[item[0]]
            node2 = node_lists[item[2]]
            relation = Relationship(node1, item[1], node2)
            test_graph.create(relation)



def relation_import_att(file, test_graph, node_lists,att):
    with open(file, "r", encoding='utf-8-sig') as csvfile:
        file_cont = csv.reader(csvfile) 
        print(file)
        for item in file_cont:

            node1 = node_lists.match('entity',ID=item[0]).first()
            node2 = node_lists.match('attribute',name=item[2]).first() 
            relation = Relationship(node1, item[1], node2)
            test_graph.create(relation)





def relation_import_new(file, test_graph, node_lists,relationship):
    with open(file, "r", encoding='utf-8-sig') as csvfile:
        file_cont = csv.reader(csvfile) 
        for item in file_cont:
            node1 = node_lists.match('entity',ID=item[0]).first()
            node2 = node_lists.match('entity',ID=item[2]).first()      
            properties={relationship:item[1]}   
            relation = Relationship(node1, item[3],node2,**properties)  
            test_graph.create(relation)



def relation_import_eventnew(file, test_graph, node_lists,relationship):
    with open(file, "r", encoding='utf-8-sig') as csvfile:
        file_cont = csv.reader(csvfile) 
        for item in file_cont:
            # 创建关系
            node1 = node_lists.match('event',ID=item[0]).first()
            node2 = node_lists.match('entity',ID=item[2]).first()      
            properties={relationship:item[1]}    
            relation = Relationship(node1, item[3],node2,**properties)   
            test_graph.create(relation)


def relation_import_evententitynew(file, test_graph, node_lists,relationship):
    with open(file, "r", encoding='utf-8-sig') as csvfile:
        file_cont = csv.reader(csvfile) 
        for item in file_cont:
            # creat relation
            node1 = node_lists.match('entity',ID=item[0]).first()
            node2 = node_lists.match('event',ID=item[2]).first()      
            properties={relationship:item[1]}  
            relation = Relationship(node1, item[3],node2,**properties)  
            test_graph.create(relation)





# Read all files in a folder
def get_FileList(path):
    file_list = []
    for filename in os.listdir(path):  # The argument to listdir is the path to the folder
        file_list.append(path + '/' + filename)
    return file_list

def attribution_creat(graph,nodes,attribution_attach=False):   #adding the attribution of Nodes
    if attribution_attach==True:
        test_graph = Graph("http://localhost:7474",auth =("***", "***"))
        nodes = NodeMatcher (test_graph) 
        relation_import_att('attribution_relation\\entity-size.csv', graph, nodes,'Size')
        relation_import_att('attribution_relation\\entity-generation.csv', graph, nodes,'Generation')
        relation_import_att('attribution_relation\\entity-birthplace.csv', graph, nodes,'Birthplace')
        relation_import_att('attribution_relation\\entity-deployment.csv', graph, nodes,'Deployment')

if __name__ == "__main__":
    # Connect to neo4j database, enter address, username, password
    test_graph = Graph("http://localhost:7474",auth =("***", "***"))

    # Read file list
    node_files = get_FileList("nodes")
    relation_files = get_FileList("relations")
    # Writing nodes and relationships to the graph database
    node_lists = {}
    for nf in node_files:
        node_list = node_import(nf, test_graph)
        node_lists.update(node_list)
    for rf in relation_files:
        relation_import(rf, test_graph, node_lists)
    #Write attribute relations to the graph database (optional)
    #Attention:If all semantic attributes are stored, it will greatly increase the query and computation time
    attribution_attach=False
    attribution_creat(test_graph,attribution_attach)
    
    print("————————————Finish————————————")





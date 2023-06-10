## LIBRARIES ##
import os
import pandas as pd
import networkx as net
from pythonds.graphs import Graph, Vertex
from pythonds.basic import Queue
import math
import random
import time

## READING FILE ##
os.getcwd()
os.chdir(r"C:\Users\David\Desktop\Unifi\PRIMO ANNO\SECONDO SEMESTRE\Algoritmi e programmazione per l'analisi dei dati\Progetto")
data=open("dati_progetto.tsv", "r")
db = pd.read_csv("dati_progetto.tsv", sep="\t", header=None)

## FORMAT WORKING ##
#yearDetect function
def yearDetect(x):
    closed=x.rfind(")") #rfind restituisce l'ultima posizione dell'argomento
    open=x.rfind("(")
    if (closed-open)<4: #se invece di un anno e.g. 1980,1990,... abbiamo come ultimi valori (TV) o (V)
        closed2=x[:open].rfind(")")
        open2=x[:open].rfind("(")
        if "/" in x[open2:closed2+1]:
            slash=x[:closed2+1].rfind("/")
            if "/" in x[open2+1:slash]:
                slash2=x[:slash].rfind("/")
                year=x[open2+1:slash2]
            else:
                year=x[open2+1:slash]
        else:
            year=x[open2+1:closed2]
    else:
        if "/" in x[open:closed+1]:
            slash=x[:closed+1].rfind("/")
            if "/" in x[open+1:slash]:
                slash2=x[:slash].rfind("/")
                year=x[open+1:slash2]
            else:
                year=x[open+1:slash]
        else:
            year=x[open+1:closed]
    if year.isnumeric():
        return year



#extract names, titles and years
Names = db[0]
Titles = db[1]
Years = []
for title in list(Titles):
    year = yearDetect(title)
    Years.append(year)

## GRAPH MAKING OF ##
G=net.Graph()
#dictionaries
#from Id to Actors name
IdActDict=dict(enumerate(db[0].unique())) #raccoglie gli attori evitando multipli e assegna a ciascuno un id
#from Actors name to Id
ActIdDict=dict(map(reversed,enumerate(db[0].unique()))) #map permette di applicare una funzione a ogni elemento del secondo argomento, in questo caso creiamo il dizionario inverso di quello creato precedentemente
#from Id to Films name
IdFilmDict=dict(enumerate(db[1].unique(),start=len(ActIdDict)+1)) #le operazioni sono le stesse, ma specificando "start" impediamo che vengano assegnati gli stessi id degli attori
#from Films name to Id
FilmIdDict=dict(map(reversed,enumerate(db[1].unique(),start=len(ActIdDict)+1))) #troviamo il dizionario inverso

#from IdName to all the IdFilms where the actor has partecipated
db["IdName"]=db[0].map(ActIdDict) #creo una lista con tutti gli id degli attori
db["IdFilm"]=db[1].map(FilmIdDict) #creo una lista con tutti gli id dei film
dictAllId=dict(db.groupby("IdName")["IdFilm"].agg(set)) #creo un dizionario in cui per ogni id degli attori inserisco tutti gli id in cui quell'attore ha partecipato e.g. 966: {2366937, 2366938, 2366939, 2366940, 2366941}
#every IdFilm for each year
db["Year"]=Years #creo una lista con tutti i film
dictAllFilmYear=dict(db.groupby("Year")["IdFilm"].agg(set)) #creo un dizionario in cui per ogni anno inserisco tutti gli id dei film usciti in quell'anno e.g. '2020': {2773711, 2785787}

#graph building
for aname in dictAllId.keys(): #per ogni id degli attori creo un nodo
    G.add_node(aname)
for atitle in FilmIdDict.keys(): #per ogni id dei film creo un nodo
    G.add_node(atitle)
for aname, titles in dictAllId.items(): #collego ogni Id degli attori a ogni id dei film in cui ha partecipato
    for atitle in titles:
        G.add_edge(aname, atitle)


#### QUESTION 1.A ####
def maxcast(year):
    max = 0
    for y,films in dictAllFilmYear.items():
        for afilm in films:
            if y <= year:
                numAct=G.degree(afilm) #G.degree(afilm) è il numero di archi che "entrano" in "afilm", quindi il numero di attori che vi ci partecipano
                if numAct>max:
                    max=numAct
                    titleMax=IdFilmDict.get(afilm) #attraverso get possiamo usare l'id, cioè "afilm", per risalire al titolo originale
    return titleMax,max

#### QUESTION 2.2 ####
#attraverso questa funzione prendo solamente i nodi che riguardano film pubblicati fino all'anno "year" che ci interessa
def nodesUpYear(year):
    nodesToUse={}
    for y in dictAllFilmYear.keys(): #per ogni anno
        if int(y)<=year:
            for amovie in dictAllFilmYear.get(str(y)):
                nodesToUse[amovie]=0 #creo un dizionario con gli id dei film come chiavi e come valori tutti 0
                for anActor in G.neighbors(int(amovie)): #G.neighbors(int(amovie)) crea un iteratore su tutti i vicini di "amovie"
                    if anActor not in nodesToUse:
                        nodesToUse[anActor]=0 #al dizionario creato precedentemente aggiungo gli id degli attori che hanno partecipato in "amovie" come chiavi e come valori tutti 0

    return list(nodesToUse.keys()) #prendo solamente le chiavi di "nodesToUse" in modo da avere gli id dei film e degli attori che recitano in film pubblicati fino a "year"

#estraggo la componente connessa più grande
def largestConnComp(graph):
    max=-1
    nodesToVisit=set(graph.nodes) #uso un set in modo che non possa avere duplicati
    while len(nodesToVisit)>0 and len(nodesToVisit)>max: #fino a che ci sono nodi da visitare e i nodi rimasti sono di più di quelli che compongono la componente connessa più numerosa
        start = nodesToVisit.pop() #rimuovo un elemento casuale dal set e lo assegno a "start"
        visitedNodes = {start} #creo un altro set in cui aggiungo il nodo "start" da cui iniziamo a far girare la bfs
        vertQueue = Queue() #creo una coda
        vertQueue.enqueue(start) #aggiungo alla coda il nodo "start"
        while vertQueue.size()>0: #fino a che la coda non è vuota
            currentVert=vertQueue.dequeue() #rimuovo l'ultimo nodo della coda e lo assegno a "currentVert"
            for anode in graph.neighbors(currentVert): #per ogni nodo direttamente collegato a "currentVert"
                if anode not in visitedNodes: #se non abbiamo visitato "anode"
                    vertQueue.enqueue(anode) #aggiungiamo "anode" alla coda in modo da poterlo visitare
                    visitedNodes.add(anode) #aggiungiamo "anode" ai nodi visitati perchè poi lo visiteremo
                    nodesToVisit.remove(anode) #rimuoviamo "anode" dai nodi da visitare
        if len(visitedNodes)>max: #troviamo i nodi più numerosi
            nodesToUse=visitedNodes.copy() #copiamo il set "visitedNodes" e lo assegniamo a "nodesToUse" senza modificare il set originale
            max=len(visitedNodes)
    return nodesToUse

#testo con il modulo di NetworkX, i risultati sono gli stessi
net.node_connected_component(subgraph,262160) == largestConnComp(subgraph)

#funzione per la bfs da utilizzare nella funzione averageDistance
def  bfs(graph, start):
    vertQueue = Queue()
    vertQueue.enqueue(start)
    dist={start:0}
    while vertQueue.size() > 0:
        currentVert = vertQueue.dequeue()
        currentDistance=dist.get(currentVert)
        nbr = graph.neighbors(currentVert)
        for anode in nbr:
            if anode not in dist:
                vertQueue.enqueue(anode)
                dist[anode]=currentDistance+1
    return dist

#funzione per calcolare la distanza media
def averageDistance(graph, epsilon):
    sumNh={} #creo un dizionario che conterà quante volte abbiamo una specifica distanza e.g. {0:3,1:2,2:4,3:8,...}
    kEw=int(math.log(len(graph),10)/(epsilon**2)) #imposto kEw per il successivo campionamento
    randomSample=random.choices(list(graph),k=kEw) #estraggo casualmente kEw nodi dal grafo
    NhaNode={} #creo un dizionario che conterà la distanza per ogni nodo la sua distanza con gli altri nodi e.g. {a:{a:0,b:1},b:{a:1,b:0}}
    for aNode in randomSample: #per ogni nodo del sottografo estratto
        NhaNode[aNode]=bfs(graph,aNode) #è un dizionario con chiavi i nodi x e valori un altro dizionario che ha come chiave ogni vicino y di x e come valore la distanza tra x e y e.g. {a:{a:0,b:1},b:{a:1,b:0}}

        # conto quante volte si ripete una distanza
        # valore atteso delle distanze
        # sommatoria valore della distanza per probabilità di quella distanza
        # sum(valore distanza * #valore distanza)/#nodi
        for nodeName in NhaNode[aNode].keys(): #per ogni nodo del sottografo estratto
            if NhaNode[aNode][nodeName] not in sumNh: #controllo se quella distanza è già presente in sumNh
                sumNh[NhaNode[aNode][nodeName]]=1 #se non c'è in sumNh metto il suo conteggio pari a 1
            else:
                sumNh[NhaNode[aNode][nodeName]]=sumNh[NhaNode[aNode][nodeName]]+1 #se quella distanza è già presente allora aumento di 1 il suo conteggio
    avgDist=0
    for distValue in sumNh.keys(): #per ogni distanza che ho trovato
        avgDist=avgDist+(distValue*(sumNh[distValue])/(kEw*(len(graph)-1))) #calcolo la distanza media facendo una media ponderata delle distanze usando come peso il numero di volte che una determinata frequenza si presenta
    return avgDist



#### QUESTION 3.1 ####
def largestNumberColl(graph):
    coll={} #creo un dizionario che per ciascun id di attore scrive il numero di collaborazione che questo attore ha avuto e.g. {0:12,1:45,2:68,...}
    max = 0
    for aMovie in IdFilmDict.keys(): #per ogni film
        for anActor in graph.neighbors(aMovie): #per ogni attore presente in questo
            if anActor not in coll: #se l'attore non è già presente nel dizionario che raccoglie le collaborazioni
                coll[anActor]=len(list(graph.neighbors(aMovie)))-1 #imposto il numero di collaborazioni come numero totale di collaborazioni in quel film -1, -1 perchè altrimenti conteremmo anche la collaborazione con l'attore stesso
            else:
                coll[anActor]=coll[anActor]+len(list(graph.neighbors(aMovie)))-1 #se è già presente allora sommo al precedente numero di collaborazioni quello relativo al nuovo film
    for anActor in coll.keys(): #per ogni attore nel dizionario delle collaborazioni
        if coll[anActor] > max: #controllo il numero di collaborazioni di ciascun attore
            max=coll[anActor]
            idAct=anActor

    return IdActDict[idAct],max




#######################################
############### OUTPUT ################
#######################################


#######################################
############ QUESTION 1.1 #############
#######################################
for year in ["1930","1940","1950","1960","1970","1980","1990","2000","2010","2020"]:
    print("#", year, maxcast(year))
# 1930 ('The King of Kings (1927)', 171)
# 1940 ('The Buccaneer (1938)', 219)
# 1950 ('Gone to Earth (1950)', 290)
# 1960 ('Around the World in Eighty Days (1956)', 1298)
# 1970 ('Around the World in Eighty Days (1956)', 1298)
# 1980 ('Around the World in Eighty Days (1956)', 1298)
# 1990 ('Around the World in Eighty Days (1956)', 1298)
# 2000 ('Around the World in Eighty Days (1956)', 1298)
# 2010 ('Around the World in Eighty Days (1956)', 1298)
# 2020 ('Around the World in Eighty Days (1956)', 1298)


#######################################
############ QUESTION 2.2 #############
#######################################

for year in [1930,1940,1950,1960,1970,1980,1990,2000,2010,2020]:
    nodes = nodesUpYear(year)
    subGraph = net.induced_subgraph(G, nodes) #uso induced_subgraph perchè altrimenti avrei solamente una lista di nodi, invece così estraggo dal grafo principale il sottografo composto dai nodi presenti in "nodes"
    nodesLcc = largestConnComp(subGraph)
    subGraphLcc = net.induced_subgraph(subGraph, nodesLcc)
    print("#", year, averageDistance(subGraphLcc, 0.8))

# 1930 8.395482574901509
# 1940 7.206630973133006
# 1950 9.357155355260518
# 1960 6.953299316651433
# 1970 7.045774421700598
# 1980 7.688757397489886
# 1990 7.22043329836851
# 2000 7.5938044129893445
# 2010 7.891873432566026
# 2020 8.147434220959608


#######################################
############ QUESTION 3.1 #############
#######################################

largestNumberColl(G)
# ('Flowers, Bess', 40431)




















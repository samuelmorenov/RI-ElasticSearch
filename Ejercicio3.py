# Para poder usar la función print e imprimir sin saltos de línea
from __future__ import print_function

import json # Para poder trabajar con objetos JSON
import pprint # Para poder hacer uso de PrettyPrinter
import sys # Para poder usar exit
import re # Para usar expresiones regulares
import requests # Para hacer solicitudes http

from elasticsearch import Elasticsearch
from elasticsearch import helpers

def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

def main():
    # Queremos imprimir bonito
    pp = pprint.PrettyPrinter(indent=2)

    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch()

    # La primera consulta que ejecutamos via GET sin especificar campos
    results = es.search(
        index="reddit-mentalhealth",
        q="diagnosed"
        )

    # pp.pprint(results)
    print(str(results["hits"]["total"]) + " resultados para la query q=\"diagnosed\"")

    # Lanzamos el scaneo
    results = es.search(
            index="reddit-mentalhealth3",
    body={
        "size":0,
        "query": {
                "query_string": {
                    "default_field": "selftext",
                    "query": "medicament OR prescrib"
                    }
            },
            "aggs": {
                "Terminos": {
                    "significant_terms": {
                        "field": "selftext",
                        "size": 1000,
                        "gnd":{}
                        }
                    }
                },
                "sort": [
                {
                    "aggregations.Terminos.buckets.doc_count": "desc"
                },
                ]
        }
    )
    # Iteramos sobre los resultados, no es preciso preocuparse de las
    # conexiones consecutivas que hay que hacer con el servidor ES
    results2 = es.search(
            index="reddit-mentalhealth3",
            body={
                "size": 0,
                "query": {
                "query_string": {
                "query": "medication AND depression"
                }
            },
            "aggs": {
                "Terminos": {
                    "significant_terms": {
                        "field": "title",
                        "size": 2000,
                        "gnd": {}
                        }
                    }
                },
            "sort": [
            {
            "aggregations.Terminos.buckets.doc_count": "desc"
            },
            ]
        }
    )


    #creamos un array vacio para las claves que se han obtenido
    claves = []
    #creamos un array vacio para las ids de estas clave que maneja wikidata
    idsComprobar = []
    # Rellenamos con el for el array con las palabras e ignorando las que tienen numeros
    #for r in range (0,len(results["aggregations"]["Terminos"]["buckets"])):
    for r in range (0,len(results["aggregations"]["Terminos"]["buckets"])):
        tmp = results["aggregations"]["Terminos"]["buckets"][r]["key"]
        if(any(char.isdigit() for char in tmp)!=True):
            claves.append(results["aggregations"]["Terminos"]["buckets"][r]["key"]);

    for r2 in range (0,len(results2["aggregations"]["Terminos"]["buckets"])):
        tmp = results2["aggregations"]["Terminos"]["buckets"][r2]["key"]
        if(any(char.isdigit() for char in tmp)!=True and results2["aggregations"]["Terminos"]["buckets"][r2]["key"] not in claves):
            claves.append(results2["aggregations"]["Terminos"]["buckets"][r2]["key"]);

    #Se establece la url a la que hacer las solicitudes JSON
    url = "https://www.wikidata.org/w/api.php"

    for x in range (0,len(claves)):
    #Se estabelecen los parametros, solo va a cambiar el indice de las claves
        if(x%25==0):
            print("Current element: "+claves[x]+" id... "+str(x)+" of "+str(len(claves))+"\n")
        params = dict(
        action = "wbsearchentities",
        search = claves[x],
        language = "en",
        format = "json"
        )
        #Se obtiene la respuesta
        resp = requests.get(url=url, params=params)
        #y el objeto json
        data = resp.json()
        # Para cada posible valor, comprobar si es un medicamento
        for v in range (0,len(data["search"])):
            vid=data["search"][v]["id"]
            #Agrego las ids a un array que tendra todas las ids para comprobar
            #Solo si no estan
            if vid not in idsComprobar:
                idsComprobar.append(vid)

    # Al final del bucle, ids tendra todas las ids con las que trabaja wikidata.
    # Ahora se trabajara con estas ids para obtener sus constraints

    #se almacena en una variable la id de medication y la id de instance of,
    #su obtencion ha sido mediante la API, esta documentado.
    idmedication = "Q12140"
    propiedad = "P31"
    #se almacena en un array las ids que son medicamentos
    medicamentos = []

    for y in range (0,len(idsComprobar)):
    #Se estabelecen los parametros, solo va a cambiar el indice de las claves
        if(y%25==0):
            print("Checking ids "+str(y)+" of "+str(len(idsComprobar))+"\n")
        params = dict(
        action = "wbgetentities",
        ids = idsComprobar[y],
        languages = "en",
        format = "json"
        )
        #Se obtiene la respuesta
        resp = requests.get(url=url, params=params)
        #y el objeto json
        data = resp.json()
        #Si tiene alguna instanceof
        if "P31" in data["entities"][idsComprobar[y]]["claims"]:
            #Para facilitar el chorro, todo son campos del json
            instances = data["entities"][idsComprobar[y]]["claims"]["P31"]
            for m in range (0,len(instances)):
                #Para cada instanceof, comprueba si es un medicamento
                idinstanceact = instances[m]["mainsnak"]["datavalue"]["value"]["id"]
                if(idinstanceact == idmedication):
                    #Si lo es, lo añade a medicamentos
                    medicamentos.append(data["entities"][idsComprobar[y]]["labels"]["en"]["value"])
                    break

    #Al terminar este bucle, tendremos todos los medicamentos
    #Paso esta lista a un archivo que se llame medicamentos
    f=open("medicamentos.txt","wb")
    for n in range (0,len(medicamentos)):
        f.write(medicamentos[n].encode("UTF-8")+'\n'.encode("UTF-8"))
    f.close()
    print("Done!")
    # Preparar consultas con default_operator y docvalue_fields, por ejemplo
    # para obtener los textos únicamente para hacer data mining...
    #
    # ...

if __name__ == '__main__':
    main()

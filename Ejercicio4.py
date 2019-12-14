#-------------------------------------------------------------------------------
# Name:        Ejercicio4
#
# Author:      UO266321
#-------------------------------------------------------------------------------

import json # Para poder trabajar con objetos JSON
from elasticsearch import Elasticsearch # Para poder trabajar con objetos Elasticsearch

#Configuracion necesaria para el funcionamiento del ejercicio
def configureMapping():
    es = Elasticsearch()
    mapping  = {
        "properties": {
            "author": {
                "type": "text",
                "fielddata": "true"
            },
            "selftext": {
                "type": "text",
                "fielddata": "true"
            },
            "title": {
                "type": "text",
                "fielddata": "true"
            },
            "subreddit": {
                "type": "text",
                "fielddata": "true"
            }
        }
    }

    response = es.indices.put_mapping(
        index="reddit-mentalhealth",
        body=mapping,
        ignore=400 # ignore 400 already exists code
    )


#Busqueda en ElasticSearch de las palabras dadas en query
def readElasticsearch(query):

    #Para evitar que la busqueda retorne las palabras dadas se hará uso de
    #una lista con todas las palabras de la query
    querywords = query.replace('\"', '').split()

    es = Elasticsearch()

    #numero de resultados maximo que dara la consulta
    numero_salidas = 500

    results = es.search(
        index="reddit-mentalhealth",
        body = {
            "size": 0,
            "query": {
                "query_string": {
                    "default_field": "selftext",
                    "query": query
                }
            },
            "aggs": {
                "Title": {
                    "significant_terms": {
                        "field": "title",
                        "size": numero_salidas,
                        "gnd": {}
                    }
                },
                "Text": {
                    "significant_terms": {
                        "field": "selftext",
                        "size": numero_salidas,
                        "gnd": {}
                    }
                },
                "Subreddit": {
                    "significant_terms": {
                        "field": "subreddit",
                        "size": numero_salidas,
                        "gnd": {}
                    }
                }
            }
    })

    words = []
    for j in ["Subreddit", "Text", "Title"]:
        for i in results["aggregations"][j]["buckets"]:
            if i["key"] not in words and i["key"] not in querywords:
                words.append(i["key"])
    print("Obtenidas palabras relacionadas con "+query+": "+str(len(words)))
    return words


#Metodo para leer el archivo Json con el nombre dado que devuelve los
#titulos de los articulos del archivo
def readJson(name):
    words = []
    #Es necesario especificar el encoding para que no de error
    with open(name, encoding='utf-8-sig') as json_file:
        data = json.load(json_file)
        for word in data:
            words.append(word['title'])
    print("Obtenidas palabras del json "+name+": "+str(len(words)))
    return words


#Dados el resultado de la query y la lista de titulos del json, devuelve
#una lista de palabras que aparezcan en los dos
def getComorbidities(query, json):
    wordsElasticSearch = readElasticsearch(query)
    wordsJson = readJson(json)
    finalWords = []


    for we in wordsElasticSearch:
        for wj in wordsJson:
            #separamos las palabras de los titulos y comprobamos que no se
            #hayan añadido ya
            if we in wj.split() and we not in finalWords:
                finalWords.append(we)
    return finalWords


def main():
    configureMapping()

    print("Ejercicio 4:")

    #Para el suicidio se utilizaran las siguientes palabras:
    query1 = "suicide suicidal \"kill myself\" \"killing myself\" \"end my life\""
    #Para las conductas autolesivas se utilizaran las siguientes palabras:
    query2 = "\"self harm\""

    #los archivos Json deben estar en la misma carpeta que el ejercicio
    #y llamarse asi:
    json1 = "GoogleScholar-SuicideComorbidity.json"
    json2 = "GoogleScholar-SelfHarmComorbidity.json"

    result1 = getComorbidities(query1, json1)
    result2 = getComorbidities(query2, json2)


    #Generacion del archivo con los resultados:
    name = 'ejercicio4.txt'
    f = open(name, "w")
    f.write("Resultados de comorbilidades para suicidio:"+"\n")
    for e in result1:
        f.write(e+"\n")

    f.write("\n")
    f.write("Resultados de comorbilidades para conductas autolesivas:"+"\n")
    for e in result2:
        f.write(e+"\n")
    f.close()

    print("Se ha generado un archivo "+name+" con los resultados")


if __name__ == '__main__':
    main()

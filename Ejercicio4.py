#-------------------------------------------------------------------------------
# Name:        Ejercicio4
#-------------------------------------------------------------------------------

import json
import codecs
from elasticsearch import Elasticsearch

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

    es.indices.create(
        index="reddit-mentalhealth",
        body=mapping,
        ignore=400 # ignore 400 already exists code
    )


def readElasticsearch(query):

    querywords = query.replace('\"', '').split()

    es = Elasticsearch()

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


def readJson(name):
    words = []
    with open(name, encoding='utf-8-sig') as json_file:
        data = json.load(json_file)
        for word in data:
            words.append(word['title'])
    print("Obtenidas palabras del json "+name+": "+str(len(words)))
    return words



def getComorbidities(query, json):
    wordsElasticSearch = readElasticsearch(query)
    wordsJson = readJson(json)
    finalWords = []


    for we in wordsElasticSearch:
        for wj in wordsJson:
            if we in wj.split() and we not in finalWords:
                finalWords.append(we)
    return finalWords


def main():
    configureMapping()

    print("Ejercicio 4:")

    query1 = "suicide suicidal \"kill myself\" \"killing myself\" \"end my life\""
    query2 = "\"self harm\""

    json1 = "GoogleScholar-SuicideComorbidity.json"
    json2 = "GoogleScholar-SuicideComorbidity.json"

    result1 = getComorbidities(query1, json1)
    result2 = getComorbidities(query2, json2)

    #for e in result1:
        #print (e)

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


"""
    name = 'ejercicio4.json'
    with open(name, 'w') as file:
        json.dump(words1, file, indent=4)
        json.dump(words2, file, indent=4)



    print("Se ha generado un archivo "+name+" con los resultados")

"""
if __name__ == '__main__':
    main()

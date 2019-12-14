#-------------------------------------------------------------------------------
# Name:        Ejercicio4
#-------------------------------------------------------------------------------

import json
from elasticsearch import Elasticsearch

def properties():
    es = Elasticsearch()

    #configuracion usada en el indice
    argumentos = {
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

    es.indices.put_mapping(index="reddit-mentalhealth",doc_type="post",body=argumentos,ignore=400)


def search(query):

    querywords = query.replace('\"', '').split()

    es = Elasticsearch()

    numero_salidas = 1000

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

    return words


def main():

    properties()

    print("Ejercicio 4:")


    query1 = "suicide  suicidal \"kill myself\" \"killing myself\" \"end my life\""
    query2 = "\"self harm\""

    words1 = search(query1)
    words2 = search(query2)


    name = 'ejercicio4.json'
    with open(name, 'w') as file:
        json.dump(words1, file, indent=4)
        json.dump(words2, file, indent=4)



    print("Se ha generado un archivo "+name+" con los resultados")


if __name__ == '__main__':
    main()

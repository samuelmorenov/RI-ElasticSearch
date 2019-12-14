#-------------------------------------------------------------------------------
# Name:        Ejercicio4
#-------------------------------------------------------------------------------

import json
from elasticsearch import Elasticsearch

def main():

    print("Ejercicio 4\n")

    es = Elasticsearch()

    numero_salidas = 5
    query = "suicide suicidal kill myself killing myself end my life"

    results = es.search(
        index="reddit-mentalhealth",
        body = {
            "size": 0,
            "query": {
                "query_string": {
                    "default_field": "selftext",
                    "query": query,
                    "operator": "or"
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
            if i["key"] not in words:
                words.append(i["key"])

    # Guardar el archivo con formato json
    with open('ejercicio4.json', 'w') as file:
        json.dump(words, file, indent=4)

    print("Se ha generado un archivo ejercicio4.json con los resultados")

if __name__ == '__main__':
    main()

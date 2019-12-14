#-------------------------------------------------------------------------------
# Name:        Ejercicio1
# Purpose:
#
# Author:      UO258774
#
# Created:     30/11/2019
# Copyright:   (c) UO258774 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import json # Para poder trabajar con objetos JSON
from elasticsearch import Elasticsearch # Para poder trabajar con objetos Elasticsearch

def main():

    print("Ejercicio 4\n")

    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch()

    numero_salidas = 5

    results = es.search(
        index="reddit-mentalhealth",
        body = {
            "size": 0,
            "query": {
                "query_string": {
                    "default_field": "selftext",
                    "query": "suicide",
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



"""

##########################################################
##########################################################
##########################################################
    words = []
    for j in ["Subreddit", "Text", "Title"]:
        [words.append(i["key"]) for i in results["aggregations"][j]["buckets"] if i["key"] not in words and any(i["key"] in s for s in data)]

    print("--- Expresiones usadas ---")
    for word in words:
        print("\t",word)
    print()
##########################################################
##########################################################
##########################################################
"""


"""


    # Objetos json (cada uno tendrá una key con el termino)
    terminosSignificativos = results["aggregations"]["TerminosSignificativos"]["buckets"]

    # Creamos un vector para almacenar los terminos
    vectorTerminos = []

    # Añadimos los términos
    for i in terminosSignificativos:
        vectorTerminos.append(i.get("key"))

    #############################################################################
    # Ya tenemos los téminos que están relaccionados con la palabra,
    # ahora obtendremos los 10 post que más de esas palabras contengan.
    #############################################################################

    # 1. Obtener un string con las palabras
    palabras=""
    for i in vectorTerminos:
        palabras+=i+" "

    # 2. Hacer la consulta
    results2=es.search(
        index="reddit-mentalhealth",
        body = {
            "size":100,
            "query": {
                "match": {
                    "selftext": {
                        "query": palabras,
                        "operator": "or"
                    }
                }
            }
        }
    )

    # Obtenemos los 100 primeros posts
    posts=results2["hits"]["hits"]

    # Ya tenemos los 100 primeros posts, pero sólo hace falta el autor,
    # la fecha de creación y el texto. Hay que hacer un nuevo archivo
    # json con los posts.

    salida={}
    salida['posts']=[]
    for i in posts:

        # Comprobamos que no haya un post igual, ya que hay post repetidos
        # de gente que lo subio varias veces
        yaEsta=False
        for j in salida['posts']:
            if j.get("Texto: ")==i["_source"].get("selftext"):
                yaEsta=True

        if yaEsta==False:
            salida['posts'].append({
                'Autor: ':i["_source"].get("author"),
                'Fecha de creacion: ':i["_source"].get("created_utc"),
                'Texto: ':i["_source"].get("selftext")
            })

    # Guardar el archivo con formato json
    with open('salida.json', 'w') as file:
        json.dump(salida, file, indent=4)

    print("Se ha generado un archivo salida.json con los resultados")

"""

"""
def config():
    pp = pprint.PrettyPrinter(indent=2)

    es = Elasticsearch()

    #configuracion usada en el indice
    argumentos = {
      "properties": {
        "author": {
            "type": "text",
            "term_vector": "yes",
            "fielddata": "true"
        },
        "selftext": {
            "type":"text",
            "term_vector": "yes",
            "fielddata": "true"
        },
        "title": {
            "type":"text",
            "term_vector": "yes",
            "fielddata": "true"
        },
        "subreddit": {
            "type":"text",
            "term_vector": "yes",
            "fielddata": "true"
        }
      }
    }

    es.indices.put_mapping(index="reddit-mentalhealth",doc_type="post",body=argumentos,ignore=400)

    return es
"""

if __name__ == '__main__':
    main()

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

    print("Ejercicio 1\n")

    print("Introduzca la palabra para hacer la búsqueda (por ejemplo alcoholism)")
    palabra=input()

    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch()

    #############################################################################
    # 1º Búsqueda: Sacar términos relaccionados con la palabra
    #############################################################################

    # Ésta consulta da una lista de palabras relaccionadas con alcoholismo
    # (o la palabra que pongamos en lugar de alcoholism),
    # sin palabras vacías como and o the que no aportan nada
    results = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    "query":palabra
                }
            },
            "aggs": {
                "TerminosSignificativos":{
                    "significant_terms":{
                        "field":"selftext",
                        "size":10,
                        "gnd":{}
                    }
                }
            }
        }
    )

    # Objetos json (cada uno tendrá una key con el termino)
    terminosSignificativos = results["aggregations"]["TerminosSignificativos"]["buckets"]

    # Creamos un set para almacenar los terminos sin que se repitan
    vectorTerminos = set([])

    print("\n####################################\nTérminos encontrados con la 1º búsqueda\n####################################\n")
    # Añadimos los términos
    for i in terminosSignificativos:
        vectorTerminos.add(i.get("key"))
        print("\t"+i.get("key"))

    #############################################################################
    # 2º Búsqueda: Ahora hay que sacar de esas palabras, más relaccionadas
    #############################################################################

    # Obtener un string con las palabras
    palabras=""
    for i in vectorTerminos:
        palabras+=i+" "

    results2 = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    #"query":palabra
                    "query":palabras
                }
            },
            "aggs": {
                "TerminosSignificativos":{
                    "significant_terms":{
                        "field":"selftext",
                        "size":10,
                        "gnd":{}
                    }
                }
            }
        }
    )

    # Objetos json (cada uno tendrá una key con el termino)
    terminosSignificativos2 = results2["aggregations"]["TerminosSignificativos"]["buckets"]

    print("\n####################################\nTérminos encontrados con la 2º búsqueda\n####################################\n")
    # Añadimos los términos
    for i in terminosSignificativos2:
        vectorTerminos.add(i.get("key"))
        print("\t"+i.get("key"))

    #############################################################################
    # 3º Búsqueda
    #############################################################################

    # Obtener un string con las palabras
    palabras=""
    for i in vectorTerminos:
        palabras+=i+" "

    results3 = es.search(
        index="reddit-mentalhealth",
        body = {
            "size":0,
            "query":{
                "query_string":{
                    "default_field":"selftext",
                    "query":palabras
                }
            },
            "aggs": {
                "TerminosSignificativos":{
                    "significant_terms":{
                        "field":"selftext",
                        "size":10,
                        "gnd":{}
                    }
                }
            }
        }
    )

    # Objetos json (cada uno tendrá una key con el termino)
    terminosSignificativos3 = results3["aggregations"]["TerminosSignificativos"]["buckets"]

    print("\n####################################\nTérminos encontrados con la 3º búsqueda\n####################################\n")
    # Añadimos los términos
    for i in terminosSignificativos3:
        vectorTerminos.add(i.get("key"))
        print("\t"+i.get("key"))

    #############################################################################
    # Ya tenemos los téminos que están relaccionados con la palabra,
    # ahora obtendremos los 10 post que más de esas palabras contengan.
    #############################################################################

    print("\n####################################\nTotal de términos encontrados\n####################################\n")
    for i in vectorTerminos:
        print("\t"+i)

    # 1. Obtener un string con las palabras
    palabras=""
    for i in vectorTerminos:
        palabras+=i+" "

    # 2. Hacer la consulta
    results2=es.search(
        index="reddit-mentalhealth",
        body = {
            "size":25, # Obtenemos los 25 primeros resultados
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

    # Obtenemos los 25 primeros posts
    posts=results2["hits"]["hits"]

    # Ya tenemos los 25 primeros posts, pero sólo hace falta el autor,
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
                'Autor':i["_source"].get("author"),
                'Fecha de creacion: ':i["_source"].get("created_utc"),
                'Texto: ':i["_source"].get("selftext"),
                '_id: ':i["_source"].get("id")
            })

    # Guardar el archivo con formato json
    with open('salida.json', 'w') as file:
        json.dump(salida, file, indent=4)

    print("Se ha generado un archivo salida.json con los resultados")

if __name__ == '__main__':
    main()

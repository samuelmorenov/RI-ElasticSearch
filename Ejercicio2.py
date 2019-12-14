#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      UO258774
#
# Created:     12/12/2019
# Copyright:   (c) UO258774 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import json # Para poder trabajar con objetos JSON
from elasticsearch import Elasticsearch # Para poder trabajar con objetos Elasticsearch

def main():

    # Nos conectamos por defecto a localhost:9200
    es = Elasticsearch()

    # Leer el archivo json que ha generado el ejercicio 1
    # y obtener un vector con todos los id de los documentos

    vectorId = set([])

    with open('salida.json') as file:
        data = json.load(file)
        for documento in data['posts']:
            vectorId.add(documento['_id: '])

    # Ya tenemos un vector con los id's de los documentos que generó
    # el ejercicio 1

    # Ahora hacemos una consulta MLT que obtenga documentos parecidos
    # a esos pasandole el id

    # Para ello tenemos que crear el cuerpo de la consulta (en JSON)
    body={'query':{'more_like_this':{'like':[]}}}
    body['size']=15 # Obtener los 15 documentos más relevantes
    for i in range(len(vectorId)):
        body.get('query').get('more_like_this').get('like').append({
            '_index': "reddit-mentalhealth",
            '_id': vectorId.pop()
        })

    # Guardar el archivo con formato json (para verlo y comprobar que está bien)
    with open('cuerpo.json', 'w') as file:
        json.dump(body, file, indent=4)

    # Ya tenemos el cuerpo de la consulta, sólo hay que crear la query ahora
    # y ejecutarla
    results = es.search(
        index="reddit-mentalhealth",
        body = body
    )

    ########################################################################
    # Una vez tenemos el resultado de la consulta, sacar los documentos a un
    # archivo json es igual que en el primer ejercicio
    ########################################################################

    # Obtenemos los 100 primeros posts
    posts=results["hits"]["hits"]

    # Ya tenemos los 15 primeros posts, pero sólo hace falta el autor,
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
    with open('salidaEjercicio2.json', 'w') as file:
        json.dump(salida, file, indent=4)

    print("Se ha generado un archivo salidaEjercicio2.json con los resultados")


if __name__ == '__main__':
    main()

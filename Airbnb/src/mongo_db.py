from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional
from datetime import datetime, timedelta
from sqlite_db import existe_propietario, existe_huesped, calificar_propietario

#uri = "mongodb+srv://marcoszucche:onwwF3D0aET1oCMo@cluster0.k3puihi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
#client = MongoClient(uri, server_api=ServerApi('1'))
client = MongoClient("mongodb://localhost:27017")
db = client["short_term_rentals"]

class ReseñaModel(BaseModel):
    id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    reserva_id: str
    propietario_id: str
    huesped_id: str
    propiedad_id: str
    calificacion_propiedad: float = Field(ge=0, le=5)
    calificacion_propietario: float = Field(ge=0, le=5)
    comentario: str
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True

    def validar_calificacion(cls, v):
        if not 0 <= v <= 5:
            raise ValueError('La calificación debe estar entre 0 y 5')
        return v

def crear_reseña(
        reserva_id: str,
        propietario_id: str,
        huesped_id: str,
        propiedad_id: str,
        calificacion_propiedad: float,
        calificacion_propietario: float,
        comentario: str
) -> Optional[str]:
    try:
        # Validar que existan todas las entidades relacionadas
        #if not existe_reserva(reserva_id):
        #    raise ValueError("La reserva especificada no existe")

        if not existe_propietario(propietario_id):
            raise ValueError("El propietario especificado no existe")

        if not existe_huesped(huesped_id):
            raise ValueError("El huésped especificado no existe")

        if not existe_propiedad(propiedad_id):
            raise ValueError("La propiedad especificada no existe")

        # Crear la reseña
        reseña = ReseñaModel(
            reserva_id=reserva_id,
            propietario_id=propietario_id,
            huesped_id=huesped_id,
            propiedad_id=propiedad_id,
            calificacion_propiedad=calificacion_propiedad,
            calificacion_propietario=calificacion_propietario,
            comentario=comentario
        )

        # Insertar en MongoDB
        result = db["reseñas"].insert_one(reseña.dict(by_alias=True))

        # Actualizar calificación promedio de la propiedad
        actualizar_calificacion_propiedad(propiedad_id)

        # Actualizar calificación promedio del propietario
        actualizar_calificacion_propietario(propietario_id)

        return str(result.inserted_id)

    except Exception as e:
        print(f"Error al crear la reseña: {str(e)}")
        return None

def actualizar_calificacion_propiedad(propiedad_id: str):
    try:
        # Obtener todas las reseñas de la propiedad
        reseñas = db["reseñas"].find({"propiedad_id": propiedad_id})

        total_calificaciones = 0
        cantidad_reseñas = 0

        for reseña in reseñas:
            total_calificaciones += reseña["calificacion_propiedad"]
            cantidad_reseñas += 1

        if cantidad_reseñas > 0:
            nueva_calificacion = round(total_calificaciones / cantidad_reseñas, 2)

            # Actualizar la propiedad
            db["propiedades"].update_one(
                {"_id": propiedad_id},
                {
                    "$set": {
                        "calificacion": nueva_calificacion,
                        "resenias_count": cantidad_reseñas
                    }
                }
            )

    except Exception as e:
        print(f"Error al actualizar calificación de la propiedad: {str(e)}")

def actualizar_calificacion_propietario(propietario_id: str):
    try:
        # Obtener todas las reseñas del propietario
        reseñas = db["reseñas"].find({"propietario_id": propietario_id})

        total_calificaciones = 0
        cantidad_reseñas = 0

        for reseña in reseñas:
            total_calificaciones += reseña["calificacion_propietario"]
            cantidad_reseñas += 1

        if cantidad_reseñas > 0:
            nueva_calificacion = round(total_calificaciones / cantidad_reseñas, 2)
            calificar_propietario(propietario_id, nueva_calificacion)

    except Exception as e:
        print(f"Error al actualizar calificación del propietario: {str(e)}")

def obtener_reseñas_propiedad(propiedad_id: str):
    try:
        reseñas = db["reseñas"].find({"propiedad_id": propiedad_id}).sort("created_at", -1)
        return list(reseñas)
    except Exception as e:
        print(f"Error al obtener reseñas: {str(e)}")
        return []

def obtener_reseñas_propietario(propietario_id: str):
    try:
        reseñas = db["reseñas"].find({"propietario_id": propietario_id}).sort("created_at", -1)
        return list(reseñas)
    except Exception as e:
        print(f"Error al obtener reseñas: {str(e)}")
        return []


class PropiedadModel(BaseModel):
    id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    propietario_id: str
    ubicacion: dict
    tipo: str
    descripcion: str
    precio: float
    servicios: List[str] = []
    calificacion: float = Field(default=0, ge=0, le=5)
    resenias_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True


def existe_propiedad(id_str: str) -> bool:
    result = db["propiedades"].find_one({"_id": id_str})

    if not result:
        try:
            obj_id = ObjectId(id_str)
            result = db["propiedades"].find_one({"_id": obj_id})
        except Exception as e:
            print(f"Error al convertir a ObjectId: {str(e)}")
            return False

    if result:
        return True
    else:
        print("Error: No se encontró ningún documento con ese ID.")
        return False

def get_document_by_id(id_str: str):
    try:
        # Primero intentamos buscar el documento con el ID como string
        result = db["propiedades"].find_one({"_id": id_str})

        # Si no lo encontramos, intentamos con ObjectId
        if not result:
            try:
                obj_id = ObjectId(id_str)
                result = db["propiedades"].find_one({"_id": obj_id})
            except Exception as e:
                print(f"Error al convertir a ObjectId: {str(e)}")
                return None

        if not result:
            print(f"No se encontró propiedad con ID: {id_str}")
            return None

        return result
    except Exception as e:
        print(f"Error al obtener propiedad: {str(e)}")
        return None

def ver_propiedades():
    propiedades = db["propiedades"].find()
    for prop in propiedades:
        print(f"ID: {prop['_id']}")
        print(f"Ubicación: {prop['ubicacion']['ciudad']}, {prop['ubicacion']['direccion']}")
        print(f"Tipo: {prop['tipo']}")
        print(f"Precio: ${prop['precio']}")
        print("-" * 50)


def ver_propiedades_ultima_semana():
    try:
        fecha_inicio = datetime.now() - timedelta(days=7)

        # Crear el filtro para la consulta
        filtro = {
            "created_at": {
                "$gte": fecha_inicio
            }
        }

        # Realizar la consulta
        propiedades = db["propiedades"].find(filtro)

        # Contador para las propiedades
        contador = 0

        print("\nPropiedades agregadas en la última semana:")
        print("-" * 50)

        for prop in propiedades:
            contador += 1
            print(f"ID: {prop['_id']}")
            print(f"Fecha de creación: {prop['created_at']}")
            print(f"Ubicación: {prop['ubicacion']['ciudad']}, {prop['ubicacion']['direccion']}")
            print(f"Tipo: {prop['tipo']}")
            print(f"Precio: ${prop['precio']}")
            print("-" * 50)

        print(f"\nTotal de propiedades encontradas: {contador}")
        return contador

    except Exception as e:
        print(f"Error al consultar propiedades: {str(e)}")
        return 0


def ver_propiedades_premium_caba():
    try:
        # Crear el filtro para la consulta
        filtro = {
            "calificacion": {"$gt": 4.5},
            "ubicacion.ciudad": {"$regex": "^CABA$", "$options": "i"}
        }

        # Realizar la consulta
        propiedades = db["propiedades"].find(filtro).sort("calificacion", -1)  # Ordenadas por calificación descendente

        # Contador para las propiedades
        contador = 0

        print("\nPropiedades premium en CABA (calificación > 4.5):")
        print("-" * 50)

        for prop in propiedades:
            contador += 1
            print(f"ID: {prop['_id']}")
            print(f"Calificación: {prop['calificacion']}")
            print(f"Ubicación: {prop['ubicacion']['direccion']}")
            print(f"Tipo: {prop['tipo']}")
            print(f"Precio: ${prop['precio']}")
            print(f"Cantidad de reseñas: {prop['resenias_count']}")
            print(f"Servicios: {', '.join(prop['servicios'])}")
            print("-" * 50)

        print(f"\nTotal de propiedades encontradas: {contador}")
        return contador

    except Exception as e:
        print(f"Error al consultar propiedades: {str(e)}")
        return 0

def solicitar_zonas_turisticas():
    """
    Solicita al usuario que ingrese zonas turísticas una por una.
    Retorna una lista con las zonas ingresadas.
    """
    zonas = []
    print("\nIngreso de zonas turísticas (presione Enter sin escribir nada para terminar)")

    while True:
        zona = input("\nIngrese el nombre de una zona turística: ").strip()
        if not zona:  # Si el usuario presiona Enter sin escribir nada
            break
        zonas.append(zona)

    return zonas

def obtener_tipos_alojamiento_populares_resenias():
    """
    Obtiene los tipos de alojamiento que:
    1. Han recibido más de 20 reseñas O
    2. Están ubicadas en zonas turísticas ingresadas por el usuario
    """
    try:
        # Solicitar zonas turísticas al usuario
        print("\n=== Análisis de Tipos de Alojamiento Populares ===")
        zonas_turisticas = solicitar_zonas_turisticas()

        if not zonas_turisticas:
            print("\nNo se ingresaron zonas turísticas. Se mostrarán solo tipos con más de 20 reseñas.")
        else:
            print("\nZonas turísticas ingresadas:", ", ".join(zonas_turisticas))

        # Pipeline para tipos con más de 20 reseñas
        pipeline_reseñas = [
            {"$match": {"resenias_count": {"$gt": 20}}},
            {"$group": {
                "_id": "$tipo",
                "count": {"$sum": 1},
                "promedio_reseñas": {"$avg": "$resenias_count"}
            }}
        ]

        # Pipeline para tipos en zonas turísticas
        pipeline_zonas = [
            {"$match": {"ubicacion.ciudad": {"$in": zonas_turisticas}}},
            {"$group": {
                "_id": "$tipo",
                "count": {"$sum": 1},
                "zonas": {"$addToSet": "$ubicacion.ciudad"}
            }}
        ]

        # Ejecutar ambas consultas
        tipos_por_reseñas = list(db["propiedades"].aggregate(pipeline_reseñas))
        tipos_por_zona = list(db["propiedades"].aggregate(pipeline_zonas)) if zonas_turisticas else []

        # Crear conjuntos de tipos para cada criterio
        tipos_reseñas = {tipo["_id"] for tipo in tipos_por_reseñas}
        tipos_zonas = {tipo["_id"] for tipo in tipos_por_zona}

        # Combinar resultados
        print("\nResultados del análisis:")
        print("-" * 50)

        # Mostrar tipos con más de 20 reseñas
        if tipos_reseñas:
            print("\nTipos de alojamiento con más de 20 reseñas:")
            for tipo in tipos_por_reseñas:
                print(f"\n- {tipo['_id']}:")
                print(f"  Cantidad de propiedades: {tipo['count']}")
                print(f"  Promedio de reseñas: {round(tipo['promedio_reseñas'], 1)}")
        else:
            print("\nNo se encontraron tipos de alojamiento con más de 20 reseñas.")

        # Mostrar tipos en zonas turísticas
        if tipos_zonas:
            print("\nTipos de alojamiento en las zonas ingresadas:")
            for tipo in tipos_por_zona:
                print(f"\n- {tipo['_id']}:")
                print(f"  Cantidad de propiedades: {tipo['count']}")
                print(f"  Presente en zonas: {', '.join(tipo['zonas'])}")
        elif zonas_turisticas:
            print("\nNo se encontraron propiedades en las zonas ingresadas.")

        # Mostrar tipos que cumplen ambos criterios
        tipos_comunes = tipos_reseñas & tipos_zonas
        if tipos_comunes:
            print("\nTipos de alojamiento que cumplen ambos criterios:")
            for tipo in tipos_comunes:
                print(f"- {tipo}")

        # Estadísticas generales
        todos_los_tipos = tipos_reseñas | tipos_zonas
        print("\n" + "=" * 50)
        print(f"Total de tipos de alojamiento únicos encontrados: {len(todos_los_tipos)}")
        if todos_los_tipos:
            print("Tipos encontrados:", ", ".join(sorted(todos_los_tipos)))

        return list(todos_los_tipos)

    except Exception as e:
        print(f"Error al consultar tipos de alojamiento: {str(e)}")
        return []



if __name__ == "__main__":
    ver_propiedades()

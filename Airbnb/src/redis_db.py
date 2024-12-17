from typing import Any, Dict, re, List, Tuple

from bson import ObjectId
from pymongo import MongoClient
import redis
import uuid
from datetime import datetime
from tabulate import tabulate
from collections import Counter, defaultdict
from datetime import timedelta
from mongo_db import get_document_by_id
from pymongo.server_api import ServerApi


r = redis.Redis(host='localhost', port=6379, db=0)

uri = "mongodb+srv://marcoszucche:onwwF3D0aET1oCMo@cluster0.k3puihi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["short_term_rentals"]

def crear_reserva(usuario_id, propiedad_id, fecha_inicio, fecha_fin, monto, metodo_pago, estado_reserva, ciudad, imprimir_tabla=True):
    reserva_id = f"reserva:{str(uuid.uuid4())}"
    reserva_data = {
        'usuario_id': usuario_id,
        'propiedad_id': propiedad_id,
        'fecha_inicio': fecha_inicio,
        'fecha_fin': fecha_fin,
        'monto': monto,
        'metodo_pago': metodo_pago,
        'estado_reserva': estado_reserva,
        'ciudad': ciudad,
        'fecha_creacion': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    try:
        r.hset(reserva_id, mapping=reserva_data)
        if imprimir_tabla:
            print(f"\nReserva {reserva_id} creada exitosamente en Redis.\n")
            stored_reserva = r.hgetall(reserva_id)
            headers = ["Campo", "Valor"]
            rows = [(key.decode('utf-8'), value.decode('utf-8')) for key, value in stored_reserva.items()]
            print(tabulate(rows, headers=headers, tablefmt="grid"))
        else:
            print(f"\nReserva {reserva_id} creada exitosamente en Redis.")

        return reserva_id
    except redis.RedisError as e:
        print(f"Error al crear la reserva en Redis: {e}")
        return None

def agregar_pago(reserva_id, estado_pago, monto, metodo_pago, imprimir_tabla=True):
    pago_id = f"pago:{str(uuid.uuid4())}"
    pago_data = {
        'reserva_id': reserva_id,
        'estado_pago': estado_pago,
        'monto': monto,
        'metodo_pago': metodo_pago,
        'fecha_pago': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    try:
        r.hset(pago_id, mapping=pago_data)
        if imprimir_tabla:
            print(f"\nPago {pago_id} registrado exitosamente en Redis.\n")
            stored_pago = r.hgetall(pago_id)
            headers = ["Campo", "Valor"]
            rows = [(key.decode('utf-8'), value.decode('utf-8')) for key, value in stored_pago.items()]
            print(tabulate(rows, headers=headers, tablefmt="grid"))
        else:
            print(f"\nPago {pago_id} registrado exitosamente en Redis.")

        return pago_id
    except redis.RedisError as e:
        print(f"Error al registrar el pago en Redis: {e}")
        return None

def ver_reservas_y_pagos():
    print("\n----- Reservas en Redis -----")
    reservas_keys = r.keys("reserva:*")
    if reservas_keys:
        for reserva_key in reservas_keys:
            tipo_reserva = r.type(reserva_key)
            print(f"Tipo de la clave '{reserva_key.decode('utf-8')}': {tipo_reserva}")

            if tipo_reserva == b'hash':
                reserva_data = r.hgetall(reserva_key)
                print(f"\nReserva ID: {reserva_key.decode('utf-8')}")
                headers = ["Campo", "Valor"]
                rows = [(key.decode('utf-8'), value.decode('utf-8')) for key, value in reserva_data.items()]
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print(f"La clave {reserva_key.decode('utf-8')} no es un hash, es de tipo {tipo_reserva}.")
    else:
        print("No hay reservas almacenadas.")

    print("\n----- Pagos en Redis -----")
    pagos_keys = r.keys("pago:*")
    if pagos_keys:
        for pago_key in pagos_keys:
            tipo_pago = r.type(pago_key)
            print(f"Tipo de la clave '{pago_key.decode('utf-8')}': {tipo_pago}")

            if tipo_pago == b'hash':
                pago_data = r.hgetall(pago_key)
                print(f"\nPago ID: {pago_key.decode('utf-8')}")
                headers = ["Campo", "Valor"]
                rows = [(key.decode('utf-8'), value.decode('utf-8')) for key, value in pago_data.items()]
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print(f"La clave {pago_key.decode('utf-8')} no es un hash, es de tipo {tipo_pago}.")
    else:
        print("No hay pagos almacenados.")

def validar_fecha(fecha):
    patron = r"\d{4}-\d{2}-\d{2}"
    if not re.match(patron, fecha):
        return False
    try:
        año, mes, dia = map(int, fecha.split("-"))
        if año < 1900 or mes < 1 or mes > 12 or dia < 1 or dia > 31:
            return False
    except ValueError:
        return False
    return True

def validar_monto(monto):
    try:
        monto = float(monto)
        return monto > 0
    except ValueError:
        return False

def validar_metodo_pago(metodo_pago):
    metodos_validos = ["Tarjeta", "Efectivo", "Transferencia"]
    return metodo_pago in metodos_validos

def validar_estado_reserva(estado):
    estados_validos = ["Confirmada", "Pendiente", "Cancelada"]
    return estado in estados_validos

def validar_estado_pago(estado):
    estados_validos = ["Pagado", "Pendiente", "Cancelado"]
    return estado in estados_validos

def validar_dni(dni):
    return dni.isdigit() and (7 <= len(dni) <= 8)

def reservas_por_ciudad_ultimo_mes():
    # Calculamos la fecha de hace un mes
    hace_un_mes = datetime.now() - timedelta(days=30)
    reservas_keys = r.keys("reserva:*")

    if reservas_keys:
        reservas_por_ciudad = defaultdict(int)

        for reserva_key in reservas_keys:
            tipo_reserva = r.type(reserva_key)

            if tipo_reserva == b'hash':
                reserva_data = r.hgetall(reserva_key)

                # Recuperamos la fecha de creación y ciudad
                fecha_creacion = reserva_data.get(b'fecha_creacion', None)
                ciudad = reserva_data.get(b'ciudad', None)  # Asegúrate de que el campo 'ciudad' esté en las reservas

                if fecha_creacion and ciudad:
                    fecha_creacion = datetime.strptime(fecha_creacion.decode('utf-8'), '%Y-%m-%d %H:%M:%S')

                    # Si la reserva fue en el último mes
                    if fecha_creacion > hace_un_mes:
                        ciudad = ciudad.decode('utf-8')
                        reservas_por_ciudad[ciudad] += 1

        if reservas_por_ciudad:
            print("\n----- Reservas por Ciudad en el Último Mes -----")
            rows = [(ciudad, reservas) for ciudad, reservas in reservas_por_ciudad.items()]
            headers = ["Ciudad", "Cantidad de Reservas"]
            print(tabulate(rows, headers=headers, tablefmt="grid"))
        else:
            print("No hay reservas en el último mes.")
    else:
        print("No hay reservas almacenadas.")

def obtener_tipo_de_propiedad(popularidad_count: list):
    # Contar la frecuencia de los tipos de propiedad
    counter = Counter(popularidad_count)
    tipo_mas_popular = counter.most_common(1)
    if tipo_mas_popular:
        tipo, cantidad = tipo_mas_popular[0]
        print(f"Tipo de alojamiento más popular: {tipo} con {cantidad} reservas.")
    else:
        print("No se encontraron propiedades populares.")

def consultar_tipo_de_alojamiento_popular():
    # Obtener todas las reservas de Redis
    reservas_keys = r.keys("reserva:*")
    propiedad_ids = []
    for reserva_key in reservas_keys:
        reserva_data = r.hgetall(reserva_key)
        propiedad_id = reserva_data.get(b'propiedad_id', None)
        if propiedad_id:
            propiedad_ids.append(propiedad_id.decode('utf-8'))

    if not propiedad_ids:
        print("No se encontraron reservas.")
        return

    # Obtener los tipos de propiedad de MongoDB
    # Modificamos aquí para obtener múltiples documentos
    propiedades = db["propiedades"].find({"_id": {"$in": [ObjectId(pid) for pid in propiedad_ids]}}) # type: ignore

    # Si no encontramos propiedades en MongoDB
    if not propiedades:
        print("No se encontraron propiedades en la base de datos.")
        return

    # Crear una lista de los tipos de propiedad
    popularidad_count = [prop['tipo'] for prop in propiedades if 'tipo' in prop]

    # Llamar a la función para obtener el tipo de propiedad más popular
    obtener_tipo_de_propiedad(popularidad_count)


def obtener_tipos_alojamiento_populares() -> List[Tuple[str, int]]:
    """
    Analiza las reservas en Redis y las propiedades en MongoDB para determinar
    los tres tipos de alojamiento más populares basados en la cantidad de reservas.

    Args:
        redis_client: Cliente de Redis conectado
        mongo_db: Base de datos de MongoDB conectada

    Returns:
        List[Tuple[str, int]]: Lista de tuplas con (tipo_alojamiento, cantidad_reservas)
        ordenada por cantidad de reservas (top 3)
    """
    try:
        # 1. Obtener todas las reservas de Redis
        reservas_keys = r.keys("reserva:*")
        if not reservas_keys:
            print("No se encontraron reservas en Redis.")
            return []

        # 2. Extraer los IDs de propiedades de las reservas
        propiedad_ids = []
        for reserva_key in reservas_keys:
            reserva_data = r.hgetall(reserva_key)
            propiedad_id = reserva_data.get(b'propiedad_id')
            if propiedad_id:
                propiedad_ids.append(propiedad_id.decode('utf-8'))

        if not propiedad_ids:
            print("No se encontraron IDs de propiedades en las reservas.")
            return []

        # 3. Consultar los tipos de propiedades en MongoDB
        propiedades = db["propiedades"].find(
            {"_id": {"$in": [ObjectId(pid) for pid in propiedad_ids]}},
            {"tipo": 1}
        )

        # 4. Contar la frecuencia de cada tipo de propiedad
        tipos_count = Counter(
            prop['tipo'] for prop in propiedades
            if 'tipo' in prop
        )

        # 5. Obtener los 3 tipos más frecuentes
        tipos_populares = tipos_count.most_common(3)

        # 6. Mostrar resultados
        print("\n=== Tipos de Alojamiento Más Populares ===")
        print("-" * 45)
        for tipo, cantidad in tipos_populares:
            print(f"Tipo: {tipo:<20} Reservas: {cantidad}")
        print("-" * 45)

        return tipos_populares

    except Exception as e:
        print(f"Error al analizar tipos de alojamiento populares: {str(e)}")
        return []

def analizar_areas_demandadas(periodo_dias=180) -> List[Dict[str, Any]]:
    """
    Analiza las áreas más demandadas para alquileres combinando datos de MongoDB y Redis.
    
    Args:
        periodo_dias (int): Período de análisis en días (default: 180 días / 6 meses)
    
    Returns:
        List[Dict[str, Any]]: Lista de diccionarios con análisis por ciudad
    """
    try:
        # 1. Análisis de reservas en Redis
        fecha_inicio = datetime.now() - timedelta(days=periodo_dias)
        reservas_por_ciudad = defaultdict(lambda: {
            'total_reservas': 0,
            'monto_total': 0.0,
            'precio_promedio': 0.0
        })
        
        # Obtener todas las reservas
        reservas_keys = r.keys("reserva:*")
        for reserva_key in reservas_keys:
            reserva_data = r.hgetall(reserva_key)
            
            # Verificar si la reserva está dentro del período
            fecha_creacion = datetime.strptime(
                reserva_data[b'fecha_creacion'].decode('utf-8'), 
                '%Y-%m-%d %H:%M:%S'
            )
            
            if fecha_creacion >= fecha_inicio:
                ciudad = reserva_data[b'ciudad'].decode('utf-8')
                monto = float(reserva_data[b'monto'].decode('utf-8'))
                
                reservas_por_ciudad[ciudad]['total_reservas'] += 1
                reservas_por_ciudad[ciudad]['monto_total'] += monto
        
        # 2. Análisis de propiedades en MongoDB
        pipeline = [
            {
                "$group": {
                    "_id": "$ubicacion.ciudad",
                    "total_propiedades": {"$sum": 1},
                    "calificacion_promedio": {"$avg": "$calificacion"},
                    "total_reseñas": {"$sum": "$resenias_count"}
                }
            }
        ]
        
        resultados_mongo = db["propiedades"].aggregate(pipeline)
        
        # 3. Combinar resultados
        analytics = []
        for ciudad_data in resultados_mongo:
            ciudad = ciudad_data['_id']
            reservas_data = reservas_por_ciudad.get(ciudad, {
                'total_reservas': 0,
                'monto_total': 0.0
            })
            
            if reservas_data['total_reservas'] > 0:
                precio_promedio = reservas_data['monto_total'] / reservas_data['total_reservas']
            else:
                precio_promedio = 0
                
            analytics.append({
                'ciudad': ciudad,
                'total_propiedades': ciudad_data['total_propiedades'],
                'total_reservas': reservas_data['total_reservas'],
                'precio_promedio': round(precio_promedio, 2),
                'calificacion_promedio': round(ciudad_data['calificacion_promedio'], 2),
                'total_reseñas': ciudad_data['total_reseñas']
            })
        
        # Ordenar por total de reservas (demanda) descendente
        analytics.sort(key=lambda x: x['total_reservas'], reverse=True)
        
        # Imprimir resultados
        print(f"\nAnálisis de áreas más demandadas (últimos {periodo_dias} días):")
        headers = ["Ciudad", "Total Props", "Total Reservas", "Precio Prom", "Calif Prom", "Total Reseñas"]
        rows = [
            [
                a['ciudad'],
                a['total_propiedades'],
                a['total_reservas'],
                f"${a['precio_promedio']:,.2f}",
                f"{a['calificacion_promedio']}/5.0",
                a['total_reseñas']
            ] 
            for a in analytics
        ]
        
        print(tabulate(rows, headers=headers, tablefmt="grid"))
        
        # Identificar las 3 ciudades más demandadas
        top_3 = analytics[:3]
        print("\nTop 3 áreas más demandadas:")
        for i, area in enumerate(top_3, 1):
            print(f"{i}. {area['ciudad']}")
            print(f"   - {area['total_reservas']} reservas en el período")
            print(f"   - Precio promedio: ${area['precio_promedio']:,.2f}")
            print(f"   - Calificación promedio: {area['calificacion_promedio']}/5.0")
        
        return analytics
        
    except Exception as e:
        print(f"Error durante el análisis: {str(e)}")
        return []

def analizar_tipos_alojamiento_populares(periodo_dias=30):

    try:
        fecha_inicio = datetime.now() - timedelta(days=periodo_dias)
        stats_por_tipo = defaultdict(lambda: {
            'total_reservas': 0,
            'ingresos_totales': 0.0,
            'precio_promedio_noche': 0.0,
            'total_propiedades': 0,
            'calificacion_promedio': 0.0,
            'total_reseñas': 0,
            'duracion_promedio_estadia': 0.0,
            'ciudades_populares': Counter()
        })

        # 1. Analizar reservas de Redis
        reservas_keys = r.keys("reserva:*")
        propiedades_con_reservas = set()
        
        for reserva_key in reservas_keys:
            reserva_data = r.hgetall(reserva_key)
            
            # Verificar si la reserva está dentro del período
            fecha_creacion = datetime.strptime(
                reserva_data[b'fecha_creacion'].decode('utf-8'), 
                '%Y-%m-%d %H:%M:%S'
            )
            
            if fecha_creacion >= fecha_inicio:
                propiedad_id = reserva_data[b'propiedad_id'].decode('utf-8')
                monto = float(reserva_data[b'monto'].decode('utf-8'))
                ciudad = reserva_data[b'ciudad'].decode('utf-8')
                
                # Obtener los datos de la propiedad de MongoDB
                propiedad = get_document_by_id(propiedad_id)
                if propiedad and 'tipo' in propiedad:
                    tipo = propiedad['tipo']
                    propiedades_con_reservas.add(propiedad_id)
                    
                    # Calcular duración de la estadia
                    fecha_inicio_reserva = datetime.strptime(
                        reserva_data[b'fecha_inicio'].decode('utf-8'), 
                        '%Y-%m-%d'
                    )
                    fecha_fin_reserva = datetime.strptime(
                        reserva_data[b'fecha_fin'].decode('utf-8'), 
                        '%Y-%m-%d'
                    )
                    duracion = (fecha_fin_reserva - fecha_inicio_reserva).days
                    
                    # Actualizar estadísticas
                    stats = stats_por_tipo[tipo]
                    stats['total_reservas'] += 1
                    stats['ingresos_totales'] += monto
                    stats['duracion_promedio_estadia'] = (
                        (stats['duracion_promedio_estadia'] * (stats['total_reservas'] - 1) + duracion) 
                        / stats['total_reservas']
                    )
                    stats['ciudades_populares'][ciudad] += 1

        # 2. Obtener estadísticas adicionales de MongoDB
        pipeline = [
            {
                "$group": {
                    "_id": "$tipo",
                    "total_propiedades": {"$sum": 1},
                    "calificacion_promedio": {"$avg": "$calificacion"},
                    "total_reseñas": {"$sum": "$resenias_count"},
                    "precio_promedio": {"$avg": "$precio"}
                }
            }
        ]
        
        resultados_mongo = db["propiedades"].aggregate(pipeline)
        
        # Combinar con estadísticas de MongoDB
        for resultado in resultados_mongo:
            tipo = resultado['_id']
            if tipo in stats_por_tipo:
                stats_por_tipo[tipo].update({
                    'total_propiedades': resultado['total_propiedades'],
                    'calificacion_promedio': resultado['calificacion_promedio'],
                    'total_reseñas': resultado['total_reseñas'],
                    'precio_promedio_noche': resultado['precio_promedio']
                })
        
        # Convertir a lista y ordenar por total de reservas
        analytics = [
            {
                'tipo': tipo,
                'estadisticas': {
                    **stats,
                    'tasa_ocupacion': (stats['total_reservas'] / stats['total_propiedades'] * 100) 
                        if stats['total_propiedades'] > 0 else 0,
                    'ciudades_mas_populares': [
                        ciudad for ciudad, _ in stats['ciudades_populares'].most_common(3)
                    ]
                }
            }
            for tipo, stats in stats_por_tipo.items()
        ]
        analytics.sort(key=lambda x: x['estadisticas']['total_reservas'], reverse=True)
        
        # Imprimir resultados
        print(f"\nAnálisis de Tipos de Alojamiento (últimos {periodo_dias} días):")
        print("-" * 80)
        
        headers = [
            "Tipo", "Reservas", "Propiedades", "Tasa Ocup.", 
            "Calif.", "Precio Prom.", "Estadia Prom."
        ]
        rows = [
            [
                a['tipo'],
                a['estadisticas']['total_reservas'],
                a['estadisticas']['total_propiedades'],
                f"{a['estadisticas']['tasa_ocupacion']:.1f}%",
                f"{a['estadisticas']['calificacion_promedio']:.1f}/5.0",
                f"${a['estadisticas']['precio_promedio_noche']:,.2f}",
                f"{a['estadisticas']['duracion_promedio_estadia']:.1f} días"
            ]
            for a in analytics
        ]
        print(tabulate(rows, headers=headers, tablefmt="grid"))
        
        # Mostrar detalles adicionales del top 3
        print("\nTop 3 Tipos de Alojamiento más Populares:")
        for i, analytic in enumerate(analytics[:3], 1):
            stats = analytic['estadisticas']
            print(f"\n{i}. {analytic['tipo']}")
            print(f"   - Total de reservas: {stats['total_reservas']}")

        
        return analytics
        
    except Exception as e:
        print(f"Error durante el análisis: {str(e)}")
        return []

if __name__ == "__main__":
    ver_reservas_y_pagos()
    consultar_tipo_de_alojamiento_popular()
    analizar_areas_demandadas()
    analizar_tipos_alojamiento_populares()
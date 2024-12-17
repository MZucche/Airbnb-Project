from mongo_db import crear_reseña, obtener_tipos_alojamiento_populares_resenias
from redis_db import *
from sqlite_db import *
from mongo_db import db, PropiedadModel, existe_propiedad, ver_propiedades_ultima_semana, ver_propiedades_premium_caba
from sqlite_db import existe_propietario, existe_huesped


def menu():
    print()
    print("Selecciona una operación:")
    print()
    print("1. Agregar huesped SQLite")
    print("2. Agregar propietario SQLite")
    print("3. Agregar propiedad MongoDB")
    print("4. Agregar reserva Redis")
    print("5. Agregar pago Redis")
    print("6. Agregar reseña MongoDB\n")
    print("7. Consultas\n")
    print("8. Salir\n")

def agregar_datos(opcion):
    if opcion == "1":
        dni = int(input("Ingrese el dni del huesped: "))
        nombre = input("Nombre: ")
        apellido = input("Apellido: ")
        telefono = input("Teléfono: ")
        insertar_huesped(dni, nombre, apellido, telefono)

    elif opcion == "2":
        dni = int(input("Ingrese el DNI del propietario: "))
        nombre = input("Nombre: ")
        apellido = input("Apellido: ")
        telefono = input("Teléfono: ")
        email = input("Email: ")
        calificacion = float(input("Calificación: "))
        insertar_propietario(dni, nombre, apellido, telefono, email, calificacion)

    elif opcion == "3":
        try:
            propietario_id = input("\nIngrese el ID del propietario: ")
            if not existe_propietario(propietario_id):
                print("no existe propietario")
                menu()
            ubicacion = {
                "ciudad": input("Ciudad: "),
                "direccion": input("Dirección: "),
                "codigo_postal": input("Código postal: ")
            }
            tipo = input("Tipo de propiedad: ")
            descripcion = input("Descripción: ")
            precio = float(input("Precio por noche: "))
            servicios = input("Servicios (separados por coma): ").split(",")

            propiedad = PropiedadModel(
                propietario_id=propietario_id,
                ubicacion=ubicacion,
                tipo=tipo,
                descripcion=descripcion,
                precio=precio,
                servicios=[s.strip() for s in servicios if s.strip()]
            )

            result = db.propiedades.insert_one(propiedad.dict(by_alias=True))
            print(f"Propiedad creada con ID: {result.inserted_id}")
        except Exception as e:
            print(f"Error al crear propiedad: {str(e)}")

    elif opcion == "4":
        while True:
            usuario_id = input("DNI del Usuario: ")
            if not validar_dni(usuario_id):
                print("DNI inválido. Debe ser un número de 7 o 8 dígitos.")
                continue
            break

        propiedad_id = input("ID Propiedad: ")

        while True:
            fecha_inicio = input("Fecha inicio (YYYY-MM-DD): ")
            #if not validar_fecha(fecha_inicio):
            #    print("Fecha de inicio inválida. Intenta de nuevo.")
            #    continue
            break

        while True:
            fecha_fin = input("Fecha fin (YYYY-MM-DD): ")
            #if not validar_fecha(fecha_fin):
            #    print("Fecha de fin inválida. Intenta de nuevo.")
            #    continue
            if fecha_inicio >= fecha_fin:
                print("La fecha de fin debe ser posterior a la fecha de inicio. Intenta de nuevo.")
                continue
            break

        while True:
            monto = input("Monto Total: ")
            if not validar_monto(monto):
                print("Monto inválido. Debe ser un número positivo. Intenta de nuevo.")
                continue
            break

        while True:
            metodo_pago = input("Método de pago (Tarjeta, Efectivo, Transferencia): ")
            if not validar_metodo_pago(metodo_pago):
                print("Método de pago inválido. Debe ser uno de los siguientes: Tarjeta, Efectivo, Transferencia. Intenta de nuevo.")
                continue
            break

        while True:
            estado_reserva = input("Estado de la reserva (Confirmada, Pendiente, Cancelada): ")
            if not validar_estado_reserva(estado_reserva):
                print("Estado de la reserva inválido. Debe ser Confirmada, Pendiente o Cancelada. Intenta de nuevo.")
                continue
            break

        while True:
            ciudad = input("Ciudad: ")
            break

        reserva_id = crear_reserva(usuario_id, propiedad_id, fecha_inicio, fecha_fin, float(monto), metodo_pago, estado_reserva, ciudad, imprimir_tabla=False)

        if reserva_id:
            print("Reserva agregada exitosamente.")


    elif opcion == "5":
        reserva_id = input("ID Reserva: ")

        while True:
            estado_pago = input("Estado del pago (Pagado, Pendiente, Cancelado): ")
            if not validar_estado_pago(estado_pago):
                print("Estado de pago inválido. Debe ser Pagado, Pendiente o Cancelado. Intenta de nuevo.")
                continue
            break

        while True:
            monto = input("Monto: ")
            if not validar_monto(monto):
                print("Monto inválido. Debe ser un número positivo. Intenta de nuevo.")
                continue
            break

        while True:
            metodo_pago = input("Método de pago (Tarjeta, Efectivo, Transferencia): ")
            if not validar_metodo_pago(metodo_pago):
                print("Método de pago inválido. Debe ser uno de los siguientes: Tarjeta, Efectivo, Transferencia. Intenta de nuevo.")
                continue
            break

        pago_id = agregar_pago(reserva_id, estado_pago, float(monto), metodo_pago, imprimir_tabla=False)
        if pago_id:
            print("Pago agregado exitosamente.")

    elif opcion == "6":
        crear_reseña(
            reserva_id = input("ID de la Reserva: ") ,
            propietario_id = input("DNI del Propietario: ") ,
            huesped_id = input("DNI del Huesped: ") ,
            propiedad_id = input("ID de la Propiedad: ") ,
            calificacion_propiedad = input("Calificacion de la Propiedad: ") ,
            calificacion_propietario = input("Calificacion del Propietario: ") ,
            comentario = input("Comentario: ")
        )

    elif opcion == "7":
        print("\n=== Consultas disponibles ===")
        print("1. ¿Cuántas reservas se realizan en una ciudad específica en el último mes? Redis")
        print("2. ¿Cuántas propiedades han sido agregadas recientemente en la plataforma? Mongo")
        print("3. ¿Qué anfitriones tienen las mejores calificaciones? SQL")
        print("4. ¿Cuáles son las áreas más demandadas para alquileres en un país? Mongo + Redis")
        print("5. ¿Cuántas propiedades tienen una calificación mayor a 4.5 Y están ubicadas en el centro de la ciudad? Mongo")
        print("6. ¿Qué tipos de alojamientos han recibido más de 20 reseñas O están en una zona turística popular? Mongo + Redis")

        opcion = input("\nSeleccione una consulta: ")

        if opcion == "1":
            reservas_por_ciudad_ultimo_mes()

        #elif opcion == "2":
        #    analizar_tipos_alojamiento_populares()

        elif opcion == "2":
            ver_propiedades_ultima_semana()

        elif opcion == "3":
            anfitrionesMejoresCalificaciones()

        elif opcion == "4":
            analizar_areas_demandadas()

        elif opcion == "5":
            ver_propiedades_premium_caba()

        elif opcion == "6":
            obtener_tipos_alojamiento_populares_resenias()

    elif opcion == "8":
        print("Saliendo del programa.")
        return False

    return True

def main():
    create_tables()

    while True:
        menu()
        opcion = input("Selecciona una opción: ")


        if not agregar_datos(opcion):
            break

if __name__ == "__main__":
    main()
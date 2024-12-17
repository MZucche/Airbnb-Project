import sqlite3
from tabulate import tabulate

def create_connection():
    connection = sqlite3.connect("airbnb.db")
    return connection

def create_tables():
    connection = create_connection()
    cursor = connection.cursor()

    tables = {
        "huesped": """
        CREATE TABLE IF NOT EXISTS huesped (
            dni INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL,
            apellido TEXT,
            telefono INTEGER
        );
    """,

        "propietario": """
        CREATE TABLE IF NOT EXISTS propietario (
            dni INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL,
            apellido TEXT,
            telefono INTEGER,
            email TEXT,
            calificacion REAL
        );
    """}

    for table_name, table_sql in tables.items():
        cursor.execute(table_sql)
        print(f"Tabla {table_name} creada correctamente.")

    # Guardar y cerrar la conexión
    connection.commit()
    connection.close()

def mostrar_info_completa_tablas():
    conn = create_connection()
    cursor = conn.cursor()
    
    # Obtener todas las tablas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()
    
    for tabla in tablas:
        nombre_tabla = tabla[0]
        
        # Obtener nombres de las columnas
        cursor.execute(f"PRAGMA table_info({nombre_tabla})")
        columnas = [tupla[1] for tupla in cursor.fetchall()]
        
        # Obtener todos los registros de la tabla
        cursor.execute(f"SELECT * FROM {nombre_tabla}")
        registros = cursor.fetchall()
        
        # Mostrar información de la tabla
        print(f"\n{'='*50}")
        print(f"TABLA: {nombre_tabla.upper()}")
        print(f"{'='*50}")
        
        if not registros:
            print("No hay registros en esta tabla")
        else:
            print(tabulate(registros, 
                         headers=columnas, 
                         tablefmt='grid', 
                         numalign="center"))
            print(f"\nTotal de registros: {len(registros)}")
        
        print(f"{'='*50}\n")
    
    conn.close()

def mostrar_info_tablas():
    conn = create_connection()
    cursor = conn.cursor()
    
    print("\n=== INFORMACIÓN DE LA BASE DE DATOS ===")
    
    # Obtener todas las tablas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()
    
    if not tablas:
        print("No hay tablas en la base de datos")
        return
    
    for tabla in tablas:
        nombre_tabla = tabla[0]
        
        # Mostrar nombre de la tabla
        print(f"\n📋 TABLA: {nombre_tabla.upper()}")
        
        # Obtener información de las columnas
        cursor.execute(f"PRAGMA table_info({nombre_tabla})")
        columnas = cursor.fetchall()
        
        # Preparar la información para tabulate
        info_columnas = []
        for col in columnas:
            info_columnas.append([
                col[1],                                  # Nombre columna
                col[2],                                  # Tipo de dato
                "✓" if col[5] == 1 else "",             # Es Primary Key
                "✓" if col[3] == 1 else "",             # Es Not Null
                f"PK{col[5]}" if col[5] > 0 else ""     # Índice de PK si existe
            ])
        
        # Mostrar estructura
        print(tabulate(info_columnas,
                      headers=['Columna', 'Tipo', 'PK', 'Not Null', 'Index'],
                      tablefmt='grid'))
        
        # Mostrar número de registros en la tabla
        cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla}")
        num_registros = cursor.fetchone()[0]
        print(f"📊 Número de registros: {num_registros}")
        
    conn.close()

def insertar_huesped(dni, nombre, apellido, telefono):
    conn = create_connection()
    cursor = conn.cursor()

    # Verificar si el id_huesped ya existe en la tabla
    cursor.execute("SELECT 1 FROM huesped WHERE dni = ?", (dni,))
    result = cursor.fetchone()

    if result:
        print(f"El DNI {dni} ya existe en la base de datos. No se puede duplicar.")
    else:
        cursor.execute("INSERT INTO huesped (dni, nombre, apellido, telefono) VALUES (?, ?, ?, ?)",
                       (dni, nombre, apellido, telefono))
        conn.commit()
        print("Huésped registrado con éxito.")

    conn.close()

def insertar_propietario(dni, nombre, apellido, telefono, email, calificacion):
    conn = create_connection()
    cursor = conn.cursor()

    # Verificar si el id_propietario ya existe
    cursor.execute("SELECT 1 FROM propietario WHERE dni = ?", (dni,))
    if cursor.fetchone() is not None:
        print(f"Error: Ya existe un propietario con el ID {dni}.")
    else:
        # Insertar propietario si el id_propietario no existe
        cursor.execute(
            "INSERT INTO propietario (dni, nombre, apellido, telefono, email, calificacion) VALUES (?, ?, ?, ?, ?, ?)",
            (dni, nombre, apellido, telefono, email, calificacion)
        )
        conn.commit()
        print("Propietario insertado correctamente.")

    conn.close()


def ver_huespedes():
    conn = sqlite3.connect("airbnb.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM huesped")
    huespedes = cursor.fetchall()

    for huesped in huespedes:
        print(huesped)

    conn.close()

def ver_propietarios():
    conn = sqlite3.connect("airbnb.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM propietario")
    huespedes = cursor.fetchall()

    for huesped in huespedes:
        print(huesped)


def anfitrionesMejoresCalificaciones():
    conn = sqlite3.connect("airbnb.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM propietario WHERE calificacion > 3")
    propietarios = cursor.fetchall()

    for propietario in propietarios:
        print(propietario)


def existe_propietario(dni) -> bool:
    conn = create_connection()
    cursor = conn.cursor()

    # Buscar si existe un propietario con el DNI dado
    cursor.execute("SELECT dni FROM propietario WHERE dni = ?", (dni,))
    dni = cursor.fetchone()

    if dni:
        return True
    else:
        return False

    conn.close()

def existe_huesped(dni) -> bool:
    conn = create_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT dni FROM huesped WHERE dni = ?", (dni,))
    dni = cursor.fetchone()

    if dni:
        return True
    else:
        return False

    conn.close()

def calificar_propietario(dni_propietario, calificacion):
    conn = sqlite3.connect("airbnb.db")
    cursor = conn.cursor()

    try:
        # Verificamos si existe el propietario con ese DNI
        cursor.execute("""
            SELECT dni, calificacion 
            FROM propietario 
            WHERE dni = ?
        """, (dni_propietario,))

        resultado = cursor.fetchone()

        if resultado is None:
            print(f"No se encontró ningún propietario con DNI {dni_propietario}")
            return False

        # Actualizamos la calificación
        cursor.execute("""
            UPDATE propietario 
            SET calificacion = ?
            WHERE dni = ?
        """, (calificacion, dni_propietario))

        conn.commit()
        print(f"Calificación actualizada exitosamente para el propietario con DNI {dni_propietario}")
        return True

    except sqlite3.Error as e:
        print(f"Error al actualizar la calificación: {e}")
        return False

    finally:
        conn.close()

def anfitrionesMejoresCalificaciones():
    conn = sqlite3.connect("airbnb.db")
    cursor = conn.cursor()
    print("\nLos anfitriones con las mejores calificaciones son:")
    cursor.execute("""
        SELECT * FROM propietario 
        ORDER BY calificacion DESC 
        LIMIT 5
    """)
    propietarios = cursor.fetchall()
    print("-" * 50)

    for propietario in propietarios:
        print(f"DNI: {propietario[0]}")
        print(f"Nombre: {propietario[1]}")
        print(f"Apellido: {propietario[2]}")
        print(f"Teléfono: {propietario[3]}")
        print(f"Email: {propietario[4]}")
        print(f"Calificación: {propietario[5]}")
        print("-" * 50)

if __name__ == "__main__":
    #create_tables()  # Primero creamos las tablas
    mostrar_info_completa_tablas()  # Luego mostramos su información


import io
import pymysql
from flask import Flask, render_template, request, jsonify, Response

app = Flask(__name__)

# Función auxiliar para obtener conexión a la base de datos
def obtener_conexion(config):
    return pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# Función para obtener la estructura actual de una base de datos
def obtener_estructura_db(config):
    conn = obtener_conexion(config)
    estructuras = {
        'tablas': {},
        'triggers': {}
    }
    try:
        with conn.cursor() as cursor:
            # Obtener tablas
            cursor.execute("SHOW TABLES")
            tablas_raw = cursor.fetchall()
            key_name = f"Tables_in_{config['database']}"
            
            for t in tablas_raw:
                tabla_nombre = t[key_name]
                # Obtener columnas de cada tabla
                cursor.execute(f"DESCRIBE `{tabla_nombre}`")
                columnas = cursor.fetchall()
                estructuras['tablas'][tabla_nombre] = {col['Field']: col for col in columnas}
                
            # Obtener triggers
            cursor.execute("""
                SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, 
                       ACTION_STATEMENT, ACTION_ORIENTATION, ACTION_TIMING
                FROM INFORMATION_SCHEMA.TRIGGERS 
                WHERE TRIGGER_SCHEMA = %s
            """, (config['database'],))
            triggers = cursor.fetchall()
            for trig in triggers:
                estructuras['triggers'][trig['TRIGGER_NAME']] = trig
    finally:
        conn.close()
    return estructuras

# Compara esquemas y calcula diferencias
def calcular_diferencias(origen_cfg, destino_cfg):
    origen = obtener_estructura_db(origen_cfg)
    destino = obtener_estructura_db(destino_cfg)
    
    comparativa = {
        'tablas_nuevas': [],
        'columnas_nuevas': [],
        'triggers_nuevos_o_modificados': []
    }
    
    # 1. Comparar Tablas
    for tabla_nombre, columnas in origen['tablas'].items():
        if tabla_nombre not in destino['tablas']:
            comparativa['tablas_nuevas'].append(tabla_nombre)
        else:
            # Comparar Columnas de tablas existentes
            for col_nombre, col_meta in columnas.items():
                if col_nombre not in destino['tablas'][tabla_nombre]:
                    comparativa['columnas_nuevas'].append({
                        'tabla': tabla_nombre,
                        'columna': col_nombre,
                        'definicion': col_meta
                    })
                    
    # 2. Comparar Triggers
    for trig_nombre, trig_meta in origen['triggers'].items():
        if trig_nombre not in destino['triggers']:
            comparativa['triggers_nuevos_o_modificados'].append({
                'nombre': trig_nombre,
                'accion': 'Crear',
                'meta': trig_meta
            })
        else:
            # Verificar si la definición cambió
            trig_destino = destino['triggers'][trig_nombre]
            if trig_meta['ACTION_STATEMENT'] != trig_destino['ACTION_STATEMENT']:
                comparativa['triggers_nuevos_o_modificados'].append({
                    'nombre': trig_nombre,
                    'accion': 'Actualizar',
                    'meta': trig_meta
                })
                
    return comparativa

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analizar', methods=['POST'])
def analizar():
    try:
        data = request.json
        origen_cfg = data['origen']
        destino_cfg = data['destino']
        
        diferencias = calcular_diferencias(origen_cfg, destino_cfg)
        return jsonify({'status': 'success', 'diferencias': diferencias})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/respaldar', methods=['POST'])
def respaldar():
    try:
        data = request.json
        destino_cfg = data['destino']
        
        conn = obtener_conexion(destino_cfg)
        sql_backup = io.StringIO()
        sql_backup.write(f"-- Respaldo generado automáticamente de la Base de Datos: {destino_cfg['database']}\n")
        sql_backup.write("SET FOREIGN_KEY_CHECKS=0;\n\n")
        
        with conn.cursor() as cursor:
            # Obtener tablas
            cursor.execute("SHOW TABLES")
            tablas_raw = cursor.fetchall()
            key_name = f"Tables_in_{destino_cfg['database']}"
            
            for t in tablas_raw:
                tabla_nombre = t[key_name]
                # Estructura de la tabla
                cursor.execute(f"SHOW CREATE TABLE `{tabla_nombre}`")
                create_stmt = cursor.fetchone()
                sql_backup.write(f"-- Estructura de tabla para {tabla_nombre}\n")
                sql_backup.write(f"DROP TABLE IF EXISTS `{tabla_nombre}`;\n")
                sql_backup.write(f"{create_stmt['Create Table']};\n\n")
                
                # Datos de la tabla
                cursor.execute(f"SELECT * FROM `{tabla_nombre}`")
                filas = cursor.fetchall()
                if filas:
                    sql_backup.write(f"-- Datos de la tabla {tabla_nombre}\n")
                    for fila in filas:
                        columnas = ", ".join([f"`{k}`" for k in fila.keys()])
                        valores = []
                        for val in fila.values():
                            if val is None:
                                valores.append("NULL")
                            elif isinstance(val, (int, float)):
                                valores.append(str(val))
                            else:
                                valores.append(f"'{conn.escape_string(str(val))}'")
                        valores_str = ", ".join(valores)
                        sql_backup.write(f"INSERT INTO `{tabla_nombre}` ({columnas}) VALUES ({valores_str});\n")
                    sql_backup.write("\n")
            
            # Triggers
            cursor.execute("""
                SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT, ACTION_TIMING 
                FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = %s
            """, (destino_cfg['database'],))
            triggers = cursor.fetchall()
            for trig in triggers:
                sql_backup.write(f"-- Trigger {trig['TRIGGER_NAME']}\n")
                sql_backup.write(f"DROP TRIGGER IF EXISTS `{trig['TRIGGER_NAME']}`;\n")
                sql_backup.write(f"CREATE TRIGGER `{trig['TRIGGER_NAME']}` {trig['ACTION_TIMING']} {trig['EVENT_MANIPULATION']} ON `{trig['EVENT_OBJECT_TABLE']}` FOR EACH ROW {trig['ACTION_STATEMENT']};\n\n")
                
        sql_backup.write("SET FOREIGN_KEY_CHECKS=1;\n")
        conn.close()
        
        return Response(
            sql_backup.getvalue(),
            mimetype="text/plain",
            headers={"Content-disposition": f"attachment; filename=backup_{destino_cfg['database']}.sql"}
        )
    except Exception as e:
        return jsonify({'status': 'error', 'message': f"Error al generar backup: {str(e)}"}), 500

@app.route('/migrar', methods=['POST'])
def migrar():
    try:
        data = request.json
        origen_cfg = data['origen']
        destino_cfg = data['destino']
        
        # Calcular de nuevo diferencias por seguridad
        diffs = calcular_diferencias(origen_cfg, destino_cfg)
        
        conn_origen = obtener_conexion(origen_cfg)
        conn_destino = obtener_conexion(destino_cfg)
        
        pasos_ejecutados = []
        
        with conn_origen.cursor() as cur_orig, conn_destino.cursor() as cur_dest:
            cur_dest.execute("SET FOREIGN_KEY_CHECKS=0;")
            
            # 1. Crear tablas completamente nuevas
            for tabla in diffs['tablas_nuevas']:
                cur_orig.execute(f"SHOW CREATE TABLE `{tabla}`")
                create_stmt = cur_orig.fetchone()['Create Table']
                cur_dest.execute(create_stmt)
                pasos_ejecutados.append(f"Creada tabla nueva: {tabla}")
                
            # 2. Agregar columnas nuevas a tablas existentes
            for col_info in diffs['columnas_nuevas']:
                tabla = col_info['tabla']
                col_nombre = col_info['columna']
                meta = col_info['definicion']
                
                # Reconstruir tipo de datos y restricciones básicas de columna
                tipo = meta['Type']
                nulo = "NULL" if meta['Null'] == "YES" else "NOT NULL"
                defecto = ""
                if meta['Default'] is not None:
                    defecto = f"DEFAULT '{meta['Default']}'"
                
                alter_stmt = f"ALTER TABLE `{tabla}` ADD COLUMN `{col_nombre}` {tipo} {nulo} {defecto}"
                cur_dest.execute(alter_stmt)
                pasos_ejecutados.append(f"Añadida columna `{col_nombre}` a la tabla `{tabla}`")
                
            # 3. Triggers nuevos o modificados
            for trig_info in diffs['triggers_nuevos_o_modificados']:
                nombre = trig_info['nombre']
                meta = trig_info['meta']
                
                # Eliminar si ya existe antes de crearlo
                cur_dest.execute(f"DROP TRIGGER IF EXISTS `{nombre}`")
                
                # Crear trigger
                create_trig = f"""
                CREATE TRIGGER `{nombre}` {meta['ACTION_TIMING']} {meta['EVENT_MANIPULATION']} 
                ON `{meta['EVENT_OBJECT_TABLE']}` 
                FOR EACH ROW 
                {meta['ACTION_STATEMENT']}
                """
                cur_dest.execute(create_trig)
                pasos_ejecutados.append(f"Procesado Trigger: {nombre} ({trig_info['accion']})")
                
            cur_dest.execute("SET FOREIGN_KEY_CHECKS=1;")
            conn_destino.commit()
            
        conn_origen.close()
        conn_destino.close()
        
        return jsonify({'status': 'success', 'pasos': pasos_ejecutados})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f"Error durante la migración: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True)
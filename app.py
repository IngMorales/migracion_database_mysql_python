"""
Migrador y Sincronizador MySQL
==============================
Herramienta web (Flask) para comparar dos bases de datos MySQL y ejecutar:

  1. Análisis estructural  : tablas, columnas y triggers faltantes en destino.
  2. Análisis de datos     : conteo de filas por tabla y detección de registros
                             nuevos (por clave primaria) pendientes de migrar.
  3. Respaldo (backup)     : volcado SQL completo de la base de datos destino.
  4. Sincronización        : aplica los cambios estructurales al destino.
  5. Migración de datos    : inserta en destino SOLO los registros cuyo id/PK
                             no exista aún (los ids repetidos se omiten).

Convención:
  - ORIGEN  -> base de datos FUENTE (de donde se leen los datos).
  - DESTINO -> base de datos AFECTADA (donde se escriben los cambios).

Progreso en tiempo real:
  Las rutas /analizar, /migrar y /migrar-datos devuelven un stream NDJSON
  (una línea JSON por evento: 'progreso', 'paso', 'tabla_resultado',
  'resultado' o 'error'). El frontend lo consume con fetch + ReadableStream
  para alimentar un modal (SweetAlert2) con el paso actual y una barra de
  avance, sin esperar a que toda la operación termine.
"""

import io
import json
import re
import pymysql
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

app = Flask(__name__)

# Cantidad de filas leídas/insertadas por lote durante la migración de datos.
TAMANO_LOTE = 500


class ErrorConexionMySQL(Exception):
    """Error de conexión listo para mostrarse al usuario, sin traza interna."""


def evento(tipo, **kwargs):
    """Serializa una línea de evento NDJSON para las rutas en streaming."""
    return json.dumps({'tipo': tipo, **kwargs}, default=str) + "\n"


def detalle_error(e):
    """Registra el detalle técnico y devuelve solamente un mensaje seguro."""
    app.logger.exception("Error durante una operación del migrador")
    return {
        'tipo_error': type(e).__name__,
        'mensaje_error': str(e),
    }


# ---------------------------------------------------------------------------
# Conexión y utilidades de introspección
# ---------------------------------------------------------------------------

def obtener_conexion(config, nombre='base de datos'):
    """
    Abre una conexión MySQL a partir del diccionario de configuración.

    Seguridad: primero intenta una conexión cifrada (SSL/TLS). Si el servidor
    no soporta SSL (frecuente en hosting compartido, p. ej. MariaDB de cPanel),
    reintenta sin cifrado para no bloquear la operación.
    """
    try:
        puerto = int(config.get('port', 3306))
        if not 1 <= puerto <= 65535:
            raise ValueError
    except (TypeError, ValueError):
        raise ErrorConexionMySQL(
            f"El puerto de {nombre} debe ser un número entre 1 y 65535."
        ) from None

    parametros = dict(
        host=config['host'],
        port=puerto,
        user=config['user'],
        password=config['password'],
        database=config['database'],
        charset='utf8mb4',
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        return pymysql.connect(**parametros, ssl={'ca': None})
    except pymysql.err.OperationalError as e:
        # 2026 = "SSL is required but the server doesn't support it"
        if e.args and e.args[0] == 2026:
            try:
                return pymysql.connect(**parametros)
            except pymysql.err.OperationalError as segundo_error:
                e = segundo_error

        codigo = e.args[0] if e.args else None
        servidor = f"{config['host']}:{puerto}"
        # MySQL informa la IP con la que nos ve: "...'usuario'@'IP'..." (1045)
        # o "Host 'IP' is not allowed to connect..." (1130).
        texto = str(e.args[1]) if len(e.args) > 1 else ''
        ip = re.search(r"@'([^']+)'|Host '([^']+)'", texto)
        ip = (ip.group(1) or ip.group(2)) if ip else None
        aviso_ip = (
            f" Si el usuario y la contraseña son correctos, agregue la IP {ip} "
            "en cPanel → Remote MySQL (Acceso remoto a MySQL)."
        ) if ip else ''
        if codigo == 1130:
            mensaje = (
                f"El servidor de {nombre} no permite conexiones desde la IP "
                f"{ip or 'de esta computadora'}. Agréguela en cPanel → Remote MySQL "
                "(Acceso remoto a MySQL)."
            )
        elif codigo in (2002, 2003, 2005):
            mensaje = (
                f"No fue posible conectar con {nombre} en {servidor}. "
                "Verifique el host y el puerto, que MySQL esté activo y que el "
                "firewall o el acceso remoto de MySQL permita esta computadora."
            )
        elif codigo == 1045:
            mensaje = (
                f"El usuario o la contraseña de {nombre} no son correctos."
                + aviso_ip
            )
        elif codigo == 1049:
            mensaje = (
                f"La base de datos '{config['database']}' no existe en {nombre}."
            )
        else:
            mensaje = f"MySQL rechazó la conexión con {nombre} (código {codigo})."
        raise ErrorConexionMySQL(mensaje) from None


def listar_tablas(cursor, database):
    """Devuelve la lista de nombres de tablas de la base de datos conectada."""
    cursor.execute("SHOW TABLES")
    clave = f"Tables_in_{database}"
    return [fila[clave] for fila in cursor.fetchall()]


def listar_tablas_y_vistas(cursor, database):
    """
    Devuelve (tablas, vistas) por separado. Es clave distinguirlas: una VIEW
    responde a SHOW CREATE TABLE con la columna 'Create View' en lugar de
    'Create Table', y sus datos no deben respaldarse ni migrarse.
    """
    cursor.execute("SHOW FULL TABLES")
    clave = f"Tables_in_{database}"
    tablas, vistas = [], []
    for fila in cursor.fetchall():
        if fila.get('Table_type') == 'VIEW':
            vistas.append(fila[clave])
        else:
            tablas.append(fila[clave])
    return tablas, vistas


def quitar_collation(sql):
    """
    Elimina toda cláusula COLLATE (a nivel de tabla y de columna) de una
    sentencia CREATE. Necesario al migrar entre motores distintos: por
    ejemplo, MySQL 8 usa 'utf8mb4_0900_ai_ci', que MariaDB no reconoce
    (error 1273 "Unknown collation"). Sin la cláusula, el servidor destino
    aplica su collation por defecto para el charset indicado.
    """
    return re.sub(r"\s*COLLATE[=\s]+[A-Za-z0-9_]+", "", sql, flags=re.IGNORECASE)


def quitar_definer(sql):
    """
    Elimina la cláusula DEFINER=`usuario`@`host` de un CREATE VIEW/TRIGGER.
    Sin esto, el backup solo podría restaurarse con el mismo usuario MySQL
    del servidor original.
    """
    return re.sub(r"DEFINER\s*=\s*`[^`]*`@`[^`]*`\s*", "", sql, flags=re.IGNORECASE)


def obtener_create(cursor, nombre):
    """
    Devuelve ('tabla' | 'vista', sentencia CREATE) para un objeto, aceptando
    tanto tablas base como vistas.
    """
    cursor.execute(f"SHOW CREATE TABLE `{nombre}`")
    fila = cursor.fetchone()
    if 'Create Table' in fila:
        return 'tabla', fila['Create Table']
    if 'Create View' in fila:
        return 'vista', quitar_definer(fila['Create View'])
    raise ValueError(
        f"Respuesta inesperada de SHOW CREATE TABLE para `{nombre}`: "
        f"columnas recibidas {list(fila.keys())}"
    )


def listar_columnas(cursor, tabla):
    """Devuelve los nombres de columna de una tabla, en su orden original."""
    cursor.execute(f"DESCRIBE `{tabla}`")
    return [col['Field'] for col in cursor.fetchall()]


def obtener_clave_primaria(cursor, tabla):
    """Devuelve las columnas que componen la clave primaria de la tabla."""
    cursor.execute(f"SHOW KEYS FROM `{tabla}` WHERE Key_name = 'PRIMARY'")
    return [fila['Column_name'] for fila in cursor.fetchall()]


def contar_filas(cursor, tabla):
    """Cuenta las filas actuales de una tabla."""
    cursor.execute(f"SELECT COUNT(*) AS total FROM `{tabla}`")
    return cursor.fetchone()['total']


def obtener_estructura_db(config, nombre='base de datos'):
    """Obtiene el esquema completo (tablas + columnas + triggers) de una BD."""
    conn = obtener_conexion(config, nombre)
    estructuras = {'tablas': {}, 'triggers': {}}
    try:
        with conn.cursor() as cursor:
            for tabla in listar_tablas(cursor, config['database']):
                cursor.execute(f"DESCRIBE `{tabla}`")
                columnas = cursor.fetchall()
                estructuras['tablas'][tabla] = {col['Field']: col for col in columnas}

            cursor.execute("""
                SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE,
                       ACTION_STATEMENT, ACTION_ORIENTATION, ACTION_TIMING
                FROM INFORMATION_SCHEMA.TRIGGERS
                WHERE TRIGGER_SCHEMA = %s
            """, (config['database'],))
            for trig in cursor.fetchall():
                estructuras['triggers'][trig['TRIGGER_NAME']] = trig
    finally:
        conn.close()
    return estructuras


# ---------------------------------------------------------------------------
# Generadores de progreso: cada uno emite ('progreso', mensaje) por etapa y
# finaliza con ('resultado', payload). Las rutas Flask los recorren y los
# traducen a eventos NDJSON para el cliente.
# ---------------------------------------------------------------------------

def comparar_estructuras_stream(origen_cfg, destino_cfg):
    """Compara ambos esquemas y reporta avance por etapa."""
    yield ('progreso', 'Leyendo estructura de la base de datos ORIGEN...')
    origen = obtener_estructura_db(origen_cfg, 'la base de datos ORIGEN')

    yield ('progreso', 'Leyendo estructura de la base de datos DESTINO...')
    destino = obtener_estructura_db(destino_cfg, 'la base de datos DESTINO')

    yield ('progreso', 'Comparando tablas y columnas...')
    comparativa = {
        'tablas_nuevas': [],
        'columnas_nuevas': [],
        'triggers_nuevos_o_modificados': []
    }

    for tabla, columnas in origen['tablas'].items():
        if tabla not in destino['tablas']:
            comparativa['tablas_nuevas'].append(tabla)
            continue
        for col_nombre, col_meta in columnas.items():
            if col_nombre not in destino['tablas'][tabla]:
                comparativa['columnas_nuevas'].append({
                    'tabla': tabla,
                    'columna': col_nombre,
                    'definicion': col_meta
                })

    yield ('progreso', 'Comparando triggers...')
    for trig_nombre, trig_meta in origen['triggers'].items():
        if trig_nombre not in destino['triggers']:
            comparativa['triggers_nuevos_o_modificados'].append({
                'nombre': trig_nombre, 'accion': 'Crear', 'meta': trig_meta
            })
        elif trig_meta['ACTION_STATEMENT'] != destino['triggers'][trig_nombre]['ACTION_STATEMENT']:
            comparativa['triggers_nuevos_o_modificados'].append({
                'nombre': trig_nombre, 'accion': 'Actualizar', 'meta': trig_meta
            })

    yield ('resultado', comparativa)


def obtener_ids_existentes(cursor, tabla, pk_columnas):
    """Devuelve el conjunto de valores de PK ya presentes en una tabla."""
    lista_pk = ", ".join(f"`{c}`" for c in pk_columnas)
    cursor.execute(f"SELECT {lista_pk} FROM `{tabla}`")
    return {tuple(fila[c] for c in pk_columnas) for fila in cursor.fetchall()}


def analizar_datos_stream(origen_cfg, destino_cfg):
    """
    Genera el detalle por tabla del origen, reportando avance tabla por tabla:
      - filas en origen y destino
      - clave primaria detectada
      - cuántas filas del origen aún NO existen en destino (candidatas a migrar)
    """
    conn_origen = obtener_conexion(origen_cfg, 'la base de datos ORIGEN')
    conn_destino = obtener_conexion(destino_cfg, 'la base de datos DESTINO')
    detalle = []
    try:
        with conn_origen.cursor() as cur_orig, conn_destino.cursor() as cur_dest:
            tablas_origen = listar_tablas(cur_orig, origen_cfg['database'])
            tablas_destino = set(listar_tablas(cur_dest, destino_cfg['database']))
            total = len(tablas_origen)

            for idx, tabla in enumerate(tablas_origen, start=1):
                yield ('progreso', f"Analizando datos ({idx}/{total}): `{tabla}`...")
                filas_origen = contar_filas(cur_orig, tabla)
                pk = obtener_clave_primaria(cur_orig, tabla)
                info = {
                    'tabla': tabla,
                    'filas_origen': filas_origen,
                    'filas_destino': None,
                    'filas_nuevas': None,
                    'clave_primaria': pk,
                    'estado': ''
                }

                if tabla not in tablas_destino:
                    # La tabla no existe en destino: todo su contenido es nuevo.
                    info['estado'] = 'nueva'
                    info['filas_destino'] = 0
                    info['filas_nuevas'] = filas_origen
                elif not pk:
                    # Sin PK no es posible detectar duplicados con seguridad.
                    info['estado'] = 'sin_pk'
                    info['filas_destino'] = contar_filas(cur_dest, tabla)
                else:
                    info['estado'] = 'existente'
                    info['filas_destino'] = contar_filas(cur_dest, tabla)
                    ids_destino = obtener_ids_existentes(cur_dest, tabla, pk)
                    ids_origen = obtener_ids_existentes(cur_orig, tabla, pk)
                    info['filas_nuevas'] = len(ids_origen - ids_destino)

                detalle.append(info)
    finally:
        conn_origen.close()
        conn_destino.close()
    yield ('resultado', detalle)


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/probar-conexion', methods=['POST'])
def probar_conexion():
    """Comprueba una sola conexión (origen o destino) y devuelve el detalle."""
    data = request.json or {}
    lado = data.get('lado', 'origen')
    nombre = 'la base de datos ORIGEN' if lado == 'origen' else 'la base de datos DESTINO'
    try:
        conexion = obtener_conexion(data.get('config', {}), nombre)
    except ErrorConexionMySQL as e:
        return jsonify({'status': 'error', 'message': str(e)})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error inesperado: {e}'})
    try:
        with conexion.cursor() as cursor:
            cursor.execute("SELECT VERSION() AS v, CURRENT_USER() AS u, DATABASE() AS d")
            info = cursor.fetchone()
            tablas, vistas = listar_tablas_y_vistas(cursor, info['d'])
    finally:
        conexion.close()
    return jsonify({
        'status': 'ok',
        'message': (
            f"Conexión exitosa a '{info['d']}' como {info['u']} "
            f"(MySQL/MariaDB {info['v']}): {len(tablas)} tablas, {len(vistas)} vistas."
        )
    })


@app.route('/analizar', methods=['POST'])
def analizar():
    """Análisis completo en streaming: diferencias estructurales + datos por tabla."""
    try:
        data = request.json
        origen_cfg = data['origen']
        destino_cfg = data['destino']
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Datos de conexión inválidos: {e}'}), 400

    def generador():
        try:
            diferencias = None
            for tipo, payload in comparar_estructuras_stream(origen_cfg, destino_cfg):
                if tipo == 'progreso':
                    yield evento('progreso', mensaje=payload)
                else:
                    diferencias = payload

            datos = None
            for tipo, payload in analizar_datos_stream(origen_cfg, destino_cfg):
                if tipo == 'progreso':
                    yield evento('progreso', mensaje=payload)
                else:
                    datos = payload

            resumen = {
                'tablas_nuevas': len(diferencias['tablas_nuevas']),
                'columnas_nuevas': len(diferencias['columnas_nuevas']),
                'triggers': len(diferencias['triggers_nuevos_o_modificados']),
                'tablas_analizadas': len(datos),
                'filas_pendientes': sum(t['filas_nuevas'] or 0 for t in datos)
            }
            yield evento('resultado', diferencias=diferencias, datos=datos, resumen=resumen)
        except Exception as e:
            det = detalle_error(e)
            yield evento('error', mensaje=det['mensaje_error'])

    return Response(stream_with_context(generador()), mimetype='application/x-ndjson')


@app.route('/respaldar', methods=['POST'])
def respaldar():
    """Genera un volcado SQL descargable de la base de datos DESTINO."""
    try:
        data = request.json
        destino_cfg = data['destino']

        conn = obtener_conexion(destino_cfg, 'la base de datos DESTINO')
        sql_backup = io.StringIO()
        sql_backup.write(f"-- Respaldo automático de la base de datos: {destino_cfg['database']}\n")
        sql_backup.write("SET FOREIGN_KEY_CHECKS=0;\n\n")

        with conn.cursor() as cursor:
            tablas, vistas = listar_tablas_y_vistas(cursor, destino_cfg['database'])

            for tabla in tablas:
                _, create_stmt = obtener_create(cursor, tabla)
                sql_backup.write(f"-- Estructura de tabla para {tabla}\n")
                sql_backup.write(f"DROP TABLE IF EXISTS `{tabla}`;\n")
                sql_backup.write(f"{create_stmt};\n\n")

                cursor.execute(f"SELECT * FROM `{tabla}`")
                filas = cursor.fetchall()
                if filas:
                    sql_backup.write(f"-- Datos de la tabla {tabla}\n")
                    for fila in filas:
                        columnas = ", ".join(f"`{k}`" for k in fila.keys())
                        valores = []
                        for val in fila.values():
                            if val is None:
                                valores.append("NULL")
                            elif isinstance(val, (int, float)):
                                valores.append(str(val))
                            else:
                                # conn.escape agrega comillas y escapa caracteres
                                # especiales según el tipo (str, datetime, bytes...).
                                valores.append(conn.escape(val))
                        sql_backup.write(
                            f"INSERT INTO `{tabla}` ({columnas}) VALUES ({', '.join(valores)});\n"
                        )
                    sql_backup.write("\n")

            # Las vistas se recrean al final, cuando ya existen las tablas
            # de las que dependen. No tienen datos propios que respaldar.
            for vista in vistas:
                _, create_stmt = obtener_create(cursor, vista)
                sql_backup.write(f"-- Estructura de vista para {vista}\n")
                sql_backup.write(f"DROP VIEW IF EXISTS `{vista}`;\n")
                sql_backup.write(f"{create_stmt};\n\n")

            cursor.execute("""
                SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE,
                       ACTION_STATEMENT, ACTION_TIMING
                FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = %s
            """, (destino_cfg['database'],))
            for trig in cursor.fetchall():
                sql_backup.write(f"-- Trigger {trig['TRIGGER_NAME']}\n")
                sql_backup.write(f"DROP TRIGGER IF EXISTS `{trig['TRIGGER_NAME']}`;\n")
                sql_backup.write(
                    f"CREATE TRIGGER `{trig['TRIGGER_NAME']}` {trig['ACTION_TIMING']} "
                    f"{trig['EVENT_MANIPULATION']} ON `{trig['EVENT_OBJECT_TABLE']}` "
                    f"FOR EACH ROW {trig['ACTION_STATEMENT']};\n\n"
                )

        sql_backup.write("SET FOREIGN_KEY_CHECKS=1;\n")
        conn.close()

        return Response(
            sql_backup.getvalue(),
            mimetype="text/plain",
            headers={"Content-disposition": f"attachment; filename=backup_{destino_cfg['database']}.sql"}
        )
    except Exception as e:
        det = detalle_error(e)
        return jsonify({
            'status': 'error',
            'message': f"Error al generar backup: {det['mensaje_error']}"
        }), 500


@app.route('/migrar', methods=['POST'])
def migrar():
    """Aplica al destino, en streaming, los cambios ESTRUCTURALES detectados."""
    try:
        data = request.json
        origen_cfg = data['origen']
        destino_cfg = data['destino']
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Datos de conexión inválidos: {e}'}), 400

    def generador():
        try:
            # Recalcular diferencias por seguridad (el esquema pudo cambiar).
            diffs = None
            for tipo, payload in comparar_estructuras_stream(origen_cfg, destino_cfg):
                if tipo == 'progreso':
                    yield evento('progreso', mensaje=payload)
                else:
                    diffs = payload

            conn_origen = obtener_conexion(origen_cfg, 'la base de datos ORIGEN')
            conn_destino = obtener_conexion(destino_cfg, 'la base de datos DESTINO')
            pasos_ejecutados = []

            try:
                with conn_origen.cursor() as cur_orig, conn_destino.cursor() as cur_dest:
                    cur_dest.execute("SET FOREIGN_KEY_CHECKS=0;")

                    # 1. Crear tablas (o vistas) completamente nuevas
                    for tabla in diffs['tablas_nuevas']:
                        yield evento('progreso', mensaje=f"Creando tabla `{tabla}`...")
                        tipo_objeto, create_stmt = obtener_create(cur_orig, tabla)
                        cur_dest.execute(quitar_collation(create_stmt))
                        paso = f"Creada {tipo_objeto} nueva: {tabla}"
                        pasos_ejecutados.append(paso)
                        yield evento('paso', mensaje=paso)

                    # 2. Agregar columnas nuevas a tablas existentes
                    for col_info in diffs['columnas_nuevas']:
                        yield evento('progreso', mensaje=(
                            f"Añadiendo columna `{col_info['columna']}` a `{col_info['tabla']}`..."
                        ))
                        meta = col_info['definicion']
                        nulo = "NULL" if meta['Null'] == "YES" else "NOT NULL"
                        defecto = f"DEFAULT '{meta['Default']}'" if meta['Default'] is not None else ""
                        cur_dest.execute(
                            f"ALTER TABLE `{col_info['tabla']}` "
                            f"ADD COLUMN `{col_info['columna']}` {meta['Type']} {nulo} {defecto}"
                        )
                        paso = f"Añadida columna `{col_info['columna']}` a la tabla `{col_info['tabla']}`"
                        pasos_ejecutados.append(paso)
                        yield evento('paso', mensaje=paso)

                    # 3. Triggers nuevos o modificados
                    for trig_info in diffs['triggers_nuevos_o_modificados']:
                        yield evento('progreso', mensaje=f"Sincronizando trigger `{trig_info['nombre']}`...")
                        meta = trig_info['meta']
                        cur_dest.execute(f"DROP TRIGGER IF EXISTS `{trig_info['nombre']}`")
                        cur_dest.execute(
                            f"CREATE TRIGGER `{trig_info['nombre']}` {meta['ACTION_TIMING']} "
                            f"{meta['EVENT_MANIPULATION']} ON `{meta['EVENT_OBJECT_TABLE']}` "
                            f"FOR EACH ROW {meta['ACTION_STATEMENT']}"
                        )
                        paso = f"Procesado Trigger: {trig_info['nombre']} ({trig_info['accion']})"
                        pasos_ejecutados.append(paso)
                        yield evento('paso', mensaje=paso)

                    cur_dest.execute("SET FOREIGN_KEY_CHECKS=1;")
                    conn_destino.commit()
            finally:
                conn_origen.close()
                conn_destino.close()

            yield evento('resultado', pasos=pasos_ejecutados)
        except Exception as e:
            det = detalle_error(e)
            yield evento('error', mensaje=f"Error durante la migración: {det['mensaje_error']}")

    return Response(stream_with_context(generador()), mimetype='application/x-ndjson')


@app.route('/migrar-datos', methods=['POST'])
def migrar_datos():
    """
    Migra en streaming los DATOS de las tablas seleccionadas por el usuario.

    Reglas:
      - Solo se insertan registros cuyo id / clave primaria NO exista en destino
        (INSERT IGNORE: los duplicados se omiten silenciosamente y se contabilizan).
      - Si la tabla no existe en destino, se crea automáticamente desde el origen.
      - Solo se copian las columnas comunes a origen y destino.
      - Cada tabla se confirma (commit) de forma independiente: un error en una
        tabla no revierte las anteriores y el proceso continúa con las demás.
    """
    try:
        data = request.json
        origen_cfg = data['origen']
        destino_cfg = data['destino']
        tablas_solicitadas = data.get('tablas', [])
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Datos de conexión inválidos: {e}'}), 400

    def generador():
        if not tablas_solicitadas:
            yield evento('error', mensaje='No se seleccionó ninguna tabla para migrar.')
            return

        try:
            conn_origen = obtener_conexion(origen_cfg, 'la base de datos ORIGEN')
            conn_destino = obtener_conexion(destino_cfg, 'la base de datos DESTINO')
            reporte = []
            total = len(tablas_solicitadas)
            yield evento('inicio', total=total)

            try:
                with conn_origen.cursor() as cur_orig, conn_destino.cursor() as cur_dest:
                    tablas_origen = set(listar_tablas(cur_orig, origen_cfg['database']))
                    tablas_destino = set(listar_tablas(cur_dest, destino_cfg['database']))
                    cur_dest.execute("SET FOREIGN_KEY_CHECKS=0;")

                    for idx, tabla in enumerate(tablas_solicitadas, start=1):
                        yield evento('progreso', mensaje=f"Migrando datos ({idx}/{total}): `{tabla}`...")
                        resultado = {
                            'tabla': tabla, 'estado': 'ok', 'leidas': 0,
                            'insertadas': 0, 'omitidas': 0, 'detalle': ''
                        }
                        try:
                            # Validación: la tabla debe existir en el origen
                            # (también evita inyección vía nombres de tabla).
                            if tabla not in tablas_origen:
                                raise ValueError("La tabla no existe en la base de datos de origen.")

                            # Crear la tabla en destino si aún no existe.
                            if tabla not in tablas_destino:
                                tipo_objeto, create_stmt = obtener_create(cur_orig, tabla)
                                cur_dest.execute(quitar_collation(create_stmt))
                                tablas_destino.add(tabla)
                                resultado['detalle'] = f'{tipo_objeto.capitalize()} creada en destino. '
                                if tipo_objeto == 'vista':
                                    # Una vista no almacena datos propios.
                                    resultado['detalle'] += 'Las vistas no requieren migración de datos.'
                                    reporte.append(resultado)
                                    yield evento('tabla_resultado', **resultado)
                                    continue

                            # Solo columnas presentes en ambos lados.
                            cols_origen = listar_columnas(cur_orig, tabla)
                            cols_destino = set(listar_columnas(cur_dest, tabla))
                            comunes = [c for c in cols_origen if c in cols_destino]
                            if not comunes:
                                raise ValueError("Origen y destino no comparten ninguna columna.")

                            lista_cols = ", ".join(f"`{c}`" for c in comunes)
                            marcadores = ", ".join(["%s"] * len(comunes))
                            insert_sql = (
                                f"INSERT IGNORE INTO `{tabla}` ({lista_cols}) VALUES ({marcadores})"
                            )

                            # Lectura y escritura por lotes para no agotar memoria.
                            cur_orig.execute(f"SELECT {lista_cols} FROM `{tabla}`")
                            while True:
                                lote = cur_orig.fetchmany(TAMANO_LOTE)
                                if not lote:
                                    break
                                valores = [[fila[c] for c in comunes] for fila in lote]
                                cur_dest.executemany(insert_sql, valores)
                                # rowcount de INSERT IGNORE = filas realmente insertadas.
                                resultado['insertadas'] += max(cur_dest.rowcount, 0)
                                resultado['leidas'] += len(lote)

                            conn_destino.commit()
                            resultado['omitidas'] = resultado['leidas'] - resultado['insertadas']
                            resultado['detalle'] += (
                                f"{resultado['insertadas']} filas nuevas insertadas, "
                                f"{resultado['omitidas']} omitidas por id ya existente."
                            )
                        except Exception as e:
                            conn_destino.rollback()
                            resultado['estado'] = 'error'
                            resultado['detalle'] = str(e)

                        reporte.append(resultado)
                        yield evento('tabla_resultado', **resultado)

                    cur_dest.execute("SET FOREIGN_KEY_CHECKS=1;")
                    conn_destino.commit()
            finally:
                conn_origen.close()
                conn_destino.close()

            total_insertadas = sum(r['insertadas'] for r in reporte)
            total_omitidas = sum(r['omitidas'] for r in reporte)
            errores = [r['tabla'] for r in reporte if r['estado'] == 'error']

            yield evento('resultado', reporte=reporte, resumen={
                'tablas_procesadas': len(reporte),
                'insertadas': total_insertadas,
                'omitidas': total_omitidas,
                'errores': errores
            })
        except Exception as e:
            det = detalle_error(e)
            yield evento('error', mensaje=f"Error durante la migración de datos: {det['mensaje_error']}")

    return Response(stream_with_context(generador()), mimetype='application/x-ndjson')


if __name__ == '__main__':
    app.run(debug=True)

"""
normalizer_lugares.py
=====================
ETL para DATOS2026-3.TXT — Lugares e información de ubicación.

Tareas según instrucción del PDF:
  - Eliminar registros duplicados
  - Separar en 3 tablas / colecciones:
      • Lugares    → {id, nombre}
      • Georeferencias → {lugar_id, latitud, longitud}
      • Direcciones    → {lugar_id, nombre_calle, numero_calle,
                          ciudad_estado_provincia, pais}

Salida: guarda en Firebase Firestore (colecciones "lugares",
        "georeferencias", "direcciones") y genera JSON local.
"""

import re
import json
from datetime import datetime
from pathlib import Path

# ──────────────────────────────────────────────
# 1. Parseo de la dirección
# ──────────────────────────────────────────────

def parsear_direccion(direccion_raw: str) -> dict:
    """
    Divide una dirección libre en sus componentes:
      nombre_calle, numero_calle, ciudad_estado_provincia, pais.

    Heurística:
      - El último token separado por coma es el país.
      - El penúltimo token es ciudad/estado/provincia.
      - En el primer fragmento: el número de calle (si hay) es el
        primer elemento numérico; el resto es nombre de calle.
    """
    if not direccion_raw or direccion_raw.strip().lower() in ("", "n/a", "-"):
        return {
            "nombre_calle"           : "",
            "numero_calle"           : "",
            "ciudad_estado_provincia": "",
            "pais"                   : "",
        }

    partes = [p.strip() for p in direccion_raw.split(",")]
    partes = [p for p in partes if p]  # eliminar vacíos

    pais = partes[-1] if len(partes) >= 1 else ""
    ciudad_estado = partes[-2] if len(partes) >= 2 else ""
    fragmento_calle = ", ".join(partes[:-2]) if len(partes) > 2 else ""

    # Extraer número de calle (primer secuencia numérica en el fragmento)
    numero_match = re.search(r"\b(\d+[A-Za-z]?)\b", fragmento_calle)
    numero_calle = numero_match.group(1) if numero_match else ""

    # Nombre de calle = resto sin el número
    if numero_calle:
        nombre_calle = re.sub(r"\b" + re.escape(numero_calle) + r"\b", "", fragmento_calle).strip(" ,.-")
    else:
        nombre_calle = fragmento_calle.strip()

    return {
        "nombre_calle"           : nombre_calle,
        "numero_calle"           : numero_calle,
        "ciudad_estado_provincia": ciudad_estado,
        "pais"                   : pais,
    }


def parsear_georef(georef_raw: str) -> dict | None:
    """
    Extrae latitud y longitud de una cadena "lat, lon".
    Devuelve None si no es parseable.
    """
    m = re.match(r"^\s*(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\s*$", georef_raw.strip())
    if m:
        return {"latitud": float(m.group(1)), "longitud": float(m.group(2))}
    return None


# ──────────────────────────────────────────────
# 2. Procesamiento del archivo
# ──────────────────────────────────────────────

def procesar_lugares(ruta_archivo: str | Path) -> dict:
    """
    Lee DATOS2026-3.TXT (separado por ;) y devuelve:
      - lugares:       lista de {id, nombre}
      - georeferencias: lista de {lugar_id, latitud, longitud}
      - direcciones:   lista de {lugar_id, nombre_calle, numero_calle,
                                  ciudad_estado_provincia, pais}
      - stats:         estadísticas del proceso
      - errores:       líneas no procesables
    """
    lineas = Path(ruta_archivo).read_text(encoding="utf-8", errors="replace").splitlines()
    lineas = [l.strip() for l in lineas if l.strip()]

    # Saltar cabecera si existe
    if lineas and "Nombre del lugar" in lineas[0]:
        lineas = lineas[1:]

    errores    = []
    duplicados = []
    # Clave de deduplicación: (nombre_lower, georef_str) para detectar copias exactas
    vistos_clave = {}

    lugares        = []
    georeferencias = []
    direcciones    = []

    id_counter = 1

    for i, linea in enumerate(lineas, 2):  # 2 porque la fila 1 era cabecera
        # Arreglar separador roto por encoding (a veces ';' llega como '?')
        columnas = linea.split(";")

        if len(columnas) < 3:
            errores.append({"linea": i, "contenido": linea, "motivo": "Menos de 3 columnas"})
            continue

        nombre_raw   = columnas[0].strip()
        direccion_raw = columnas[1].strip()
        georef_raw   = columnas[2].strip()

        # Sanear nombre (quitar caracteres de control)
        nombre = re.sub(r"[\x00-\x1f\x7f]", "", nombre_raw).strip()
        if not nombre:
            errores.append({"linea": i, "contenido": linea, "motivo": "Nombre vacío"})
            continue

        # Clave de deduplicación
        clave = (nombre.lower(), georef_raw.lower())
        if clave in vistos_clave:
            duplicados.append({
                "linea"     : i,
                "nombre"    : nombre,
                "duplica_a" : vistos_clave[clave],
            })
            continue
        vistos_clave[clave] = i

        # Parsear georeferencia
        georef = parsear_georef(georef_raw)
        if georef is None:
            errores.append({"linea": i, "contenido": linea, "motivo": f"Georef inválida: '{georef_raw}'"})
            # Incluimos igual sin georef
            georef = {"latitud": None, "longitud": None}

        # Parsear dirección
        direccion = parsear_direccion(direccion_raw)

        lugar_id = f"lugar_{id_counter:04d}"
        id_counter += 1

        lugares.append({
            "id"    : lugar_id,
            "nombre": nombre,
        })

        georeferencias.append({
            "lugar_id": lugar_id,
            "nombre"  : nombre,   # referencia amigable
            **georef,
        })

        direcciones.append({
            "lugar_id": lugar_id,
            "nombre"  : nombre,   # referencia amigable
            **direccion,
        })

    stats = {
        "leidos"     : len(lineas),
        "unicos"     : len(lugares),
        "duplicados" : len(duplicados),
        "errores"    : len(errores),
    }

    return {
        "lugares"        : lugares,
        "georeferencias" : georeferencias,
        "direcciones"    : direcciones,
        "stats"          : stats,
        "duplicados"     : duplicados,
        "errores"        : errores,
    }


# ──────────────────────────────────────────────
# 3. Exportar JSON
# ──────────────────────────────────────────────

def exportar_json_lugares(resultado: dict, ruta_salida: str | Path) -> Path:
    """Guarda las tres colecciones como JSON para importar a Firestore."""
    ruta_salida = Path(ruta_salida)
    ruta_salida.parent.mkdir(exist_ok=True)

    exportacion = {
        "timestamp"     : datetime.now().isoformat(),
        "stats"         : resultado["stats"],
        "colecciones"   : {
            "lugares"       : resultado["lugares"],
            "georeferencias": resultado["georeferencias"],
            "direcciones"   : resultado["direcciones"],
        },
    }

    ruta_salida.write_text(
        json.dumps(exportacion, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    return ruta_salida


# ──────────────────────────────────────────────
# 4. Guardar en Firebase
# ──────────────────────────────────────────────

def guardar_en_firebase(resultado: dict, firebase_config: dict | None = None):
    """
    Guarda las tres colecciones en Firebase Firestore.

    Colecciones creadas:
      - lugares
      - georeferencias
      - direcciones

    Parámetros:
      firebase_config: {"credential_path": "serviceAccountKey.json"}
                       Si es None usa credenciales de entorno.
    """
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        print("⚠  firebase-admin no instalado. Ejecute: pip install firebase-admin")
        return False

    if not firebase_admin._apps:
        if firebase_config and "credential_path" in firebase_config:
            cred = credentials.Certificate(firebase_config["credential_path"])
        else:
            cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    def subir_coleccion(nombre_col: list[dict], col_name: str):
        """Sube una lista de registros a una colección Firestore."""
        col = db.collection(col_name)

        # Limpiar colección (batch delete)
        for doc in col.stream():
            doc.reference.delete()

        batch = db.batch()
        count = 0
        for registro in nombre_col:
            doc_id  = registro.get("id") or registro.get("lugar_id") or None
            doc_ref = col.document(doc_id) if doc_id else col.document()
            batch.set(doc_ref, registro)
            count += 1
            if count % 500 == 0:
                batch.commit()
                batch = db.batch()
        batch.commit()
        print(f"   ✅ {count} documentos → colección '{col_name}'")

    print("Subiendo a Firestore…")
    subir_coleccion(resultado["lugares"],        "lugares")
    subir_coleccion(resultado["georeferencias"], "georeferencias")
    subir_coleccion(resultado["direcciones"],    "direcciones")
    return True


# ──────────────────────────────────────────────
# 5. Main
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    ruta = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("DATOS2026-3.TXT")
    if not ruta.exists():
        print(f"Archivo no encontrado: {ruta}")
        sys.exit(1)

    print(f"Procesando: {ruta}")
    resultado = procesar_lugares(ruta)

    s = resultado["stats"]
    print(f"\n📊 Estadísticas:")
    print(f"   Leidos      : {s['leidos']}")
    print(f"   Únicos      : {s['unicos']}")
    print(f"   Duplicados  : {s['duplicados']}")
    print(f"   Errores     : {s['errores']}")

    salida = Path("output") / "lugares_normalizados.json"
    exportar_json_lugares(resultado, salida)
    print(f"\n💾 JSON exportado: {salida}")

    # Guardar en Firebase (descomentar y configurar)
    # guardar_en_firebase(resultado, {"credential_path": "serviceAccountKey.json"})

    print("\n🗺  Primeros 5 lugares:")
    for l in resultado["lugares"][:5]:
        g = resultado["georeferencias"][resultado["lugares"].index(l)]
        d = resultado["direcciones"][resultado["lugares"].index(l)]
        print(f"   {l['nombre']:40} | lat={g['latitud']}, lon={g['longitud']}")
        print(f"   {'':42} calle={d['nombre_calle']} #{d['numero_calle']} | {d['ciudad_estado_provincia']} | {d['pais']}")

    print(f"\n🔁 Duplicados eliminados ({len(resultado['duplicados'])}):")
    for d in resultado["duplicados"][:5]:
        print(f"   L{d['linea']:>4} | {d['nombre']} (duplica L{d['duplica_a']})")

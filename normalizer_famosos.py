"""
normalizer_famosos.py
=====================
ETL para DATOS2026-2.TXT — Famosos y fechas de nacimiento.

Tareas según instrucción del PDF:
  - Unificar fechas al formato chileno DD-MM-YYYY
  - Agregar nombres de los famosos en la nueva tabla
  - Quitar separadores no permitidos
  - Eliminar registros duplicados
  - Agregar atributo edad (calculada al día de hoy)
  - Agregar flag es_cumpleanios (True si hoy coincide con DD-MM)

Salida: guarda en Firebase Firestore (colección "famosos")
        y genera un archivo JSON local para revisión/importación.
"""

import re
import json
from datetime import datetime, date
from pathlib import Path

# Fecha de referencia (hoy)
HOY = date.today()

# ──────────────────────────────────────────────
# 1. Parseo de fechas  (múltiples formatos)
# ──────────────────────────────────────────────

# Patrones soportados (en orden de prioridad):
FORMATOS_FECHA = [
    # YYYY/MM/DD  o  YYYY-MM-DD
    (r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", "YMD"),
    # DD/MM/YYYY  o  DD-MM-YYYY
    (r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", "DMY"),
    # DD/MM/YYYY orden ambiguo por barra → se trata como DMY
    (r"(\d{1,2})/(\d{1,2})/(\d{4})", "DMY"),
]

MESES_TEXTO = {
    "enero": 1, "february": 2, "febrero": 2, "marzo": 3, "april": 4,
    "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

def parsear_fecha(texto: str):
    """
    Intenta extraer una fecha del texto.
    Devuelve (dia, mes, anio) o None si es fecha histórica/vaga.
    """
    texto = texto.strip()

    # Fechas históricas vagas (a.C., "alrededor de", etc.) → ignorar
    if re.search(r"a\.?c\.?|alrededor", texto, re.I):
        return None

    for patron, orden in FORMATOS_FECHA:
        m = re.search(patron, texto)
        if m:
            g = m.groups()
            if orden == "YMD":
                anio, mes, dia = int(g[0]), int(g[1]), int(g[2])
            else:  # DMY
                dia, mes, anio = int(g[0]), int(g[1]), int(g[2])
            # Validación básica
            if 1 <= mes <= 12 and 1 <= dia <= 31 and 1000 <= anio <= 2100:
                return dia, mes, anio

    return None


def calcular_edad(dia: int, mes: int, anio: int) -> int | None:
    """Calcula la edad en años cumplidos."""
    try:
        nacimiento = date(anio, mes, dia)
        edad = HOY.year - nacimiento.year
        # Ajuste si aún no llegó el cumpleaños este año
        if (HOY.month, HOY.day) < (nacimiento.month, nacimiento.day):
            edad -= 1
        return edad
    except ValueError:
        return None


def es_cumpleanios_hoy(dia: int, mes: int) -> bool:
    """True si el día y mes coinciden con hoy."""
    return HOY.day == dia and HOY.month == mes


# ──────────────────────────────────────────────
# 2. Procesamiento del archivo
# ──────────────────────────────────────────────

def procesar_famosos(ruta_archivo: str | Path) -> dict:
    """
    Lee DATOS2026-2.TXT y devuelve un dict con:
      - registros: lista de dicts listos para Firestore
      - stats: estadísticas del proceso
      - cambios: log de transformaciones
    """
    lineas = Path(ruta_archivo).read_text(encoding="utf-8", errors="replace").splitlines()
    lineas = [l.strip() for l in lineas if l.strip()]

    cambios = []
    errores = []
    vistos = {}   # clave: (nombre_normalizado, fecha_str) → evita duplicados

    registros = []

    for i, linea in enumerate(lineas, 1):
        # Separar número de línea opcional: "1. William Shakespeare - 1564/04/23"
        partes = re.split(r"^\d+\.\s*", linea, maxsplit=1)
        contenido = partes[-1].strip()

        # Separar nombre y fecha por " - "
        if " - " in contenido:
            nombre_raw, fecha_raw = contenido.split(" - ", 1)
        else:
            errores.append({"linea": i, "contenido": linea, "motivo": "No se encontró separador ' - '"})
            continue

        nombre_raw = nombre_raw.strip()
        fecha_raw  = fecha_raw.strip()

        # Parsear fecha
        fecha_parsed = parsear_fecha(fecha_raw)
        if fecha_parsed is None:
            # Fecha histórica/vaga → incluir sin edad ni flag
            fecha_normalizada = fecha_raw  # conservar original
            edad = None
            flag_cumple = False
            dia = mes = None
        else:
            dia, mes, anio = fecha_parsed
            fecha_normalizada = f"{dia:02d}-{mes:02d}-{anio}"
            edad = calcular_edad(dia, mes, anio)
            flag_cumple = es_cumpleanios_hoy(dia, mes)

        # Clave de deduplicación (nombre + fecha DD-MM-YYYY)
        clave = (nombre_raw.lower(), fecha_normalizada.lower())
        if clave in vistos:
            cambios.append({
                "linea": i,
                "original": linea,
                "accion": f"DUPLICADO de línea {vistos[clave]}, eliminado"
            })
            continue
        vistos[clave] = i

        # Detectar cambio de formato de fecha
        if fecha_raw != fecha_normalizada and fecha_parsed is not None:
            cambios.append({
                "linea": i,
                "original": linea,
                "accion": f"Fecha normalizada: '{fecha_raw}' → '{fecha_normalizada}'"
            })

        registro = {
            "nombre"          : nombre_raw,
            "fecha_nacimiento": fecha_normalizada,
            "edad"            : edad,
            "es_cumpleanios"  : flag_cumple,
        }
        registros.append(registro)

    stats = {
        "leidos"     : len(lineas),
        "procesados" : len(registros),
        "duplicados" : len(lineas) - len(registros) - len(errores),
        "errores"    : len(errores),
        "cumpleanios": sum(1 for r in registros if r["es_cumpleanios"]),
    }

    return {
        "registros": registros,
        "stats"    : stats,
        "cambios"  : cambios,
        "errores"  : errores,
    }


# ──────────────────────────────────────────────
# 3. Exportar JSON (para importar a Firestore)
# ──────────────────────────────────────────────

def exportar_json_famosos(resultado: dict, ruta_salida: str | Path) -> Path:
    """Guarda los registros como JSON para importar a Firestore."""
    ruta_salida = Path(ruta_salida)
    ruta_salida.parent.mkdir(exist_ok=True)

    exportacion = {
        "coleccion": "famosos",
        "timestamp": datetime.now().isoformat(),
        "stats"    : resultado["stats"],
        "registros": resultado["registros"],
    }

    ruta_salida.write_text(
        json.dumps(exportacion, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    return ruta_salida


# ──────────────────────────────────────────────
# 4. Guardar en Firebase (requiere credenciales)
# ──────────────────────────────────────────────

def guardar_en_firebase(resultado: dict, firebase_config: dict | None = None):
    """
    Guarda los registros en Firebase Firestore.
    
    Requiere:
      pip install firebase-admin
    
    Parámetros:
      firebase_config: dict con la ruta al archivo de credenciales de servicio.
                       Ejemplo: {"credential_path": "serviceAccountKey.json"}
                       Si es None, intenta con credenciales de entorno.
    """
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        print("⚠  firebase-admin no instalado. Ejecute: pip install firebase-admin")
        return False

    # Inicializar app solo si no está ya inicializada
    if not firebase_admin._apps:
        if firebase_config and "credential_path" in firebase_config:
            cred = credentials.Certificate(firebase_config["credential_path"])
        else:
            cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    col = db.collection("famosos")

    # Limpiar colección antes de insertar (batch delete)
    batch_size = 100
    docs = col.limit(batch_size).stream()
    for doc in docs:
        doc.reference.delete()

    # Insertar en lotes de 500 (límite de Firestore)
    batch = db.batch()
    count = 0
    for registro in resultado["registros"]:
        doc_ref = col.document()   # ID automático
        batch.set(doc_ref, registro)
        count += 1
        if count % 500 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()

    print(f"✅ {count} registros guardados en Firestore (colección 'famosos')")
    return True


# ──────────────────────────────────────────────
# 5. Main / uso directo
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    ruta = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("DATOS2026-2.TXT")
    if not ruta.exists():
        print(f"Archivo no encontrado: {ruta}")
        sys.exit(1)

    print(f"Procesando: {ruta}")
    resultado = procesar_famosos(ruta)

    # Mostrar estadísticas
    s = resultado["stats"]
    print(f"\n📊 Estadísticas:")
    print(f"   Leidos      : {s['leidos']}")
    print(f"   Procesados  : {s['procesados']}")
    print(f"   Duplicados  : {s['duplicados']}")
    print(f"   Errores     : {s['errores']}")
    print(f"   Cumpleaños  : {s['cumpleanios']}")

    # Exportar JSON
    salida = Path("output") / "famosos_normalizados.json"
    exportar_json_famosos(resultado, salida)
    print(f"\n💾 JSON exportado: {salida}")

    # Guardar en Firebase (descomentar y configurar credenciales)
    # guardar_en_firebase(resultado, {"credential_path": "serviceAccountKey.json"})

    # Mostrar primeros registros
    print("\n🔍 Primeros 5 registros normalizados:")
    for r in resultado["registros"][:5]:
        cumple = "🎂" if r["es_cumpleanios"] else ""
        print(f"   {r['nombre']:35} | {r['fecha_nacimiento']} | {r['edad']} años {cumple}")

    # Mostrar cambios
    if resultado["cambios"]:
        print(f"\n📝 Cambios realizados ({len(resultado['cambios'])}):")
        for c in resultado["cambios"][:10]:
            print(f"   L{c['linea']:>4} | {c['accion']}")

"""
actualizar_precios.py
Descarga el ZIP del SEPA, filtra Viedma y Carmen de Patagones,
y genera precios_viedma_hoy.json en la misma carpeta.

Funciona tanto en la PC (python actualizar_precios.py)
como en GitHub Actions (sin prompts interactivos).
"""

import requests
import zipfile
import io
import csv
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# ── Configuración ──────────────────────────────────────────────────────────────
CODIGOS_POSTALES = {'8500', '8504'}
LOCALIDADES      = {'viedma', 'carmen de patagones', 'patagones'}
API_CKAN         = 'https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios'
SALIDA           = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'precios_viedma_hoy.json')
EN_CI            = os.environ.get('CI') == 'true'   # True cuando corre en GitHub Actions

AR_TZ            = timezone(timedelta(hours=-3))

# ── Helpers CSV ────────────────────────────────────────────────────────────────
def leer_csv_bytes(data_bytes, encoding='utf-8'):
    texto = data_bytes.decode(encoding, errors='replace')
    lineas = [l for l in texto.splitlines()
              if l.strip() and not l.startswith('Última actualización')]
    return list(csv.DictReader(lineas, delimiter='|'))

def es_local(row):
    cp  = (row.get('sucursales_codigo_postal') or '').strip()
    loc = (row.get('sucursales_localidad')     or '').strip().lower()
    return cp in CODIGOS_POSTALES or any(l in loc for l in LOCALIDADES)

# ── Obtener URL del ZIP más reciente ──────────────────────────────────────────
def obtener_url_zip():
    print('📡 Consultando API del gobierno...')
    r = requests.get(API_CKAN, timeout=30)
    r.raise_for_status()
    recursos = r.json()['result']['resources']

    # Buscar el recurso ZIP más reciente
    for rec in sorted(recursos,
                      key=lambda x: x.get('last_modified', ''), reverse=True):
        url = rec.get('url', '')
        if url.lower().endswith('.zip'):
            mod = rec.get('last_modified', 'desconocida')
            print(f'📦 ZIP encontrado: {url}')
            print(f'   Última modificación: {mod}')

            # Verificar si es de hoy
            if mod:
                try:
                    fecha_rec = datetime.fromisoformat(
                        mod.replace('Z', '+00:00')).astimezone(AR_TZ).date()
                    hoy = datetime.now(AR_TZ).date()
                    if fecha_rec < hoy:
                        msg = (f'⚠️  El archivo es del {fecha_rec} '
                               f'(hoy es {hoy}). Los datos pueden ser de ayer.')
                        print(msg)
                        if not EN_CI:
                            resp = input('¿Continuar igual? (s/n): ').strip().lower()
                            if resp != 's':
                                print('Cancelado.')
                                sys.exit(0)
                        else:
                            print('   (Modo automático: continuando igual)')
                except Exception:
                    pass
            return url

    raise RuntimeError('No se encontró ningún ZIP en la API del SEPA.')

# ── Descarga con progreso ──────────────────────────────────────────────────────
def descargar_zip(url):
    print('⬇️  Descargando ZIP...')
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()
    total = int(r.headers.get('content-length', 0))
    buf   = io.BytesIO()
    bajas = 0
    for chunk in r.iter_content(65536):
        buf.write(chunk)
        bajas += len(chunk)
        if total:
            pct = bajas * 100 // total
            mb  = bajas / 1_048_576
            print(f'\r   {pct}% — {mb:.1f} MB', end='', flush=True)
    print(f'\r✅ Descarga completa: {bajas/1_048_576:.1f} MB          ')
    buf.seek(0)
    return buf

# ── Procesar ──────────────────────────────────────────────────────────────────
def to_float(v):
    try: return float(str(v).strip()) if str(v).strip() else None
    except: return None

def procesar(zip_externo_buf):
    sucursales_out = {}
    productos_out  = {}
    errores        = []

    with zipfile.ZipFile(zip_externo_buf) as zext:
        zips_internos = [n for n in zext.namelist() if n.lower().endswith('.zip')]
        total = len(zips_internos)
        print(f'📂 ZIPs internos encontrados: {total}')

        for idx, nombre_zip in enumerate(zips_internos, 1):
            print(f'\r   [{idx}/{total}] {nombre_zip[:55]}', end='', flush=True)
            try:
                with zipfile.ZipFile(io.BytesIO(zext.read(nombre_zip))) as zint:
                    archivos = {n.lower(): n for n in zint.namelist()}

                    # 1) Leer comercio.csv UNA sola vez por ZIP
                    bandera_comercio = ''
                    if 'comercio.csv' in archivos:
                        try:
                            for cr in leer_csv_bytes(zint.read(archivos['comercio.csv'])):
                                b = (cr.get('comercio_bandera_nombre') or
                                     cr.get('bandera_nombre') or '').strip()
                                if b:
                                    bandera_comercio = b
                                    break
                        except Exception as e:
                            errores.append(f'comercio.csv en {nombre_zip}: {e}')

                    # 2) Leer sucursales.csv
                    if 'sucursales.csv' not in archivos:
                        continue
                    suc_bytes = zint.read(archivos['sucursales.csv'])
                    locales = {}
                    for row in leer_csv_bytes(suc_bytes):
                        if not es_local(row):
                            continue
                        id_c = str(row.get('id_comercio','')).strip()
                        id_b = str(row.get('id_bandera','')).strip()
                        id_s = str(row.get('id_sucursal','')).strip()
                        skey = f'{id_c}_{id_b}_{id_s}'
                        locales[skey] = True
                        sucursales_out[skey] = {
                            'nombre'   : row.get('sucursales_nombre','').strip(),
                            'bandera'  : bandera_comercio,
                            'calle'    : row.get('sucursales_calle','').strip(),
                            'numero'   : row.get('sucursales_numero','').strip(),
                            'localidad': row.get('sucursales_localidad','').strip(),
                            'cp'       : row.get('sucursales_codigo_postal','').strip(),
                            'horarios' : {
                                d: row.get(f'sucursales_horario_{d}','').strip()
                                for d in ['lunes','martes','miercoles','jueves',
                                          'viernes','sabado','domingo']
                            }
                        }

                    if not locales:
                        continue
                    print(f' → {len(locales)} sucursal(es)', end='', flush=True)

                    # 3) Leer productos.csv
                    if 'productos.csv' not in archivos:
                        continue
                    prod_bytes = zint.read(archivos['productos.csv'])
                    prod_antes = len(productos_out)
                    for row in leer_csv_bytes(prod_bytes):
                        id_c = str(row.get('id_comercio','')).strip()
                        id_b = str(row.get('id_bandera','')).strip()
                        id_s = str(row.get('id_sucursal','')).strip()
                        skey = f'{id_c}_{id_b}_{id_s}'
                        if skey not in locales:
                            continue
                        ean = (row.get('productos_ean') or
                               row.get('id_producto') or '').strip()
                        if not ean:
                            continue
                        if ean not in productos_out:
                            productos_out[ean] = {
                                'descripcion': row.get('productos_descripcion','').strip(),
                                'marca'      : row.get('productos_marca','').strip(),
                                'sucursales' : {}
                            }
                        productos_out[ean]['sucursales'][skey] = {
                            'precio_lista'   : to_float(row.get('productos_precio_lista')),
                            'promo1_precio'  : to_float(row.get('productos_precio_unitario_promo1')),
                            'promo1_leyenda' : row.get('productos_leyenda_promo1','').strip() or None,
                            'promo2_precio'  : to_float(row.get('productos_precio_unitario_promo2')),
                            'promo2_leyenda' : row.get('productos_leyenda_promo2','').strip() or None,
                        }
                    nuevos = len(productos_out) - prod_antes
                    print(f', {nuevos} producto(s) nuevos')

            except Exception as e:
                print(f'\n   ⚠️  Error en {nombre_zip}: {e}')
                errores.append(f'{nombre_zip}: {e}')

    print(f'\n✅ Procesados {total} ZIPs')
    print(f'   Sucursales encontradas : {len(sucursales_out)}')
    print(f'   Productos encontrados  : {len(productos_out)}')
    if errores:
        print(f'   ⚠️  Errores ({len(errores)}): ' + ' | '.join(errores[:5]))
    return sucursales_out, productos_out

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    ahora = datetime.now(AR_TZ)
    print(f'🕐 Inicio: {ahora.strftime("%Y-%m-%d %H:%M")} (hora Argentina)')

    url     = obtener_url_zip()
    buf     = descargar_zip(url)
    suc, prod = procesar(buf)

    resultado = {
        'fecha'      : ahora.strftime('%Y-%m-%d'),
        'hora'       : ahora.strftime('%H:%M'),
        'sucursales' : suc,
        'productos'  : prod,
    }

    with open(SALIDA, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, separators=(',',':'))

    kb = os.path.getsize(SALIDA) / 1024
    print(f'💾 Guardado: {SALIDA} ({kb:.0f} KB)')
    print('🎉 ¡Listo!')

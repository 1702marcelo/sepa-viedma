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
def procesar(zip_externo_buf):
    sucursales_out = {}
    productos_out  = {}
    zips_procesados = 0
    zips_con_locales = 0

    with zipfile.ZipFile(zip_externo_buf) as zext:
        nombres = zext.namelist()
        zips_internos = [n for n in nombres if n.lower().endswith('.zip')]
        total = len(zips_internos)
        print(f'📂 ZIPs internos encontrados: {total}')

        for idx, nombre_zip in enumerate(zips_internos, 1):
            print(f'\r   Procesando {idx}/{total}: {nombre_zip[:50]}', end='', flush=True)
            try:
                zip_data = zext.read(nombre_zip)
                with zipfile.ZipFile(io.BytesIO(zip_data)) as zint:
                    archivos = {n.lower(): n for n in zint.namelist()}

                    # Leer sucursales
                    nombre_suc = archivos.get('sucursales.csv')
                    if not nombre_suc:
                        continue
                    suc_rows  = leer_csv_bytes(zint.read(nombre_suc))
                    locales   = {}
                    for row in suc_rows:
                        if es_local(row):
                            id_c = row.get('id_comercio','').strip()
                            id_b = row.get('id_bandera','').strip()
                            id_s = row.get('id_sucursal','').strip()
                            skey = f'{id_c}_{id_b}_{id_s}'

                            # Leer comercio.csv para nombre de bandera
                            nombre_com_f = archivos.get('comercio.csv')
                            bandera = ''
                            if nombre_com_f and skey not in sucursales_out:
                                try:
                                    com_rows = leer_csv_bytes(zint.read(nombre_com_f))
                                    for cr in com_rows:
                                        if cr.get('id_comercio','').strip() == id_c:
                                            bandera = cr.get('comercio_bandera_nombre',
                                                     cr.get('bandera_nombre','')).strip()
                                            break
                                except Exception:
                                    pass

                            locales[skey] = True
                            sucursales_out[skey] = {
                                'nombre'   : row.get('sucursales_nombre','').strip(),
                                'bandera'  : bandera,
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
                    zips_con_locales += 1

                    # Leer productos
                    nombre_prod = archivos.get('productos.csv')
                    if not nombre_prod:
                        continue
                    prod_rows = leer_csv_bytes(zint.read(nombre_prod))
                    for row in prod_rows:
                        id_c = row.get('id_comercio','').strip()
                        id_b = row.get('id_bandera','').strip()
                        id_s = row.get('id_sucursal','').strip()
                        skey = f'{id_c}_{id_b}_{id_s}'
                        if skey not in locales:
                            continue
                        ean = (row.get('productos_ean') or
                               row.get('id_producto','')).strip()
                        if not ean:
                            continue

                        def f(campo):
                            v = row.get(campo,'').strip()
                            try: return float(v) if v else None
                            except: return None

                        if ean not in productos_out:
                            productos_out[ean] = {
                                'descripcion': row.get('productos_descripcion','').strip(),
                                'marca'      : row.get('productos_marca','').strip(),
                                'sucursales' : {}
                            }
                        productos_out[ean]['sucursales'][skey] = {
                            'precio_lista'   : f('productos_precio_lista'),
                            'promo1_precio'  : f('productos_precio_unitario_promo1'),
                            'promo1_leyenda' : row.get('productos_leyenda_promo1','').strip() or None,
                            'promo2_precio'  : f('productos_precio_unitario_promo2'),
                            'promo2_leyenda' : row.get('productos_leyenda_promo2','').strip() or None,
                        }

            except Exception as e:
                pass  # ZIP interno corrupto → saltar

            zips_procesados += 1

    print(f'\n✅ Procesados {zips_procesados} ZIPs, {zips_con_locales} con sucursales locales.')
    print(f'   Sucursales encontradas : {len(sucursales_out)}')
    print(f'   Productos encontrados  : {len(productos_out)}')
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

"""
actualizar_precios.py
Descarga el ZIP del SEPA, filtra Viedma y Carmen de Patagones,
y genera precios_viedma_hoy.json en la misma carpeta.
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
PROVINCIAS       = {'AR-R', 'AR-B'}   # Río Negro y Buenos Aires (para Patagones)
API_CKAN         = 'https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios'
SALIDA           = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'precios_viedma_hoy.json')
EN_CI            = os.environ.get('CI') == 'true'
AR_TZ            = timezone(timedelta(hours=-3))

# ── Helpers CSV ────────────────────────────────────────────────────────────────
def leer_csv_bytes(data_bytes):
    """Lee CSV con separador pipe, maneja BOM y CRLF."""
    # utf-8-sig elimina el BOM automáticamente
    texto = data_bytes.decode('utf-8-sig', errors='replace')
    lineas = []
    for l in texto.splitlines():
        l = l.rstrip('\r')
        if l.strip() and not l.startswith('Última actualización'):
            lineas.append(l)
    if not lineas:
        return []
    return list(csv.DictReader(lineas, delimiter='|'))

def es_local(row):
    """Devuelve True si la sucursal pertenece a Viedma o Patagones."""
    cp   = (row.get('sucursales_codigo_postal') or '').strip()
    loc  = (row.get('sucursales_localidad')     or '').strip().lower()
    prov = (row.get('sucursales_provincia')     or '').strip()
    if cp in CODIGOS_POSTALES:
        return True
    if any(l in loc for l in LOCALIDADES):
        return True
    # Si la provincia es AR-R o AR-B, revisar localidad más ampliamente
    # (Evitar meter toda la provincia entera, solo usar como filtro extra)
    return False

# ── Obtener URL del ZIP más reciente ──────────────────────────────────────────
def obtener_url_zip():
    print('📡 Consultando API del gobierno...')
    r = requests.get(API_CKAN, timeout=30)
    r.raise_for_status()
    recursos = r.json()['result']['resources']

    for rec in sorted(recursos, key=lambda x: x.get('last_modified',''), reverse=True):
        url = rec.get('url','')
        if url.lower().endswith('.zip'):
            mod = rec.get('last_modified','desconocida')
            print(f'📦 ZIP encontrado: {url}')
            print(f'   Última modificación: {mod}')
            if mod:
                try:
                    fecha_rec = datetime.fromisoformat(
                        mod.replace('Z','+00:00')).astimezone(AR_TZ).date()
                    hoy = datetime.now(AR_TZ).date()
                    if fecha_rec < hoy:
                        print(f'⚠️  El archivo es del {fecha_rec} (hoy es {hoy}).')
                        if not EN_CI:
                            resp = input('¿Continuar igual? (s/n): ').strip().lower()
                            if resp != 's':
                                print('Cancelado.'); sys.exit(0)
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
    buf = io.BytesIO()
    bajas = 0
    for chunk in r.iter_content(65536):
        buf.write(chunk)
        bajas += len(chunk)
        if total:
            print(f'\r   {bajas*100//total}% — {bajas/1_048_576:.1f} MB', end='', flush=True)
    print(f'\r✅ Descarga completa: {bajas/1_048_576:.1f} MB          ')
    buf.seek(0)
    return buf

# ── Procesar ──────────────────────────────────────────────────────────────────
def procesar(zip_externo_buf):
    sucursales_out  = {}
    productos_out   = {}
    zips_procesados = 0
    zips_con_locales = 0

    with zipfile.ZipFile(zip_externo_buf) as zext:
        zips_internos = [n for n in zext.namelist() if n.lower().endswith('.zip')]
        total = len(zips_internos)
        print(f'📂 ZIPs internos encontrados: {total}')

        for idx, nombre_zip in enumerate(zips_internos, 1):
            print(f'\r   Procesando {idx}/{total}: {nombre_zip[:50]}', end='', flush=True)
            try:
                with zipfile.ZipFile(io.BytesIO(zext.read(nombre_zip))) as zint:
                    archivos = {n.lower(): n for n in zint.namelist()}

                    # ── Sucursales ──────────────────────────────────────────
                    fn_suc = archivos.get('sucursales.csv')
                    if not fn_suc:
                        continue
                    suc_rows = leer_csv_bytes(zint.read(fn_suc))
                    locales = {}

                    # Leer bandera desde comercio.csv (una vez por ZIP)
                    bandera_cache = {}
                    fn_com = archivos.get('comercio.csv')
                    if fn_com:
                        try:
                            for cr in leer_csv_bytes(zint.read(fn_com)):
                                id_c = cr.get('id_comercio','').strip()
                                id_b = cr.get('id_bandera','').strip()
                                k = f'{id_c}_{id_b}'
                                bandera_cache[k] = cr.get('comercio_bandera_nombre','').strip()
                        except Exception:
                            pass

                    for row in suc_rows:
                        if not es_local(row):
                            continue
                        id_c = row.get('id_comercio','').strip()
                        id_b = row.get('id_bandera','').strip()
                        id_s = row.get('id_sucursal','').strip()
                        skey = f'{id_c}_{id_b}_{id_s}'
                        locales[skey] = True

                        # Horarios — nombres reales del CSV
                        horarios = {
                            'lunes'    : row.get('sucursales_lunes_horario_atencion','').strip() or None,
                            'martes'   : row.get('sucursales_martes_horario_atencion','').strip() or None,
                            'miercoles': row.get('sucursales_miercoles_horario_atencion','').strip() or None,
                            'jueves'   : row.get('sucursales_jueves_horario_atencion','').strip() or None,
                            'viernes'  : row.get('sucursales_viernes_horario_atencion','').strip() or None,
                            'sabado'   : row.get('sucursales_sabado_horario_atencion','').strip() or None,
                            'domingo'  : row.get('sucursales_domingo_horario_atencion','').strip() or None,
                        }

                        sucursales_out[skey] = {
                            'nombre'   : row.get('sucursales_nombre','').strip(),
                            'bandera'  : bandera_cache.get(f'{id_c}_{id_b}',''),
                            'calle'    : row.get('sucursales_calle','').strip(),
                            'numero'   : row.get('sucursales_numero','').strip(),
                            'localidad': row.get('sucursales_localidad','').strip(),
                            'cp'       : row.get('sucursales_codigo_postal','').strip(),
                            'horarios' : horarios,
                        }

                    if not locales:
                        continue
                    zips_con_locales += 1

                    # ── Productos ───────────────────────────────────────────
                    fn_prod = archivos.get('productos.csv')
                    if not fn_prod:
                        continue
                    prod_rows = leer_csv_bytes(zint.read(fn_prod))

                    for row in prod_rows:
                        id_c = row.get('id_comercio','').strip()
                        id_b = row.get('id_bandera','').strip()
                        id_s = row.get('id_sucursal','').strip()
                        skey = f'{id_c}_{id_b}_{id_s}'
                        if skey not in locales:
                            continue

                        # EAN real está en id_producto, NO en productos_ean
                        ean = row.get('id_producto','').strip()
                        if not ean or not ean.isdigit():
                            continue

                        def f(campo):
                            v = row.get(campo,'').strip()
                            try:
                                fv = float(v)
                                return fv if fv > 0 else None
                            except:
                                return None

                        if ean not in productos_out:
                            productos_out[ean] = {
                                'descripcion': row.get('productos_descripcion','').strip(),
                                'marca'      : row.get('productos_marca','').strip(),
                                'sucursales' : {}
                            }

                        productos_out[ean]['sucursales'][skey] = {
                            'precio_lista'  : f('productos_precio_lista'),
                            'promo1_precio' : f('productos_precio_unitario_promo1'),
                            'promo1_leyenda': row.get('productos_leyenda_promo1','').strip() or None,
                            'promo2_precio' : f('productos_precio_unitario_promo2'),
                            'promo2_leyenda': row.get('productos_leyenda_promo2','').strip() or None,
                        }

            except Exception as e:
                print(f'\n   ⚠️  Error en {nombre_zip}: {e}')

            zips_procesados += 1

    print(f'\n✅ Procesados {zips_procesados} ZIPs, {zips_con_locales} con sucursales locales.')
    print(f'   Sucursales : {len(sucursales_out)}')
    print(f'   Productos  : {len(productos_out)}')
    return sucursales_out, productos_out

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    ahora = datetime.now(AR_TZ)
    print(f'🕐 Inicio: {ahora.strftime("%Y-%m-%d %H:%M")} (hora Argentina)')
    url           = obtener_url_zip()
    buf           = descargar_zip(url)
    suc, prod     = procesar(buf)
    resultado     = {
        'fecha'     : ahora.strftime('%Y-%m-%d'),
        'hora'      : ahora.strftime('%H:%M'),
        'sucursales': suc,
        'productos' : prod,
    }
    with open(SALIDA, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, separators=(',',':'))
    print(f'💾 Guardado: {SALIDA} ({os.path.getsize(SALIDA)/1024:.0f} KB)')
    print('🎉 ¡Listo!')

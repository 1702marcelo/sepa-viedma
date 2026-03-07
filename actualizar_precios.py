"""
actualizar_precios.py — SEPA Viedma / Carmen de Patagones
"""
import requests, zipfile, io, csv, json, os, sys
from datetime import datetime, timezone, timedelta

CODIGOS_POSTALES = {'8500', '8504'}
LOCALIDADES      = {'viedma', 'carmen de patagones', 'patagones'}
API_CKAN         = 'https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios'
SALIDA           = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'precios_viedma_hoy.json')
EN_CI            = os.environ.get('CI') == 'true'
AR_TZ            = timezone(timedelta(hours=-3))

def leer_csv_bytes(data_bytes):
    texto  = data_bytes.decode('utf-8', errors='replace')
    lineas = [l for l in texto.splitlines()
              if l.strip() and not l.startswith('Última actualización')]
    return list(csv.DictReader(lineas, delimiter='|'))

def es_local(row):
    cp  = (row.get('sucursales_codigo_postal') or '').strip()
    loc = (row.get('sucursales_localidad')     or '').strip().lower()
    return cp in CODIGOS_POSTALES or any(l in loc for l in LOCALIDADES)

def to_float(v):
    try: return float(str(v).strip()) if str(v).strip() else None
    except: return None

def obtener_url_zip():
    print('📡 Consultando API...')
    r = requests.get(API_CKAN, timeout=30); r.raise_for_status()
    for rec in sorted(r.json()['result']['resources'],
                      key=lambda x: x.get('last_modified',''), reverse=True):
        url = rec.get('url','')
        if url.lower().endswith('.zip'):
            mod = rec.get('last_modified','')
            print(f'📦 {url}\n   Modificado: {mod}')
            if mod:
                try:
                    fecha_rec = datetime.fromisoformat(mod.replace('Z','+00:00')).astimezone(AR_TZ).date()
                    hoy = datetime.now(AR_TZ).date()
                    if fecha_rec < hoy:
                        print(f'⚠️  Archivo del {fecha_rec} (hoy es {hoy})')
                        if not EN_CI:
                            if input('¿Continuar? (s/n): ').strip().lower() != 's':
                                sys.exit(0)
                        else:
                            print('   (Modo automático: continuando)')
                except Exception: pass
            return url
    raise RuntimeError('No se encontró ZIP en la API.')

def descargar_zip(url):
    print('⬇️  Descargando...')
    r = requests.get(url, stream=True, timeout=300); r.raise_for_status()
    total = int(r.headers.get('content-length', 0))
    buf = io.BytesIO(); bajas = 0
    for chunk in r.iter_content(65536):
        buf.write(chunk); bajas += len(chunk)
        if total:
            print(f'\r   {bajas*100//total}% — {bajas/1048576:.1f} MB', end='', flush=True)
    print(f'\r✅ {bajas/1048576:.1f} MB descargados          ')
    buf.seek(0); return buf

def procesar(zip_buf):
    sucursales_out = {}
    productos_out  = {}

    with zipfile.ZipFile(zip_buf) as zext:
        zips = [n for n in zext.namelist() if n.lower().endswith('.zip')]
        print(f'📂 {len(zips)} ZIPs internos')

        for idx, nz in enumerate(zips, 1):
            print(f'\r   [{idx}/{len(zips)}] {nz[:55]}', end='', flush=True)
            try:
                with zipfile.ZipFile(io.BytesIO(zext.read(nz))) as zi:
                    arch = {n.lower(): n for n in zi.namelist()}

                    # ── Bandera (nombre de la cadena) ──────────────────────
                    bandera = ''
                    if 'comercio.csv' in arch:
                        for cr in leer_csv_bytes(zi.read(arch['comercio.csv'])):
                            b = (cr.get('comercio_bandera_nombre') or
                                 cr.get('bandera_nombre') or '').strip()
                            if b: bandera = b; break

                    # ── Sucursales locales ─────────────────────────────────
                    if 'sucursales.csv' not in arch: continue
                    locales = {}   # skey → True
                    cb_map  = {}   # (id_comercio, id_bandera) → skey  [para fallback]

                    for row in leer_csv_bytes(zi.read(arch['sucursales.csv'])):
                        if not es_local(row): continue
                        ic = str(row.get('id_comercio','') or '').strip()
                        ib = str(row.get('id_bandera','')  or '').strip()
                        is_ = str(row.get('id_sucursal','') or '').strip()
                        sk  = f'{ic}_{ib}_{is_}'
                        locales[sk] = True
                        cb_map[(ic, ib)] = sk   # último gana (no importa cual)
                        sucursales_out[sk] = {
                            'nombre'   : row.get('sucursales_nombre','').strip(),
                            'bandera'  : bandera,
                            'calle'    : row.get('sucursales_calle','').strip(),
                            'numero'   : row.get('sucursales_numero','').strip(),
                            'localidad': row.get('sucursales_localidad','').strip(),
                            'cp'       : row.get('sucursales_codigo_postal','').strip(),
                            'horarios' : {d: row.get(f'sucursales_horario_{d}','').strip()
                                          for d in ['lunes','martes','miercoles','jueves',
                                                    'viernes','sabado','domingo']}
                        }

                    if not locales: continue
                    print(f' → {len(locales)} suc.', end='', flush=True)

                    # ── Productos ──────────────────────────────────────────
                    if 'productos.csv' not in arch:
                        print(' (sin productos.csv)'); continue

                    antes = len(productos_out)
                    prod_filas = leer_csv_bytes(zi.read(arch['productos.csv']))
                    # DEBUG: mostrar primeras filas para ZIPs con sucursales locales
                    if prod_filas:
                        cols = list(prod_filas[0].keys())
                        print(f'\n      cols_prod: {cols[:6]}')
                        for fila_debug in prod_filas[:3]:
                            ic_d  = str(fila_debug.get('id_comercio','?') or '?').strip()
                            ib_d  = str(fila_debug.get('id_bandera','?')  or '?').strip()
                            is_d  = str(fila_debug.get('id_sucursal','?') or '?').strip()
                            ean_d = str(fila_debug.get('productos_ean', fila_debug.get('id_producto','?')) or '?').strip()
                            print(f'      fila: ic={ic_d!r} ib={ib_d!r} is={is_d!r} ean={ean_d!r}')
                        print(f'      cb_map keys: {list(cb_map.keys())}')
                    for row in prod_filas:
                        ic  = str(row.get('id_comercio','') or '').strip()
                        ib  = str(row.get('id_bandera','')  or '').strip()
                        is_ = str(row.get('id_sucursal','') or '').strip()
                        sk_exact = f'{ic}_{ib}_{is_}'

                        # Match exacto primero; si no, mismo comercio+bandera
                        if sk_exact in locales:
                            sk_usar = sk_exact
                        elif (ic, ib) in cb_map:
                            sk_usar = cb_map[(ic, ib)]
                        else:
                            continue   # este producto no es de un comercio local

                        ean = (row.get('productos_ean') or row.get('id_producto') or '').strip()
                        if not ean: continue

                        if ean not in productos_out:
                            productos_out[ean] = {
                                'descripcion': row.get('productos_descripcion','').strip(),
                                'marca'      : row.get('productos_marca','').strip(),
                                'sucursales' : {}
                            }
                        productos_out[ean]['sucursales'][sk_usar] = {
                            'precio_lista'   : to_float(row.get('productos_precio_lista')),
                            'promo1_precio'  : to_float(row.get('productos_precio_unitario_promo1')),
                            'promo1_leyenda' : row.get('productos_leyenda_promo1','').strip() or None,
                            'promo2_precio'  : to_float(row.get('productos_precio_unitario_promo2')),
                            'promo2_leyenda' : row.get('productos_leyenda_promo2','').strip() or None,
                        }

                    nuevos = len(productos_out) - antes
                    print(f', {nuevos} prod. nuevos')

            except Exception as e:
                print(f'\n   ⚠️  Error en {nz}: {e}')

    print(f'\n✅ Sucursales: {len(sucursales_out)} | Productos: {len(productos_out)}')
    return sucursales_out, productos_out

if __name__ == '__main__':
    ahora = datetime.now(AR_TZ)
    print(f'🕐 {ahora.strftime("%Y-%m-%d %H:%M")} hora Argentina')
    url = obtener_url_zip()
    buf = descargar_zip(url)
    suc, prod = procesar(buf)
    resultado = {'fecha': ahora.strftime('%Y-%m-%d'),
                 'hora' : ahora.strftime('%H:%M'),
                 'sucursales': suc, 'productos': prod}
    with open(SALIDA, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, separators=(',',':'))
    print(f'💾 {SALIDA} ({os.path.getsize(SALIDA)/1024:.0f} KB)\n🎉 ¡Listo!')

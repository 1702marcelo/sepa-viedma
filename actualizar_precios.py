"""
actualizar_precios.py — SEPA Viedma / Carmen de Patagones
"""
import requests, zipfile, io, csv, json, os, sys
from datetime import datetime, timezone, timedelta

CODIGOS_POSTALES = {'8500', '8504'}
LOCALIDADES      = {'viedma', 'carmen de patagones', 'patagones'}
API_CKAN  = 'https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios'
SALIDA    = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'precios_viedma_hoy.json')
EN_CI     = os.environ.get('CI') == 'true'
AR_TZ     = timezone(timedelta(hours=-3))

def sg(row, key):
    """get seguro: nunca lanza NoneType error"""
    return (row.get(key) or '').strip()

def leer_csv_bytes(data_bytes):
    texto  = data_bytes.decode('utf-8-sig', errors='replace')
    lineas = [l.rstrip('\r') for l in texto.splitlines()
              if l.strip() and not l.startswith('Última actualización')]
    return list(csv.DictReader(lineas, delimiter='|')) if lineas else []

def es_local(row):
    cp  = sg(row, 'sucursales_codigo_postal')
    loc = sg(row, 'sucursales_localidad').lower()
    return cp in CODIGOS_POSTALES or any(l in loc for l in LOCALIDADES)

def buscar_archivo(nombres_zip, sufijo):
    sufijo = sufijo.lower()
    for n in nombres_zip:
        if n.lower().endswith('/' + sufijo) or n.lower() == sufijo:
            return n
    return None

def obtener_url_zip():
    print('📡 Consultando API del gobierno...')
    r = requests.get(API_CKAN, timeout=30); r.raise_for_status()
    recursos = r.json()['result']['resources']
    for rec in sorted(recursos, key=lambda x: x.get('last_modified',''), reverse=True):
        url = rec.get('url','')
        if url.lower().endswith('.zip'):
            mod = rec.get('last_modified','')
            print(f'📦 ZIP: {url}\n   Modificado: {mod}')
            if mod:
                try:
                    fecha_rec = datetime.fromisoformat(mod.replace('Z','+00:00')).astimezone(AR_TZ).date()
                    hoy = datetime.now(AR_TZ).date()
                    if fecha_rec < hoy:
                        print(f'⚠️  Archivo del {fecha_rec}, hoy es {hoy}.')
                        if not EN_CI:
                            if input('¿Continuar? (s/n): ').strip().lower() != 's':
                                sys.exit(0)
                        else:
                            print('   (CI: continuando igual)')
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
        if total: print(f'\r   {bajas*100//total}% — {bajas/1_048_576:.1f} MB', end='', flush=True)
    print(f'\r✅ Descarga: {bajas/1_048_576:.1f} MB          ')
    buf.seek(0); return buf

def procesar(zip_buf):
    sucursales_out = {}; productos_out = {}; zips_ok = 0
    errores = []

    with zipfile.ZipFile(zip_buf) as zext:
        zips_int = [n for n in zext.namelist() if n.lower().endswith('.zip')]
        print(f'📂 ZIPs internos: {len(zips_int)}')

        for idx, nzip in enumerate(zips_int, 1):
            print(f'\r   {idx}/{len(zips_int)}: {nzip[:55]}', end='', flush=True)
            try:
                with zipfile.ZipFile(io.BytesIO(zext.read(nzip))) as zint:
                    nombres = zint.namelist()
                    fn_suc  = buscar_archivo(nombres, 'sucursales.csv')
                    fn_prod = buscar_archivo(nombres, 'productos.csv')
                    fn_com  = buscar_archivo(nombres, 'comercio.csv')
                    if not fn_suc or not fn_prod: continue

                    # Banderas
                    bandera_cache = {}
                    if fn_com:
                        for cr in leer_csv_bytes(zint.read(fn_com)):
                            try:
                                k = f"{sg(cr,'id_comercio')}_{sg(cr,'id_bandera')}"
                                bandera_cache[k] = sg(cr, 'comercio_bandera_nombre')
                            except Exception: pass

                    # Sucursales locales
                    locales = {}
                    for row in leer_csv_bytes(zint.read(fn_suc)):
                        try:
                            if not es_local(row): continue
                            id_c = sg(row, 'id_comercio')
                            id_b = sg(row, 'id_bandera')
                            id_s = sg(row, 'id_sucursal')
                            if not id_c or not id_s: continue
                            skey = f'{id_c}_{id_b}_{id_s}'
                            locales[skey] = True
                            sucursales_out[skey] = {
                                'nombre'   : sg(row, 'sucursales_nombre'),
                                'bandera'  : bandera_cache.get(f'{id_c}_{id_b}', ''),
                                'calle'    : sg(row, 'sucursales_calle'),
                                'numero'   : sg(row, 'sucursales_numero'),
                                'localidad': sg(row, 'sucursales_localidad'),
                                'cp'       : sg(row, 'sucursales_codigo_postal'),
                                'horarios' : {
                                    d: sg(row, f'sucursales_{d}_horario_atencion') or None
                                    for d in ['lunes','martes','miercoles','jueves','viernes','sabado','domingo']
                                }
                            }
                        except Exception: continue

                    if not locales: continue
                    zips_ok += 1

                    # Productos
                    for row in leer_csv_bytes(zint.read(fn_prod)):
                        try:
                            id_c = sg(row, 'id_comercio')
                            id_b = sg(row, 'id_bandera')
                            id_s = sg(row, 'id_sucursal')
                            if not id_c or not id_s: continue
                            skey = f'{id_c}_{id_b}_{id_s}'
                            if skey not in locales: continue
                            ean = sg(row, 'id_producto')
                            if not ean or not ean.isdigit(): continue

                            def f(c):
                                v = (row.get(c) or '').strip()
                                try: fv = float(v); return fv if fv > 0 else None
                                except: return None

                            if ean not in productos_out:
                                productos_out[ean] = {
                                    'descripcion': sg(row, 'productos_descripcion'),
                                    'marca'      : sg(row, 'productos_marca'),
                                    'sucursales' : {}
                                }
                            productos_out[ean]['sucursales'][skey] = {
                                'precio_lista'  : f('productos_precio_lista'),
                                'promo1_precio' : f('productos_precio_unitario_promo1'),
                                'promo1_leyenda': sg(row, 'productos_leyenda_promo1') or None,
                                'promo2_precio' : f('productos_precio_unitario_promo2'),
                                'promo2_leyenda': sg(row, 'productos_leyenda_promo2') or None,
                            }
                        except Exception: continue

            except Exception as e:
                errores.append(f'{nzip}: {e}')
                print(f'\n   ⚠️  Error en {nzip}: {e}')

    print(f'\n✅ {zips_ok} ZIPs | {len(sucursales_out)} sucursales | {len(productos_out)} productos')
    if errores:
        print(f'⚠️  {len(errores)} ZIPs con errores (saltados): {errores}')
    return sucursales_out, productos_out

if __name__ == '__main__':
    ahora = datetime.now(AR_TZ)
    print(f'🕐 {ahora.strftime("%Y-%m-%d %H:%M")} (hora AR)')
    url       = obtener_url_zip()
    buf       = descargar_zip(url)
    suc, prod = procesar(buf)
    out = {'fecha': ahora.strftime('%Y-%m-%d'), 'hora': ahora.strftime('%H:%M'),
           'sucursales': suc, 'productos': prod}
    with open(SALIDA, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',',':'))
    print(f'💾 {SALIDA} ({os.path.getsize(SALIDA)/1024:.0f} KB)')
    print('🎉 ¡Listo!')

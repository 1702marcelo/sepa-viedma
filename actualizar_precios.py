"""
actualizar_precios.py — SEPA Viedma / Carmen de Patagones
Incluye descarga de imágenes de productos por EAN.
"""
import requests, zipfile, io, csv, json, os, sys, time, re, urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

CODIGOS_POSTALES = {'8500', '8504'}
LOCALIDADES      = {'viedma', 'carmen de patagones', 'patagones'}
API_CKAN  = 'https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios'
BASE_DIR  = Path(os.path.dirname(os.path.abspath(__file__)))
SALIDA    = BASE_DIR / 'precios_viedma_hoy.json'
IMG_DIR   = BASE_DIR / 'img'
EN_CI     = os.environ.get('CI') == 'true'
AR_TZ     = timezone(timedelta(hours=-3))

HEADERS_WEB = {
    'User-Agent': 'Mozilla/5.0 (Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
    'Accept-Language': 'es-AR,es;q=0.9,en;q=0.8',
}

# ── Helpers CSV ────────────────────────────────────────────────────────────────
def leer_csv_bytes(data_bytes):
    texto  = data_bytes.decode('utf-8-sig', errors='replace')
    lineas = [l.rstrip('\r') for l in texto.splitlines()
              if l.strip() and not l.startswith('Última actualización')]
    return list(csv.DictReader(lineas, delimiter='|')) if lineas else []

def es_local(row):
    cp  = (row.get('sucursales_codigo_postal') or '').strip()
    loc = (row.get('sucursales_localidad')     or '').strip().lower()
    return cp in CODIGOS_POSTALES or any(l in loc for l in LOCALIDADES)

def buscar_archivo(nombres_zip, sufijo):
    sufijo = sufijo.lower()
    for n in nombres_zip:
        if n.lower().endswith('/' + sufijo) or n.lower() == sufijo:
            return n
    return None

# ── API SEPA ───────────────────────────────────────────────────────────────────
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
    print('⬇️  Descargando ZIP...')
    r = requests.get(url, stream=True, timeout=300); r.raise_for_status()
    total = int(r.headers.get('content-length', 0))
    buf = io.BytesIO(); bajas = 0
    for chunk in r.iter_content(65536):
        buf.write(chunk); bajas += len(chunk)
        if total: print(f'\r   {bajas*100//total}% — {bajas/1_048_576:.1f} MB', end='', flush=True)
    print(f'\r✅ Descarga: {bajas/1_048_576:.1f} MB          ')
    buf.seek(0); return buf

def procesar(zip_buf):
    sucursales_out = {}; productos_out = {}
    zips_ok = 0
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

                    bandera_cache = {}
                    if fn_com:
                        try:
                            for cr in leer_csv_bytes(zint.read(fn_com)):
                                k = f"{cr.get('id_comercio','').strip()}_{cr.get('id_bandera','').strip()}"
                                bandera_cache[k] = cr.get('comercio_bandera_nombre','').strip()
                        except Exception: pass

                    locales = {}
                    for row in leer_csv_bytes(zint.read(fn_suc)):
                        if not es_local(row): continue
                        id_c = row.get('id_comercio','').strip()
                        id_b = row.get('id_bandera','').strip()
                        id_s = row.get('id_sucursal','').strip()
                        skey = f'{id_c}_{id_b}_{id_s}'
                        locales[skey] = True
                        sucursales_out[skey] = {
                            'nombre'   : row.get('sucursales_nombre','').strip(),
                            'bandera'  : bandera_cache.get(f'{id_c}_{id_b}',''),
                            'calle'    : row.get('sucursales_calle','').strip(),
                            'numero'   : row.get('sucursales_numero','').strip(),
                            'localidad': row.get('sucursales_localidad','').strip(),
                            'cp'       : row.get('sucursales_codigo_postal','').strip(),
                            'horarios' : {
                                d: row.get(f'sucursales_{d}_horario_atencion','').strip() or None
                                for d in ['lunes','martes','miercoles','jueves','viernes','sabado','domingo']
                            }
                        }
                    if not locales: continue
                    zips_ok += 1

                    for row in leer_csv_bytes(zint.read(fn_prod)):
                        id_c = row.get('id_comercio','').strip()
                        id_b = row.get('id_bandera','').strip()
                        id_s = row.get('id_sucursal','').strip()
                        skey = f'{id_c}_{id_b}_{id_s}'
                        if skey not in locales: continue
                        ean = row.get('id_producto','').strip()
                        if not ean or not ean.isdigit(): continue

                        def f(c):
                            v = row.get(c,'').strip()
                            try: fv = float(v); return fv if fv > 0 else None
                            except: return None

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
                print(f'\n   ⚠️  Error en {nzip}: {e}')

    print(f'\n✅ {zips_ok} ZIPs | {len(sucursales_out)} sucursales | {len(productos_out)} productos')
    return sucursales_out, productos_out

# ── IMÁGENES ───────────────────────────────────────────────────────────────────
def ean_a_path_off(ean):
    """Convierte EAN en path de Open Food Facts."""
    e = str(ean).zfill(13)
    return f"{e[0:3]}/{e[3:6]}/{e[6:9]}/{e[9:13]}"

def intentar_off(ean, sesion):
    """Intenta obtener imagen de Open Food Facts."""
    path = ean_a_path_off(ean)
    # Probar variantes de nombre de imagen
    for nombre in ['front_es', 'front', 'front_world', '1']:
        url = f"https://images.openfoodfacts.org/images/products/{path}/{nombre}.200.jpg"
        try:
            r = sesion.get(url, timeout=8, allow_redirects=True)
            if r.status_code == 200 and len(r.content) > 2000:  # mínimo 2KB = imagen real
                return r.content, url
        except Exception:
            pass
    return None, None

def intentar_duckduckgo(ean, descripcion, marca, sesion):
    """Busca imagen en DuckDuckGo usando EAN + descripción."""
    # Consulta: EAN primero, luego nombre del producto
    queries = [
        f"{ean} producto",
        f"{descripcion} {marca}".strip(),
    ]
    for query in queries:
        try:
            q_enc = urllib.parse.quote(query)
            # Paso 1: obtener token vqd de DuckDuckGo
            r0 = sesion.get(
                f"https://duckduckgo.com/?q={q_enc}&ia=images",
                headers={**HEADERS_WEB, 'Referer': 'https://duckduckgo.com/'},
                timeout=10
            )
            # Extraer vqd del HTML
            m = re.search(r'vqd=([0-9-]+)', r0.text)
            if not m:
                continue
            vqd = m.group(1)

            # Paso 2: llamar a la API de imágenes
            r1 = sesion.get(
                'https://duckduckgo.com/i.js',
                params={'l':'es-ar', 'o':'json', 'q':query, 'vqd':vqd, 'f':',,,,,', 'p':'1'},
                headers={**HEADERS_WEB, 'Referer': f'https://duckduckgo.com/?q={q_enc}&ia=images'},
                timeout=10
            )
            if r1.status_code != 200:
                continue
            datos = r1.json()
            resultados = datos.get('results', [])
            if not resultados:
                continue

            # Intentar las primeras 3 imágenes
            for res in resultados[:3]:
                img_url = res.get('image', '')
                if not img_url:
                    continue
                try:
                    r2 = sesion.get(img_url, timeout=8, headers=HEADERS_WEB)
                    if r2.status_code == 200 and len(r2.content) > 3000:
                        ct = r2.headers.get('content-type','')
                        if 'image' in ct or img_url.lower().endswith(('.jpg','.jpeg','.png','.webp')):
                            return r2.content, img_url
                except Exception:
                    continue

            time.sleep(0.5)
        except Exception:
            continue
    return None, None

def redimensionar_imagen(datos_bytes, max_px=200):
    """Redimensiona imagen a max_px usando Pillow si está disponible."""
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(datos_bytes))
        img = img.convert('RGB')
        w, h = img.size
        if max(w, h) > max_px:
            factor = max_px / max(w, h)
            img = img.resize((int(w*factor), int(h*factor)), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, 'JPEG', quality=82, optimize=True)
        return out.getvalue()
    except Exception:
        return datos_bytes  # Si Pillow falla, devolver original

def descargar_imagenes(productos, img_dir):
    """Descarga imágenes para todos los EANs que no tengan imagen guardada."""
    img_dir.mkdir(exist_ok=True)
    eans = list(productos.keys())
    pendientes = [e for e in eans if not (img_dir / f'{e}.jpg').exists()]

    if not pendientes:
        print(f'🖼️  Imágenes: todas ya en caché ({len(eans)} productos).')
        return

    print(f'🖼️  Descargando imágenes: {len(pendientes)} nuevas de {len(eans)} totales...')
    ok_off = 0; ok_ddg = 0; falla = 0

    sesion = requests.Session()
    sesion.headers.update(HEADERS_WEB)

    for i, ean in enumerate(pendientes, 1):
        prod = productos[ean]
        desc = prod.get('descripcion','')
        marca = prod.get('marca','')
        dest = img_dir / f'{ean}.jpg'

        # Indicador de progreso
        print(f'\r   [{i}/{len(pendientes)}] {ean} {desc[:30]:<30}', end='', flush=True)

        # Intentar Open Food Facts primero
        datos, fuente = intentar_off(ean, sesion)

        if datos:
            ok_off += 1
        else:
            # Fallback: DuckDuckGo
            time.sleep(0.3)  # respetar rate limits
            datos, fuente = intentar_duckduckgo(ean, desc, marca, sesion)
            if datos:
                ok_ddg += 1
            else:
                falla += 1

        if datos:
            try:
                datos_final = redimensionar_imagen(datos)
                dest.write_bytes(datos_final)
            except Exception as e:
                print(f'\n   ⚠️  Error guardando {ean}: {e}')

        # Pausa corta para no saturar
        if i % 10 == 0:
            time.sleep(1)

    print(f'\n   ✅ OFF:{ok_off}  DDG:{ok_ddg}  Sin imagen:{falla}')

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    ahora = datetime.now(AR_TZ)
    print(f'🕐 {ahora.strftime("%Y-%m-%d %H:%M")} (hora AR)')

    url       = obtener_url_zip()
    buf       = descargar_zip(url)
    suc, prod = procesar(buf)

    # Descargar imágenes para los EANs nuevos
    descargar_imagenes(prod, IMG_DIR)

    out = {
        'fecha'     : ahora.strftime('%Y-%m-%d'),
        'hora'      : ahora.strftime('%H:%M'),
        'sucursales': suc,
        'productos' : prod
    }
    with open(SALIDA, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',',':'))

    print(f'💾 {SALIDA} ({SALIDA.stat().st_size/1024:.0f} KB)')
    n_imgs = len(list(IMG_DIR.glob('*.jpg')))
    print(f'🖼️  {n_imgs} imágenes en img/')
    print('🎉 ¡Listo!')

# 🛒 Precios Viedma — SEPA

App para consultar precios del SEPA filtrados para **Viedma y Carmen de Patagones**.  
Funciona 100% desde el celular: los precios se actualizan solos todos los días.

---

## ⚡ Configuración inicial (una sola vez, desde el celular)

### Paso 1 — Crear cuenta en GitHub
1. Abrí [github.com](https://github.com) en el navegador del celular
2. Tocá **Sign up** y creá una cuenta gratuita
3. Verificá el email

### Paso 2 — Crear el repositorio
1. Tocá el **+** (arriba a la derecha) → **New repository**
2. Nombre: `sepa-viedma` (o cualquier nombre)
3. Marcá **Public** ✅ (necesario para GitHub Pages gratis)
4. Tocá **Create repository**

### Paso 3 — Subir los archivos
En la página del repositorio vacío:
1. Tocá **uploading an existing file**
2. Subí estos archivos (uno por uno o todos juntos):
   - `index.html`
   - `actualizar_precios.py`
   - `precios_viedma_hoy.json`
3. Tocá **Commit changes**

### Paso 4 — Subir el workflow de GitHub Actions
1. En el repositorio, tocá el **+** para crear archivo
2. En el nombre escribí exactamente: `.github/workflows/actualizar.yml`
   (GitHub creará las carpetas automáticamente)
3. Pegá el contenido del archivo `actualizar.yml`
4. Tocá **Commit changes**

### Paso 5 — Activar GitHub Pages
1. En el repositorio, tocá **Settings** (⚙️)
2. En el menú izquierdo: **Pages**
3. En *Source*: seleccioná **Deploy from a branch**
4. Branch: **main**, carpeta: **/ (root)**
5. Tocá **Save**
6. Esperá 1-2 minutos y aparecerá tu URL:  
   `https://TU-USUARIO.github.io/sepa-viedma/`

### Paso 6 — Primer actualización de precios
1. En el repositorio, tocá la pestaña **Actions**
2. En la lista izquierda: **Actualizar Precios SEPA**
3. Tocá **Run workflow** → **Run workflow** (botón verde)
4. Esperá ~10-20 minutos (descarga el ZIP del gobierno)
5. Cuando el círculo se ponga verde ✅, recargá la app

---

## 🔄 Actualización automática

El script corre **todos los días a las 13:00 hora Argentina** automáticamente.  
No necesitás hacer nada.

## 🔄 Actualización manual desde el celular

Si querés actualizar antes de la hora programada:
1. Entrá a tu repositorio en GitHub
2. Pestaña **Actions** → **Actualizar Precios SEPA**
3. **Run workflow** → **Run workflow**

---

## 🔑 API Key de OpenAI (para reconocimiento por foto)

La API key ya está incluida en el código. Si necesitás cambiarla:
1. Abrí la app → tocá **⚙️**
2. Ingresá la nueva API key
3. (Opcional) También podés editar `index.html` directamente en GitHub

---

## 📁 Archivos del repositorio

| Archivo | Descripción |
|---|---|
| `index.html` | La app web (se abre desde el celular) |
| `actualizar_precios.py` | Script que descarga y procesa el SEPA |
| `precios_viedma_hoy.json` | Datos de precios (se actualiza automáticamente) |
| `.github/workflows/actualizar.yml` | Programación de la actualización automática |

---

## ❓ Preguntas frecuentes

**¿Cuándo se publican los datos del gobierno?**  
Generalmente después de las 12:00 hora Argentina. El script está programado para las 13:00 para asegurarse de que estén disponibles.

**¿Qué pasa si el workflow falla?**  
Tocá el círculo rojo en la pestaña Actions para ver el error. Lo más común es que el gobierno no haya publicado el ZIP todavía.

**¿Funciona sin internet?**  
No, necesitás internet para abrir la app. Los datos sí están guardados en GitHub.

**¿Puedo compartir la URL con alguien más?**  
Sí, cualquiera con la URL puede usar la app.

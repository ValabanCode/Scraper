# -*- coding: utf-8 -*-
import csv
import os
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains

# --- CONFIGURACIÓN ---
BASE_URL = "https://www.euromoto85.com"
OUTPUT_FILE = "repuestos_motos_completo.csv"
TASKS_FILE = "lista_de_tareas_completa.csv"
LOG_FILE = "scraper_log.txt"
START_AFTER_BRAND = AJP  # Para reanudar desde una marca específica
MAX_RETRIES = 3
MAX_RECOVERY_ATTEMPTS = 3  # Intentos de recuperación cuando se bugea
DELAY_BETWEEN_REQUESTS = 2

# CONFIGURACIÓN DE RESET
FORCE_FRESH_START = False  # True para empezar con CSV limpio
SKIP_PHASE_1 = True  # True para saltar la creación de tareas y usar archivo existente

# --- FUNCIONES DE LOGGING ---
def log_message(message):
    """Registra mensajes en archivo de log y consola."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + "\n")
# <--- AÑADE ESTA NUEVA FUNCIÓN ---
def guardar_registro_csv(registro, filename):
    """
    Guarda una única fila en el CSV, manejando el header de forma segura.
    Abre y cierra el archivo en cada llamada para garantizar la escritura.
    """
    try:
        file_exists = os.path.exists(filename)
        needs_header = not file_exists or os.path.getsize(filename) == 0

        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            if needs_header:
                header = [
                    'TIPO', 'MARCA', 'MODELO', 'CC', 'AÑO', 'URL GENERAL',
                    'Producto', 'Marca Producto', 'Referencia',
                    'Referencia MEIWA', 'Referencia HIFLO', 'URL DEL PRODUCTO'
                ]
                writer.writerow(header)
            writer.writerow(registro)
    except Exception as e:
        log_message(f"‼️ ERROR CRÍTICO AL GUARDAR EN CSV: {e}")
# --- NUEVAS FUNCIONES PARA MANEJAR PRODUCTOS POR AÑO ---
def crear_clave_unica(url_producto, datos_moto):
    """Crea una clave única que incluye el contexto del año/modelo"""
    return f"{url_producto}|{datos_moto['marca_text']}|{datos_moto['modelo_parseado']}|{datos_moto['anio']}"

def leer_registros_procesados(filename):
    """Lee el CSV existente y devuelve conjunto de claves únicas procesadas"""
    processed_keys = set()
    if not os.path.exists(filename):
        return processed_keys
    
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader, None)
            if header:
                # Encontrar índices de las columnas necesarias
                try:
                    url_index = header.index('URL DEL PRODUCTO')
                    marca_index = header.index('MARCA')
                    modelo_index = header.index('MODELO') 
                    anio_index = header.index('AÑO')
                    
                    for row in reader:
                        if len(row) > max(url_index, marca_index, modelo_index, anio_index):
                            # Crear clave única con contexto
                            clave_unica = f"{row[url_index]}|{row[marca_index]}|{row[modelo_index]}|{row[anio_index]}"
                            processed_keys.add(clave_unica)
                except ValueError as e:
                    log_message(f"Error encontrando columnas en CSV: {e}")
                    return set()
        
        log_message(f"Se encontraron {len(processed_keys)} registros únicos ya procesados")
    except Exception as e:
        log_message(f"Error leyendo registros procesados: {e}")
    
    return processed_keys

# --- FUNCIONES DE AYUDA ---
def configurar_driver():
    """Configura e inicia el navegador Chrome con Selenium (versión para servidor)."""
    options = webdriver.ChromeOptions()
    
    options.add_argument('--headless')
    options.add_argument(f'--user-data-dir=/tmp/chrome-session-{os.getpid()}')
    options.add_argument('--log-level=3')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        return driver
    except Exception as e:
        log_message(f"Error al iniciar Selenium: {e}")
        return None

def reiniciar_selectores(driver):
    """Reinicia todos los selectores a su estado inicial."""
    try:
        log_message("    🔄 Reiniciando selectores...")
        driver.get(BASE_URL)
        time.sleep(3)
        
        # Esperar a que los selectores estén disponibles
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, 'itipo')))
        
        # Verificar que todos los selectores estén en estado inicial
        selectors_ids = ['itipo', 'imarca', 'icc', 'imodel']
        for selector_id in selectors_ids:
            try:
                select_element = driver.find_element(By.ID, selector_id)
                select_obj = Select(select_element)
                if select_obj.first_selected_option.get_attribute('value') not in ["-1", "", "0"]:
                    # Si no está en valor por defecto, resetear
                    select_obj.select_by_index(0)
                    time.sleep(0.5)
            except Exception as e:
                log_message(f"    ⚠️ Error reseteando selector {selector_id}: {e}")
        
        log_message("    ✅ Selectores reiniciados correctamente")
        return True
        
    except Exception as e:
        log_message(f"    ❌ Error reiniciando selectores: {e}")
        return False

def verificar_estado_selector(driver, locator, descripcion="selector"):
    """Verifica si un selector está en buen estado y tiene opciones válidas."""
    try:
        element = driver.find_element(*locator)
        select_obj = Select(element)
        opciones_validas = [opt for opt in select_obj.options 
                          if opt.get_attribute('value') not in ["-1", "", "0"] 
                          and opt.text.strip() not in ["- Seleccionar -", ""]]
        
        log_message(f"    🔍 {descripcion}: {len(opciones_validas)} opciones válidas")
        return len(opciones_validas) > 0
        
    except Exception as e:
        log_message(f"    ❌ Error verificando {descripcion}: {e}")
        return False

def safe_wait_and_click(driver, locator, timeout=10):
    """Función segura para hacer clic en elementos."""
    try:
        wait = WebDriverWait(driver, timeout)
        element = wait.until(EC.element_to_be_clickable(locator))
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        time.sleep(0.5)
        element.click()
        return True
    except Exception as e:
        log_message(f"Error al hacer clic en elemento {locator}: {e}")
        return False

def obtener_opciones_desplegable_seguro(driver, locator, timeout=15, max_recovery_attempts=MAX_RECOVERY_ATTEMPTS):
    """Obtiene todas las opciones válidas de un menú desplegable con recuperación de errores."""
    
    for recovery_attempt in range(max_recovery_attempts):
        for intento in range(MAX_RETRIES):
            try:
                wait = WebDriverWait(driver, timeout)
                desplegable_element = wait.until(EC.presence_of_element_located(locator))
                select = Select(desplegable_element)
                opciones = []
                
                for option in select.options:
                    value = option.get_attribute('value')
                    text = option.text.strip()
                    if value and value not in ["-1", "", "0"] and text and text != "- Seleccionar -":
                        opciones.append({'value': value, 'text': text})
                
                if opciones:  # Si encontramos opciones válidas
                    log_message(f"Encontradas {len(opciones)} opciones válidas en {locator}")
                    return opciones
                else:
                    log_message(f"⚠️ No se encontraron opciones válidas en {locator}")
                    if recovery_attempt < max_recovery_attempts - 1:
                        log_message(f"    🔄 Intento de recuperación {recovery_attempt + 1}/{max_recovery_attempts}")
                        if reiniciar_selectores(driver):
                            break  # Salir del bucle de intentos y probar recovery
                    return []
                
            except StaleElementReferenceException:
                log_message(f"Elemento 'stale' detectado. Reintentando... ({MAX_RETRIES - intento} intentos restantes)")
                time.sleep(1)
            except Exception as e:
                log_message(f"Error obteniendo opciones del desplegable {locator}: {e}")
                if intento == MAX_RETRIES - 1:
                    if recovery_attempt < max_recovery_attempts - 1:
                        log_message(f"    🔄 Intento de recuperación {recovery_attempt + 1}/{max_recovery_attempts}")
                        if reiniciar_selectores(driver):
                            break
                    else:
                        return []
                time.sleep(1)
    
    return []

def seleccionar_opcion_segura_con_recuperacion(driver, select_locator, option_value, next_select_locator=None, descripcion="opción"):
    """Selecciona una opción de forma segura con recuperación ante errores."""
    
    for recovery_attempt in range(MAX_RECOVERY_ATTEMPTS):
        for intento in range(MAX_RETRIES):
            try:
                wait = WebDriverWait(driver, 20)
                select_element = wait.until(EC.element_to_be_clickable(select_locator))
                select_obj = Select(select_element)
                
                # Intentar seleccionar por valor primero, luego por texto
                try:
                    select_obj.select_by_value(option_value)
                except:
                    select_obj.select_by_visible_text(option_value)
                
                time.sleep(DELAY_BETWEEN_REQUESTS)
                
                # Si hay un siguiente select, esperar a que se actualice y verificar
                if next_select_locator:
                    wait.until(EC.presence_of_element_located(next_select_locator))
                    
                    # Verificar que el siguiente selector tiene opciones válidas
                    time.sleep(1)  # Dar tiempo extra para que carguen las opciones
                    
                    if not verificar_estado_selector(driver, next_select_locator, f"siguiente selector después de {descripcion}"):
                        log_message(f"⚠️ El siguiente selector no tiene opciones válidas después de seleccionar {descripcion}: {option_value}")
                        
                        if recovery_attempt < MAX_RECOVERY_ATTEMPTS - 1:
                            log_message(f"🔄 Intento de recuperación {recovery_attempt + 1}/{MAX_RECOVERY_ATTEMPTS}")
                            if reiniciar_selectores(driver):
                                break  # Salir del bucle de intentos y probar recovery
                        return False
                
                log_message(f"✅ Seleccionado correctamente {descripcion}: {option_value}")
                return True
                
            except Exception as e:
                log_message(f"Error seleccionando {descripcion} {option_value} (intento {intento + 1}): {e}")
                if intento < MAX_RETRIES - 1:
                    time.sleep(2)
                elif recovery_attempt < MAX_RECOVERY_ATTEMPTS - 1:
                    log_message(f"🔄 Intento de recuperación {recovery_attempt + 1}/{MAX_RECOVERY_ATTEMPTS}")
                    if reiniciar_selectores(driver):
                        break
                else:
                    return False
        
        # Si llegamos aquí, hubo recovery, así que reintentamos desde el principio
        time.sleep(2)
    
    return False

# --- FASE 1: RECOPILACIÓN DE TAREAS CON RECUPERACIÓN ---
def recopilar_todas_las_tareas_seguro(driver):
    """Navega por todos los menús para crear una lista maestra de todas las combinaciones con manejo de errores."""
    log_message("=== FASE 1: Creando mapa completo de todas las motos y scooters (con recuperación) ===")
    tareas = []
    
    # Definir localizadores
    TIPO_LOCATOR = (By.ID, 'itipo')
    MARCA_LOCATOR = (By.ID, 'imarca')
    CC_LOCATOR = (By.ID, 'icc')
    MODELO_LOCATOR = (By.ID, 'imodel')
    
    try:
        # Inicializar página
        if not reiniciar_selectores(driver):
            log_message("❌ No se pudo inicializar la página")
            return []
        
        # Obtener tipos (Moto y Scooter)
        tipos_vehiculos = [
            {'value': '3', 'text': 'Moto'},
            {'value': '4', 'text': 'Scooter'}
        ]
        
        for tipo in tipos_vehiculos:
            log_message(f"Procesando tipo: {tipo['text']}")
            
            # Reiniciar selectores antes de cada tipo
            if not reiniciar_selectores(driver):
                log_message(f"❌ Error reiniciando para tipo {tipo['text']}")
                continue
            
            # Seleccionar tipo
            if not seleccionar_opcion_segura_con_recuperacion(driver, TIPO_LOCATOR, tipo['value'], MARCA_LOCATOR, "tipo"):
                log_message(f"❌ ERROR: No se pudo seleccionar el tipo {tipo['text']}")
                continue
            
            # Obtener marcas
            marcas = obtener_opciones_desplegable_seguro(driver, MARCA_LOCATOR)
            if not marcas:
                log_message(f"❌ No se encontraron marcas para {tipo['text']}")
                continue
            
            for idx_marca, marca in enumerate(marcas):
                log_message(f"  Procesando marca {idx_marca + 1}/{len(marcas)}: {marca['text']}")
                
                # Re-seleccionar tipo antes de cada marca (por seguridad)
                if not seleccionar_opcion_segura_con_recuperacion(driver, TIPO_LOCATOR, tipo['value'], MARCA_LOCATOR, "tipo"):
                    log_message(f"❌ Error re-seleccionando tipo para marca {marca['text']}")
                    continue
                
                if not seleccionar_opcion_segura_con_recuperacion(driver, MARCA_LOCATOR, marca['value'], CC_LOCATOR, "marca"):
                    log_message(f"❌ ERROR: No se pudo seleccionar la marca {marca['text']}")
                    continue
                
                # Obtener CCs
                ccs = obtener_opciones_desplegable_seguro(driver, CC_LOCATOR)
                if not ccs:
                    log_message(f"⚠️ No se encontraron CCs para {marca['text']}")
                    continue
                
                for idx_cc, cc in enumerate(ccs):
                    log_message(f"    Procesando CC {idx_cc + 1}/{len(ccs)}: {cc['text']}")
                    
                    # Re-seleccionar tipo y marca antes de cada CC
                    if not seleccionar_opcion_segura_con_recuperacion(driver, TIPO_LOCATOR, tipo['value'], MARCA_LOCATOR, "tipo"):
                        log_message(f"❌ Error re-seleccionando tipo para CC {cc['text']}")
                        continue
                    
                    if not seleccionar_opcion_segura_con_recuperacion(driver, MARCA_LOCATOR, marca['value'], CC_LOCATOR, "marca"):
                        log_message(f"❌ Error re-seleccionando marca para CC {cc['text']}")
                        continue
                    
                    if not seleccionar_opcion_segura_con_recuperacion(driver, CC_LOCATOR, cc['value'], MODELO_LOCATOR, "CC"):
                        log_message(f"❌ ERROR: No se pudo seleccionar CC {cc['text']} - SALTANDO")
                        continue
                    
                    # Obtener modelos
                    modelos = obtener_opciones_desplegable_seguro(driver, MODELO_LOCATOR)
                    if not modelos:
                        log_message(f"⚠️ No se encontraron modelos para {cc['text']}cc - SALTANDO")
                        continue
                    
                    log_message(f"      ✅ Encontrados {len(modelos)} modelos para {cc['text']}cc")
                    for modelo in modelos:
                        tarea = {
                            'tipo_value': tipo['value'],
                            'tipo_text': tipo['text'],
                            'marca_value': marca['value'],
                            'marca_text': marca['text'],
                            'cc_value': cc['value'],
                            'cc_text': cc['text'],
                            'modelo_value': modelo['value'],
                            'modelo_text': modelo['text']
                        }
                        tareas.append(tarea)
        
        # Guardar tareas
        if tareas:
            with open(TASKS_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=tareas[0].keys())
                writer.writeheader()
                writer.writerows(tareas)
            
            log_message(f"=== FASE 1 COMPLETADA: {len(tareas)} tareas guardadas en '{TASKS_FILE}' ===")
        else:
            log_message("❌ ERROR: No se recopilaron tareas")
            
        return tareas
        
    except Exception as e:
        log_message(f"❌ ERROR CRÍTICO en Fase 1: {e}")
        return []

# --- CONTINUAMOS CON LAS FUNCIONES ORIGINALES (sin cambios significativos) ---
def extraer_productos_de_pagina(driver):
    """Extrae todos los productos de la página actual, incluyendo paginación."""
    productos = []
    
    try:
        # Esperar a que cargue el contenido
        wait = WebDriverWait(driver, 20)
        
        # Intentar detectar si hay productos o si la página está vacía
        try:
            # Esperar a que aparezca el contenedor de productos O un mensaje de "no productos"
            wait.until(lambda d: 
                d.find_elements(By.CSS_SELECTOR, 'div.vista_fitxes') or 
                d.find_elements(By.CSS_SELECTOR, '.no-products, .sin-productos') or
                "No se han encontrado productos" in d.page_source.lower()
            )
        except TimeoutException:
            log_message("        ⚠️ Timeout esperando contenido de productos")
            return []
        
        # Verificar si hay productos
        contenedor_productos = driver.find_elements(By.CSS_SELECTOR, 'div.vista_fitxes')
        if not contenedor_productos:
            log_message("        ℹ️ No se encontró contenedor de productos en esta página")
            return []
        
        # Verificar si hay mensaje de "sin productos"
        if "no se han encontrado productos" in driver.page_source.lower():
            log_message("        ℹ️ La página indica que no hay productos disponibles")
            return []
        
        # Obtener URLs de todas las páginas de paginación
        paginas_urls = {driver.current_url}
        
        try:
            # Buscar enlaces de paginación
            pagination_links = driver.find_elements(By.CSS_SELECTOR, "div.paginacio a.num[href], .pagination a[href]")
            for link in pagination_links:
                href = link.get_attribute('href')
                if href and href != driver.current_url:
                    paginas_urls.add(href)
            
            if len(paginas_urls) > 1:
                log_message(f"        🔄 Detectadas {len(paginas_urls)} páginas de productos")
        except Exception as e:
            log_message(f"        ⚠️ Error obteniendo paginación: {e}")
        
        # Procesar cada página
        for i, pagina_url in enumerate(sorted(list(paginas_urls))):
            try:
                if i > 0:  # Si no es la primera página, navegar
                    log_message(f"        🔄 Navegando a página {i+1}/{len(paginas_urls)}: {pagina_url}")
                    driver.get(pagina_url)
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    
                    # Esperar a que cargue la nueva página
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.vista_fitxes')))
                
                log_message(f"        🔍 Extrayendo productos de página {i+1}/{len(paginas_urls)}")
                
                # Extraer productos de la página actual
                productos_pagina = extraer_productos_pagina_actual(driver)
                productos.extend(productos_pagina)
                
                log_message(f"        📦 Página {i+1}: {len(productos_pagina)} productos extraídos")
                
            except Exception as e:
                log_message(f"        ❌ Error procesando página {i+1}: {e}")
                continue
        
        # Eliminar duplicados basados en URL
        productos_unicos = []
        urls_vistas = set()
        for producto in productos:
            if producto['url'] not in urls_vistas:
                productos_unicos.append(producto)
                urls_vistas.add(producto['url'])
            else:
                log_message(f"        🔄 Producto duplicado omitido: {producto['url'].split('/')[-1]}")
        
        log_message(f"        ✅ Total productos únicos encontrados: {len(productos_unicos)}")
        return productos_unicos
        
    except Exception as e:
        log_message(f"        ❌ Error extrayendo productos: {e}")
        return []

def extraer_productos_pagina_actual(driver):
    """Extrae productos de la página actual con mejor detección."""
    productos = []
    
    try:
        # Buscar todos los contenedores de productos con múltiples selectores
        contenedores_productos = []
        
        # Intentar diferentes selectores para encontrar productos
        selectores_productos = [
            'div.vista_fitxes > div.producte',
            'div.vista_fitxes .producte',
            '.producte',
            'div[class*="product"]',
            'article.product'
        ]
        
        for selector in selectores_productos:
            contenedores = driver.find_elements(By.CSS_SELECTOR, selector)
            if contenedores:
                contenedores_productos = contenedores
                log_message(f"          🎯 Productos encontrados con selector: {selector} ({len(contenedores)} elementos)")
                break
        
        if not contenedores_productos:
            log_message("          ⚠️ No se encontraron contenedores de productos con ningún selector")
            return []
        
        for idx, contenedor in enumerate(contenedores_productos):
            try:
                # Obtener URL del producto
                link_element = contenedor.find_element(By.TAG_NAME, 'a')
                url_producto = link_element.get_attribute('href')
                
                if not url_producto:
                    log_message(f"          ⚠️ Producto {idx+1} sin URL válida")
                    continue
                
                # Obtener marca del producto con múltiples estrategias
                marca_producto = "N/A"
                
                # Estrategia 1: Imagen con título
                try:
                    marca_img = contenedor.find_element(By.CSS_SELECTOR, 'div.marca img.marcaprod, .marca img, img[class*="marca"]')
                    marca_producto = marca_img.get_attribute('title') or marca_img.get_attribute('alt')
                except:
                    pass
                
                # Estrategia 2: Elemento de marca directo
                if not marca_producto or marca_producto == "N/A":
                    try:
                        marca_element = contenedor.find_element(By.CSS_SELECTOR, '.marca, [class*="brand"], .brand')
                        marca_producto = marca_element.get_attribute('title') or marca_element.text.strip()
                    except:
                        pass
                
                # Estrategia 3: Buscar en el título o nombre del producto
                if not marca_producto or marca_producto == "N/A":
                    try:
                        titulo_element = contenedor.find_element(By.CSS_SELECTOR, '.nom_producte, .product-name, .title, h3, h4')
                        titulo_texto = titulo_element.text.strip()
                        # Intentar extraer marca del inicio del título
                        palabras = titulo_texto.split()
                        if palabras:
                            marca_producto = palabras[0]
                    except:
                        pass
                
                marca_producto = marca_producto.strip() if marca_producto else "N/A"
                
                producto = {
                    'url': url_producto,
                    'marca_producto': marca_producto
                }
                productos.append(producto)
                
                log_message(f"          ✓ Producto {idx+1}: {marca_producto} - {url_producto.split('/')[-1]}")
                
            except Exception as e:
                log_message(f"          ❌ Error procesando producto {idx+1}: {e}")
                continue
        
        return productos
        
    except Exception as e:
        log_message(f"          ❌ Error en extraer_productos_pagina_actual: {e}")
        return []

def extraer_detalle_producto(driver, url_producto, marca_producto, datos_moto):
    """Extrae los detalles completos de un producto específico (SIN buscar MEIWA/HIFLO)."""
    try:
        driver.get(url_producto)
        wait = WebDriverWait(driver, 20)
        
        # Esperar a que cargue la página del producto
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'detalls')))
        time.sleep(1)
        
        # Extraer nombre del producto
        nombre_producto = "N/A"
        try:
            nombre_element = driver.find_element(By.CSS_SELECTOR, '.nom_producte > span')
            nombre_producto = nombre_element.text.strip()
        except:
            try:
                nombre_element = driver.find_element(By.CSS_SELECTOR, '.nom_producte')
                nombre_producto = nombre_element.text.strip()
            except:
                pass
        
        # Extraer referencia principal
        referencia_principal = "N/A"
        try:
            ref_element = driver.find_element(By.XPATH, "//div[span[contains(text(), 'Referencia:')]]")
            referencia_principal = ref_element.text.replace('Referencia:', '').strip()
        except:
            try:
                ref_element = driver.find_element(By.XPATH, "//span[contains(text(), 'Referencia:')]")
                referencia_principal = ref_element.text.replace('Referencia:', '').strip()
            except:
                pass
        
        # --- CAMBIO PRINCIPAL ---
        # Asignar N/A directamente sin buscar en la web.
        ref_meiwa = "N/A"
        ref_hiflo = "N/A"
        # Todo el bloque de código que buscaba "más información" ha sido eliminado.
        
        # Crear registro completo
        registro = [
            datos_moto['tipo_text'],           # TIPO
            datos_moto['marca_text'],          # MARCA MOTO
            datos_moto['modelo_parseado'],     # MODELO
            datos_moto['cc_parseado'],         # CC
            datos_moto['anio'],                # AÑO
            datos_moto['url_general'],         # URL GENERAL
            nombre_producto,                   # PRODUCTO
            marca_producto,                    # MARCA PRODUCTO
            referencia_principal,              # REFERENCIA
            ref_meiwa,                         # REFERENCIA MEIWA (siempre N/A)
            ref_hiflo,                         # REFERENCIA HIFLO (siempre N/A)
            url_producto                       # URL DEL PRODUCTO
        ]
        
        return registro
        
    except Exception as e:
        log_message(f"ERROR extrayendo detalles de {url_producto}: {e}")
        return None

def parsear_modelo_y_anio(texto_modelo, cc_text):
    """Parsea el modelo completo para extraer modelo, CC y año."""
    modelo_limpio = texto_modelo
    cc_parseado = cc_text
    anio = "N/A"
    
    try:
        # Extraer año (buscar patrones como (2020), (2018-2021), etc.)
        anio_match = re.search(r'\((\d{4}(?:[-/]\d{4})?)\)', texto_modelo)
        if anio_match:
            anio_texto = anio_match.group(1)
            anio = anio_texto.split('-')[0].split('/')[0]  # Tomar primer año
            # Remover el año del modelo
            modelo_limpio = re.sub(r'\s*\(\d{4}(?:[-/]\d{4})?\)\s*', '', texto_modelo).strip()
        
        # Extraer CC del final del modelo si está presente
        cc_match = re.search(r'\b(\d{2,4})\s*', modelo_limpio)
        if cc_match:
            cc_extraido = cc_match.group(1)
            if len(cc_extraido) >= 2:  # Validar que sea un CC válido
                cc_parseado = cc_extraido
                modelo_limpio = re.sub(r'\s*\b\d{2,4}\s*,', '', modelo_limpio).strip()
        
    except Exception as e:
        log_message(f"Error parseando modelo '{texto_modelo}': {e}")
    
    return modelo_limpio, cc_parseado, anio

# <--- REEMPLAZA TU FUNCIÓN ORIGINAL CON ESTA ---
def procesar_tarea_seguro(driver, tarea, processed_keys):
    """Procesa una tarea específica con manejo robusto de errores y productos por año."""
    log_message(f"\n--- Procesando: {tarea['tipo_text']} | {tarea['marca_text']} | {tarea['cc_text']} | {tarea['modelo_text']} ---")
    
    productos_procesados = 0
    
    try:
        if not reiniciar_selectores(driver):
            log_message(f"❌ ERROR: No se pudo reiniciar selectores para la tarea")
            return 0
        
        if not seleccionar_opcion_segura_con_recuperacion(driver, (By.ID, 'itipo'), tarea['tipo_value'], (By.ID, 'imarca'), "tipo"):
            log_message(f"❌ ERROR: No se pudo seleccionar tipo {tarea['tipo_text']}")
            return 0
        
        if not seleccionar_opcion_segura_con_recuperacion(driver, (By.ID, 'imarca'), tarea['marca_value'], (By.ID, 'icc'), "marca"):
            log_message(f"❌ ERROR: No se pudo seleccionar marca {tarea['marca_text']}")
            return 0
        
        if not seleccionar_opcion_segura_con_recuperacion(driver, (By.ID, 'icc'), tarea['cc_value'], (By.ID, 'imodel'), "CC"):
            log_message(f"❌ ERROR: No se pudo seleccionar CC {tarea['cc_text']}")
            return 0
        
        if not seleccionar_opcion_segura_con_recuperacion(driver, (By.ID, 'imodel'), tarea['modelo_value'], None, "modelo"):
            log_message(f"❌ ERROR: No se pudo seleccionar modelo {tarea['modelo_text']}")
            return 0
        
        time.sleep(3)
        
        tabla_anios = driver.find_elements(By.CSS_SELECTOR, "table.resultats tbody tr")
        
        if tabla_anios:
            log_message(f"    Encontrada tabla con {len(tabla_anios)} filas de años")
            
            year_column_index = -1
            try:
                headers = driver.find_elements(By.CSS_SELECTOR, "table.resultats thead th")
                for idx, header in enumerate(headers):
                    if "AÑO" in header.text.upper() or "ANY" in header.text.upper():
                        year_column_index = idx
                        log_message(f"    📅 Columna de año encontrada en posición: {idx}")
                        break
            except:
                year_column_index = -1
            
            filas_info = []
            for idx_fila, fila in enumerate(tabla_anios):
                try:
                    celdas = fila.find_elements(By.TAG_NAME, 'td')
                    if len(celdas) < 2:
                        continue
                    
                    link_element = celdas[0].find_element(By.TAG_NAME, 'a')
                    url_general = link_element.get_attribute('href')
                    modelo_completo = celdas[0].text.strip()
                    
                    anio = "N/A"
                    if year_column_index >= 0 and year_column_index < len(celdas):
                        anio_texto = celdas[year_column_index].text.strip()
                        if anio_texto.isdigit() and len(anio_texto) >= 4:
                            anio = anio_texto
                    
                    if anio == "N/A":
                        modelo_parseado, cc_parseado, anio_parseado = parsear_modelo_y_anio(modelo_completo, tarea['cc_text'])
                        anio = anio_parseado
                    else:
                        modelo_parseado, cc_parseado, _ = parsear_modelo_y_anio(modelo_completo, tarea['cc_text'])
                    
                    fila_info = {
                        'url_general': url_general,
                        'modelo_completo': modelo_completo,
                        'modelo_parseado': modelo_parseado,
                        'cc_parseado': cc_parseado,
                        'anio': anio,
                        'fila_numero': idx_fila + 1
                    }
                    filas_info.append(fila_info)
                    
                    log_message(f"      🔍 Fila {idx_fila + 1}: {modelo_completo} - Año: {anio} - URL: {url_general}")
                    
                except Exception as e:
                    log_message(f"❌ ERROR recolectando información de fila {idx_fila + 1}: {e}")
                    continue
            
            log_message(f"    📊 Total de filas válidas encontradas: {len(filas_info)}")
            
            for fila_info in filas_info:
                try:
                    log_message(f"\n    🔄 Procesando Año {fila_info['anio']} (Fila {fila_info['fila_numero']})...")
                    
                    datos_moto = {
                        'tipo_text': tarea['tipo_text'],
                        'marca_text': tarea['marca_text'],
                        'modelo_parseado': fila_info['modelo_parseado'],
                        'cc_parseado': fila_info['cc_parseado'],
                        'anio': fila_info['anio'],
                        'url_general': fila_info['url_general']
                    }
                    
                    log_message(f"      🌐 Navegando a: {fila_info['url_general']}")
                    driver.get(fila_info['url_general'])
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    
                    productos = extraer_productos_de_pagina(driver)
                    
                    log_message(f"      📦 Año {fila_info['anio']}: {len(productos)} productos encontrados")
                    
                    productos_procesados_anio = 0
                    for producto in productos:
                        clave_unica = crear_clave_unica(producto['url'], datos_moto)
                        
                        if clave_unica in processed_keys:
                            log_message(f"        ⏭️ OMITIENDO (ya procesado para este contexto): {producto['url'].split('/')[-1]} - Año: {datos_moto['anio']}")
                            continue
                        
                        detalle = extraer_detalle_producto(driver, producto['url'], producto['marca_producto'], datos_moto)
                        if detalle:
                            guardar_registro_csv(detalle, OUTPUT_FILE)
                            processed_keys.add(clave_unica)
                            productos_procesados += 1
                            productos_procesados_anio += 1
                            log_message(f"        ✅ Procesado: {detalle[6]} ({detalle[7]}) - Año: {fila_info['anio']}")
                        else:
                            log_message(f"        ❌ Error procesando producto: {producto['url']}")
                    
                    log_message(f"      📈 Año {fila_info['anio']} completado: {productos_procesados_anio} productos procesados")
                    
                    if len(filas_info) > 1:
                        time.sleep(1)
                    
                except Exception as e:
                    log_message(f"❌ ERROR procesando año {fila_info['anio']}: {e}")
                    continue
        
        else:
            log_message("    Sin tabla de años, procesando productos directos")
            
            url_general = driver.current_url
            modelo_parseado, cc_parseado, anio = parsear_modelo_y_anio(tarea['modelo_text'], tarea['cc_text'])
            
            datos_moto = {
                'tipo_text': tarea['tipo_text'],
                'marca_text': tarea['marca_text'],
                'modelo_parseado': modelo_parseado,
                'cc_parseado': cc_parseado,
                'anio': anio,
                'url_general': url_general
            }
            
            productos = extraer_productos_de_pagina(driver)
            log_message(f"    {len(productos)} productos encontrados")
            
            for producto in productos:
                clave_unica = crear_clave_unica(producto['url'], datos_moto)
                
                if clave_unica in processed_keys:
                    log_message(f"      ⏭️ OMITIENDO (ya procesado para este contexto): {producto['url'].split('/')[-1]} - Año: {datos_moto['anio']}")
                    continue
                
                detalle = extraer_detalle_producto(driver, producto['url'], producto['marca_producto'], datos_moto)
                if detalle:
                    guardar_registro_csv(detalle, OUTPUT_FILE)
                    processed_keys.add(clave_unica)
                    productos_procesados += 1
                    log_message(f"      ✅ Procesado: {detalle[6]} - {detalle[7]} - Año: {datos_moto['anio']}")
        
        log_message(f"--- Tarea completada: {productos_procesados} productos procesados ---")
        return productos_procesados
        
    except Exception as e:
        log_message(f"❌ ERROR CRÍTICO procesando tarea: {e}")
        return 0

def hacer_backup_archivos():
    """Crear backup de archivos existentes antes de empezar"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    archivos_backup = []
    
    files_to_backup = [OUTPUT_FILE, LOG_FILE]
    
    for file in files_to_backup:
        if os.path.exists(file):
            try:
                backup_name = file.replace('.csv', f'_backup_{timestamp}.csv').replace('.txt', f'_backup_{timestamp}.txt')
                with open(file, 'r', encoding='utf-8') as original:
                    with open(backup_name, 'w', encoding='utf-8') as backup:
                        backup.write(original.read())
                archivos_backup.append(backup_name)
                log_message(f"📁 Backup creado: {backup_name}")
            except Exception as e:
                log_message(f"⚠️ Error creando backup de {file}: {e}")
    
    return archivos_backup

def verificar_resultado_final(csv_file):
    """Verifica que el proceso funcionó correctamente"""
    try:
        productos_por_url = {}
        total_registros = 0
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_registros += 1
                url = row.get('URL DEL PRODUCTO', '')
                if url:
                    if url not in productos_por_url:
                        productos_por_url[url] = []
                    productos_por_url[url].append({
                        'marca': row.get('MARCA', ''),
                        'modelo': row.get('MODELO', ''),
                        'anio': row.get('AÑO', ''),
                        'producto': row.get('Producto', '')
                    })
        
        log_message(f"\n{'='*60}")
        log_message(f"📊 VERIFICACIÓN FINAL DEL RESULTADO:")
        log_message(f"   • Total de registros: {total_registros}")
        log_message(f"   • Productos únicos (por URL): {len(productos_por_url)}")
        
        # Mostrar ejemplos de productos con múltiples años
        productos_multiples_años = {url: data for url, data in productos_por_url.items() if len(data) > 1}
        log_message(f"   • Productos con múltiples años: {len(productos_multiples_años)}")
        
        if productos_multiples_años:
            log_message(f"\n📝 EJEMPLOS DE PRODUCTOS CON MÚLTIPLES AÑOS (primeros 5):")
            for i, (url, registros) in enumerate(list(productos_multiples_años.items())[:5]):
                producto_nombre = registros[0]['producto']
                años = sorted([r['anio'] for r in registros if r['anio'] != 'N/A'])
                marca_moto = registros[0]['marca']
                modelo_moto = registros[0]['modelo']
                log_message(f"   {i+1}. {producto_nombre}")
                log_message(f"      Moto: {marca_moto} {modelo_moto}")
                log_message(f"      Años compatibles: {', '.join(años) if años else 'N/A'}")
                log_message(f"      URL: {url.split('/')[-1]}")
        
        # Estadísticas adicionales
        registros_con_anio = sum(1 for productos in productos_por_url.values() 
                               for producto in productos if producto['anio'] != 'N/A')
        
        log_message(f"\n📈 ESTADÍSTICAS ADICIONALES:")
        log_message(f"   • Registros con año específico: {registros_con_anio}")
        log_message(f"   • Registros sin año específico: {total_registros - registros_con_anio}")
        log_message(f"   • Promedio de años por producto: {total_registros/len(productos_por_url):.1f}")
        
        return True
        
    except Exception as e:
        log_message(f"❌ Error verificando resultado: {e}")
        return False

# --- SCRIPT PRINCIPAL CON MANEJO MEJORADO DE ERRORES ---
# <--- REEMPLAZA TU BLOQUE PRINCIPAL CON ESTE ---
if __name__ == "__main__":
    log_message("=== INICIANDO SCRAPER EUROMOTO85 CON PRODUCTOS POR AÑO ===")
    
    if FORCE_FRESH_START:
        log_message("🔄 MODO RESET ACTIVADO - Iniciando proceso limpio")
        if os.path.exists(OUTPUT_FILE):
            hacer_backup_archivos()
            try:
                os.remove(OUTPUT_FILE)
                log_message(f"🗑️ Archivo anterior eliminado: {OUTPUT_FILE}")
            except Exception as e:
                log_message(f"⚠️ Error eliminando archivo anterior: {e}")
    
    lista_de_tareas = []
    if SKIP_PHASE_1 and os.path.exists(TASKS_FILE):
        log_message(f"⏭️ SALTANDO FASE 1 - Cargando tareas existentes desde '{TASKS_FILE}'")
        try:
            with open(TASKS_FILE, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                lista_de_tareas = list(reader)
            log_message(f"✅ Se cargaron {len(lista_de_tareas)} tareas")
        except Exception as e:
            log_message(f"❌ Error cargando tareas: {e}")
            exit()
    elif not SKIP_PHASE_1:
        if os.path.exists(TASKS_FILE):
            log_message(f"📋 Cargando tareas existentes desde '{TASKS_FILE}'")
            try:
                with open(TASKS_FILE, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    lista_de_tareas = list(reader)
                log_message(f"✅ Se cargaron {len(lista_de_tareas)} tareas")
            except Exception as e:
                log_message(f"❌ Error cargando tareas: {e}")
        if not lista_de_tareas:
            log_message("🔄 Creando nueva lista de tareas...")
            driver_fase1 = configurar_driver()
            if not driver_fase1:
                log_message("❌ ERROR: No se pudo iniciar driver para Fase 1")
                exit()
            lista_de_tareas = recopilar_todas_las_tareas_seguro(driver_fase1)
            driver_fase1.quit()
            if not lista_de_tareas:
                log_message("❌ ERROR: No se pudieron recopilar tareas")
                exit()
    else:
        log_message(f"❌ ERROR: SKIP_PHASE_1=True pero no existe '{TASKS_FILE}'")
        log_message("Opciones:\n1. Cambiar SKIP_PHASE_1 = False para crear tareas\n2. Asegurarse de que existe el archivo de tareas")
        exit()
    
    if START_AFTER_BRAND:
        try:
            indices = [i for i, task in enumerate(lista_de_tareas) if task['marca_text'] == START_AFTER_BRAND]
            if indices:
                lista_de_tareas = lista_de_tareas[indices[-1] + 1:]
                log_message(f"🔍 Continuando desde después de la marca: {START_AFTER_BRAND}")
                log_message(f"📊 Tareas restantes: {len(lista_de_tareas)}")
        except Exception as e:
            log_message(f"⚠️ Error aplicando filtro de marca de inicio: {e}")
    
    processed_keys = leer_registros_procesados(OUTPUT_FILE) if not FORCE_FRESH_START else set()
    
    log_message(f"=== FASE 2: Procesando {len(lista_de_tareas)} tareas con productos por año ===")
    total_productos_procesados, tareas_exitosas, tareas_con_error, tareas_saltadas = 0, 0, 0, 0
    
    for i, tarea in enumerate(lista_de_tareas):
        log_message(f"\n>>> TAREA {i+1}/{len(lista_de_tareas)} <<<")
        driver = None
        try:
            driver = configurar_driver()
            if not driver:
                log_message("❌ ERROR: No se pudo iniciar el driver. Saltando tarea.")
                tareas_con_error += 1
                continue
            
            productos_en_tarea = procesar_tarea_seguro(driver, tarea, processed_keys)
            
            if productos_en_tarea > 0:
                total_productos_procesados += productos_en_tarea
                tareas_exitosas += 1
                log_message(f"✅ Tarea {i+1} exitosa: {productos_en_tarea} productos procesados")
            elif productos_en_tarea == 0:
                log_message("⚠️ Tarea completada pero sin productos nuevos")
                tareas_saltadas += 1
            else:
                log_message("❌ Tarea falló")
                tareas_con_error += 1
        except Exception as e:
            log_message(f"❌ ERROR CRÍTICO en tarea {i+1}: {e}")
            tareas_con_error += 1
        finally:
            if driver:
                try:
                    driver.quit()
                    log_message(f"🔧 Driver cerrado para tarea {i+1}")
                except:
                    log_message(f"⚠️ Error cerrando driver para tarea {i+1}")
            if i < len(lista_de_tareas) - 1:
                log_message("⏳ Pausa entre tareas...")
                time.sleep(3)
    
    log_message(f"\n" + "="*60)
    log_message(f"=== PROCESO COMPLETADO CON PRODUCTOS POR AÑO ===")
    log_message(f"="*60)
    log_message(f"📊 ESTADÍSTICAS FINALES:")
    log_message(f"   • Total de tareas procesadas: {len(lista_de_tareas)}")
    log_message(f"   • Tareas exitosas: {tareas_exitosas}")
    log_message(f"   • Tareas saltadas (sin productos): {tareas_saltadas}")
    log_message(f"   • Tareas con error: {tareas_con_error}")
    log_message(f"   • Total de productos procesados: {total_productos_procesados}")
    log_message(f"   • Tasa de éxito: {(tareas_exitosas/len(lista_de_tareas)*100 if len(lista_de_tareas) > 0 else 0):.1f}%")
    log_message(f"")
    log_message(f"📁 ARCHIVOS GENERADOS:")
    log_message(f"   • Datos CSV: {OUTPUT_FILE}")
    log_message(f"   • Lista de tareas: {TASKS_FILE}")
    log_message(f"   • Archivo de log: {LOG_FILE}")
    
    if total_productos_procesados > 0 or (os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0):
        log_message(f"\n🔍 VERIFICANDO RESULTADO FINAL...")
        if verificar_resultado_final(OUTPUT_FILE):
            log_message(f"\n🎉 ¡SCRAPING COMPLETADO EXITOSAMENTE CON PRODUCTOS POR AÑO!")
        else:
            log_message(f"\n⚠️ SCRAPING COMPLETADO PERO HAY PROBLEMAS EN LA VERIFICACIÓN")
    else:
        log_message(f"\n⚠️ SCRAPING COMPLETADO PERO SIN NUEVOS PRODUCTOS")
        log_message(f"   Es posible que todos los productos ya hayan sido procesados")
    
    log_message(f"\n" + "="*60)
    print(f"\n🏁 ¡PROCESO TERMINADO! Revisa el archivo '{OUTPUT_FILE}' para ver los resultados.")
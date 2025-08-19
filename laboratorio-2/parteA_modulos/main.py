import utilidades_cadenas as uc

print("=== DEMO UTILIDADES DE CADENAS ===")

texto = "   Hola Mundo  "
print("Original:", texto)
print("Normalizado:", uc.normalizar(texto))
print("Es palabra válida:", uc.es_palabra_valida("Python3"))  # caso límite
print("Formato título:", uc.formatear_titulo("bienvenido a python"))
print("Longitud:", uc.obtener_longitud(texto))

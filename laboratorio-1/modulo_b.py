# modulo_b.py

# B.1 – Validación de entrada

def parsear_enteros(entradas):
    enteros = []
    errores = []

    for valor in entradas:
        try:
            numero = int(valor)
            enteros.append(numero)
        except ValueError:
            errores.append(f"No se pudo convertir: {valor}")
    
    return enteros, errores


# B.2 – Excepciones personalizadas

class CantidadInvalida(Exception):
    pass

def calcular_total(precio_unitario, cantidad):
    if cantidad <= 0:
        raise CantidadInvalida("La cantidad debe ser mayor que cero.")
    if precio_unitario < 0:
        raise ValueError("El precio no puede ser negativo.")
    return precio_unitario * cantidad


# Pruebas
if __name__ == "__main__":
    entradas = ["10", "x", "3"]
    valores, errores = parsear_enteros(entradas)
    print("Valores válidos:", valores)
    print("Errores:", errores)

    try:
        print(calcular_total(10, 3))  # 30
        print(calcular_total(10, 0))  # Error
    except Exception as e:
        print("Error:", e)

# modulo_c.py

# C.1 – Decorador de validación

def requiere_positivos(func):
    def wrapper(*args, **kwargs):
        for arg in args:
            if isinstance(arg, (int, float)) and arg <= 0:
                raise ValueError(f"Argumento inválido: {arg} (debe ser > 0)")
        return func(*args, **kwargs)
    return wrapper


@requiere_positivos
def calcular_descuento(precio, porcentaje):
    return precio * (1 - porcentaje)


@requiere_positivos
def escala(valor, factor):
    return valor * factor


# Pruebas
if __name__ == "__main__":
    print(calcular_descuento(100, 0.2))  # 80.0

    try:
        print(calcular_descuento(-1, 0.2))
    except ValueError as e:
        print("Error:", e)

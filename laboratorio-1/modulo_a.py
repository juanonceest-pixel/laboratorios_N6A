# modulo_a.py

# A.1 – Funciones como valores

def saludar(nombre):
    return f"Hola, {nombre}"

def despedir(nombre):
    return f"Adiós, {nombre}"

def aplaudir(nombre):
    return f"{nombre}, ¡bravo!"

acciones = {
    "saludar": saludar,
    "despedir": despedir,
    "aplaudir": aplaudir
}

def ejecutar(accion, *args, **kwargs):
    if accion not in acciones:
        raise ValueError(f"Acción inválida: {accion}")
    return acciones[accion](*args, **kwargs)


# A.2 – Funciones internas y closures

def crear_descuento(porcentaje):
    def aplicar_descuento(precio):
        return precio * (1 - porcentaje)
    return aplicar_descuento


# Pruebas
if __name__ == "__main__":
    print(ejecutar("saludar", "Ana"))
    
    try:
        print(ejecutar("bailar", "Ana"))
    except ValueError as e:
        print(e)
    
    descuento10 = crear_descuento(0.10)
    descuento25 = crear_descuento(0.25)
    
    print(descuento10(100))  # 90.0
    print(descuento25(80))   # 60.0

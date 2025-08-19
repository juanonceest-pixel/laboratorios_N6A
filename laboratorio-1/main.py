# main.py

import modulo_a
import modulo_b
import modulo_c

if __name__ == "__main__":
    print("=== MÓDULO A ===")
    print(modulo_a.ejecutar("saludar", "Sebastian"))

    print("\n=== MÓDULO B ===")
    valores, errores = modulo_b.parsear_enteros(["5", "abc", "10"])
    print("Valores:", valores)
    print("Errores:", errores)

    try:
        print("Total:", modulo_b.calcular_total(10, 2))
    except Exception as e:
        print("Error:", e)

    print("\n=== MÓDULO C ===")
    try:
        print("Descuento:", modulo_c.calcular_descuento(100, 0.1))
        print("Escala:", modulo_c.escala(3, 2))
    except Exception as e:
        print("Error:", e)

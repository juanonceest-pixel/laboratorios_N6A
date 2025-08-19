import mi_paquete as mp

print("=== DEMO PAQUETE ===")
print("Suma:", mp.sumar(5, 7))
print("División:", mp.dividir(10, 2))
print("¿Es par 8?:", mp.es_par(8))

try:
    print("Suma si par (4+6):", mp.suma_si_par(4, 6))
    print("Suma si par (3+6):", mp.suma_si_par(3, 6))  # caso límite
except ValueError as e:
    print("Error:", e)

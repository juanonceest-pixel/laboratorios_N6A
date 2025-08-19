# main.py
from src.operaciones import suma, division, multiplicar

def main() -> None:
    print("Suma:", suma(5, 3))
    print("División:", division(10, 2))
    
    try:
        print("División por cero:", division(10, 0))
    except ZeroDivisionError as e:
        print("Error:", e)
    
    print("Multiplicación:", multiplicar(4, 2))

if __name__ == "__main__":
    main()
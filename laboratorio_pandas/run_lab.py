#!/usr/bin/env python3
"""
Script principal para ejecutar todo el laboratorio
Laboratorio: Análisis Tabular con pandas y DuckDB
"""

import sys
import os
from datetime import datetime

def run_section(section_name, script_path):
    """Ejecuta una sección del laboratorio"""
    print(f"\n{'='*60}")
    print(f"EJECUTANDO: {section_name}")
    print(f"{'='*60}")
    
    try:
        # Verificar que el archivo existe
        if not os.path.exists(script_path):
            print(f"❌ Error: No se encuentra el archivo {script_path}")
            return False
            
        # Ejecutar el script
        with open(script_path, 'r', encoding='utf-8') as file:
            script_content = file.read()
            exec(script_content)
        print(f"\n✓ {section_name} completado exitosamente")
    except Exception as e:
        print(f"\n❌ Error en {section_name}: {e}")
        return False
    return True

def check_requirements():
    """Verificar que las dependencias están instaladas"""
    print("Verificando dependencias...")
    try:
        import pandas
        print("✓ pandas instalado")
    except ImportError:
        print("❌ pandas no instalado. Ejecuta: pip install pandas")
        return False
    
    try:
        import duckdb
        print("✓ duckdb instalado")
    except ImportError:
        print("❌ duckdb no instalado. Ejecuta: pip install duckdb")
        return False
        
    try:
        import pyarrow
        print("✓ pyarrow instalado")
    except ImportError:
        print("❌ pyarrow no instalado. Ejecuta: pip install pyarrow")
        return False
    
    return True

def main():
    """Función principal"""
    print(f"🚀 Iniciando Laboratorio: Análisis Tabular con pandas y DuckDB")
    print(f"📅 Tiempo de inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Directorio actual: {os.getcwd()}")
    
    # Verificar dependencias
    if not check_requirements():
        print("\n❌ Faltan dependencias. Instálalas y vuelve a ejecutar.")
        return
    
    # Verificar estructura del proyecto
    print(f"\n📂 Verificando estructura del proyecto...")
    required_dirs = ['data', 'notebooks', 'outputs']
    for dir_name in required_dirs:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"✓ Creado directorio: {dir_name}")
        else:
            print(f"✓ Directorio existe: {dir_name}")
    
    # Verificar que existe el dataset
    dataset_path = 'data/chennai_reservoir_levels.csv'
    if not os.path.exists(dataset_path):
        print(f"\n❌ Error: No se encuentra el archivo {dataset_path}")
        print("📥 Por favor, descarga el dataset desde:")
        print("   https://raw.githubusercontent.com/MainakRepositor/Datasets/master/chennai_reservoir_levels.csv")
        print(f"   Y guárdalo como: {dataset_path}")
        
        # Intentar descargar automáticamente
        try:
            import urllib.request
            print("\n🔄 Intentando descargar automáticamente...")
            url = "https://raw.githubusercontent.com/MainakRepositor/Datasets/master/chennai_reservoir_levels.csv"
            urllib.request.urlretrieve(url, dataset_path)
            print(f"✅ Dataset descargado exitosamente: {dataset_path}")
        except Exception as e:
            print(f"❌ Error al descargar: {e}")
            print("🔧 Descárgalo manualmente por favor.")
            return
    else:
        print(f"✓ Dataset encontrado: {dataset_path}")
    
    # Verificar scripts del laboratorio
    print(f"\n📝 Verificando scripts...")
    scripts_to_check = [
        "notebooks/01_pandas_analysis.py",
        "notebooks/02_duckdb_analysis.py", 
        "notebooks/03_comparison.py"
    ]
    
    missing_scripts = []
    for script in scripts_to_check:
        if os.path.exists(script):
            print(f"✓ {script}")
        else:
            print(f"❌ {script} - NO ENCONTRADO")
            missing_scripts.append(script)
    
    if missing_scripts:
        print(f"\n⚠️  Faltan {len(missing_scripts)} script(s):")
        for script in missing_scripts:
            print(f"   - {script}")
        print("\n🔧 Crea estos archivos siguiendo la guía antes de continuar.")
        print("💡 Puedes ejecutar las secciones individualmente copiando el código.")
        return
    
    # Ejecutar secciones del laboratorio
    print(f"\n🎯 Ejecutando laboratorio...")
    sections = [
        ("Análisis con pandas", "notebooks/01_pandas_analysis.py"),
        ("Análisis con DuckDB", "notebooks/02_duckdb_analysis.py"),  
        ("Comparación y Conclusiones", "notebooks/03_comparison.py")
    ]
    
    results = []
    for section_name, script_path in sections:
        success = run_section(section_name, script_path)
        results.append((section_name, success))
        
        # Pausa entre secciones
        if success:
            print(f"⏳ Continuando en 2 segundos...")
            import time
            time.sleep(2)
    
    # Resumen final
    print(f"\n{'='*60}")
    print("📊 RESUMEN FINAL DEL LABORATORIO")
    print(f"{'='*60}")
    
    for section_name, success in results:
        status = "✅ COMPLETADO" if success else "❌ FALLIDO"
        print(f"  {section_name}: {status}")
    
    total_success = sum(1 for _, success in results if success)
    print(f"\n📈 Secciones completadas: {total_success}/{len(results)}")
    
    # Mostrar archivos generados
    if os.path.exists('outputs') and os.listdir('outputs'):
        print(f"\n📁 Archivos generados en outputs/:")
        output_files = os.listdir('outputs')
        for file in sorted(output_files):
            size = os.path.getsize(f'outputs/{file}')
            print(f"   📄 {file}: {size:,} bytes")
    
    if total_success == len(results):
        print(f"\n🎉 ¡Laboratorio completado exitosamente!")
        print(f"✨ Revisa los archivos en la carpeta 'outputs/' para ver los resultados.")
    else:
        print(f"\n⚠️  Algunas secciones fallaron. Revisa los errores anteriores.")
        print(f"💡 Puedes ejecutar las secciones individualmente.")
    
    print(f"\n⏰ Tiempo de finalización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Laboratorio interrumpido por el usuario.")
    except Exception as e:
        print(f"\n\n❌ Error inesperado: {e}")
        print(f"🔧 Verifica tu instalación y vuelve a intentar.")
    
    print(f"\n👋 ¡Gracias por usar el laboratorio!")
"""
Módulo para validación de datasets CSV.
Específicamente diseñado para validar el dataset de crímenes de LA.
"""

import csv
import os
from typing import List, Dict, Any, Tuple
from datetime import datetime

class CrimeDataValidator:
    """Validador para el dataset de crímenes de Los Angeles"""
    
    # Columnas obligatorias esperadas
    REQUIRED_COLUMNS = [
        'DR_NO', 'Date Rptd', 'DATE OCC', 'TIME OCC', 'AREA', 
        'AREA NAME', 'Crm Cd', 'Crm Cd Desc', 'Vict Age', 
        'Vict Sex', 'Status', 'LAT', 'LON'
    ]
    
    # Valores válidos para columnas categóricas
    VALID_SEX_VALUES = {'M', 'F', 'X', ''}  # M, F, X (desconocido), o vacío
    VALID_STATUS_VALUES = {'IC', 'CC', 'AO', 'JO'}  # Códigos de estado conocidos
    
    def __init__(self, csv_file_path: str):
        """
        Inicializa el validador con la ruta del archivo CSV.
        
        Args:
            csv_file_path (str): Ruta al archivo CSV
        """
        self.csv_file_path = csv_file_path
        self.data = []
        self.headers = []
    
    def load_data(self) -> bool:
        """
        Carga los datos del archivo CSV.
        
        Returns:
            bool: True si se cargó correctamente
            
        Raises:
            FileNotFoundError: Si el archivo no existe
            csv.Error: Si hay error al leer el CSV
        """
        if not os.path.exists(self.csv_file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {self.csv_file_path}")
        
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                self.headers = reader.fieldnames or []
                self.data = list(reader)
            return True
        except Exception as e:
            raise csv.Error(f"Error al leer el archivo CSV: {str(e)}")
    
    def validate_headers(self) -> Tuple[bool, List[str]]:
        """
        Valida que estén presentes todas las columnas obligatorias.
        
        Returns:
            Tuple[bool, List[str]]: (es_válido, columnas_faltantes)
        """
        missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in self.headers]
        return len(missing_columns) == 0, missing_columns
    
    def validate_dr_no_unique(self) -> bool:
        """
        Valida que los números DR_NO sean únicos.
        
        Returns:
            bool: True si todos los DR_NO son únicos
        """
        dr_numbers = [row.get('DR_NO', '') for row in self.data if row.get('DR_NO', '')]
        return len(dr_numbers) == len(set(dr_numbers))
    
    def validate_coordinates(self) -> Dict[str, Any]:
        """
        Valida que las coordenadas LAT/LON sean válidas.
        
        Returns:
            Dict: Estadísticas de validación de coordenadas
        """
        valid_coords = 0
        invalid_coords = 0
        missing_coords = 0
        
        for row in self.data:
            lat = row.get('LAT', '').strip()
            lon = row.get('LON', '').strip()
            
            if not lat or not lon:
                missing_coords += 1
                continue
            
            try:
                lat_float = float(lat)
                lon_float = float(lon)
                
                # Validar rangos aproximados para LA
                if -119 <= lon_float <= -117 and 33 <= lat_float <= 35:
                    valid_coords += 1
                else:
                    invalid_coords += 1
            except ValueError:
                invalid_coords += 1
        
        total = len(self.data)
        return {
            'total_rows': total,
            'valid_coordinates': valid_coords,
            'invalid_coordinates': invalid_coords,
            'missing_coordinates': missing_coords,
            'valid_percentage': (valid_coords / total * 100) if total > 0 else 0
        }
    
    def validate_victim_age(self) -> Dict[str, int]:
        """
        Valida las edades de las víctimas.
        
        Returns:
            Dict: Estadísticas de edades válidas/inválidas
        """
        valid_ages = 0
        invalid_ages = 0
        missing_ages = 0
        
        for row in self.data:
            age = row.get('Vict Age', '').strip()
            
            if not age:
                missing_ages += 1
                continue
            
            try:
                age_int = int(age)
                if 0 <= age_int <= 120:  # Rango razonable de edad
                    valid_ages += 1
                else:
                    invalid_ages += 1
            except ValueError:
                invalid_ages += 1
        
        return {
            'valid_ages': valid_ages,
            'invalid_ages': invalid_ages,
            'missing_ages': missing_ages
        }
    
    def validate_sex_values(self) -> bool:
        """
        Valida que los valores de sexo sean válidos.
        
        Returns:
            bool: True si todos los valores son válidos
        """
        for row in self.data:
            sex = row.get('Vict Sex', '').strip()
            if sex and sex not in self.VALID_SEX_VALUES:
                return False
        return True
    
    def get_basic_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas básicas del dataset.
        
        Returns:
            Dict: Estadísticas básicas
        """
        return {
            'total_rows': len(self.data),
            'total_columns': len(self.headers),
            'columns': self.headers,
            'sample_row': self.data[0] if self.data else {}
        }
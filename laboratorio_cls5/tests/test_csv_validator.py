"""
Pruebas unitarias para el validador de CSV.
Incluye pruebas para todas las reglas de validación del dataset de crímenes.
"""

import pytest
import csv
import tempfile
import os
from src.csv_validator import CrimeDataValidator

class TestCrimeDataValidator:
    """Pruebas para el validador de datos de crímenes"""
    
    @pytest.fixture
    def sample_csv_content(self):
        """Contenido CSV de muestra para las pruebas"""
        return """DR_NO,Date Rptd,DATE OCC,TIME OCC,AREA,AREA NAME,Rpt Dist No,Part 1-2,Crm Cd,Crm Cd Desc,Mocodes,Vict Age,Vict Sex,Vict Descent,Premis Cd,Premis Desc,Weapon Used Cd,Weapon Desc,Status,Status Desc,Crm Cd 1,Crm Cd 2,Crm Cd 3,Crm Cd 4,LOCATION,Cross Street,LAT,LON
211507896,04/11/2021 12:00:00 AM,11/07/2020 12:00:00 AM,0845,15,N Hollywood,1502,2,354,THEFT OF IDENTITY,0377,31,M,H,501,SINGLE FAMILY DWELLING,,,IC,Invest Cont,354,,,,7800 BEEMAN AV,,34.2124,-118.4092
201516622,10/21/2020 12:00:00 AM,10/18/2020 12:00:00 AM,1845,15,N Hollywood,1521,1,230,ASSAULT WITH DEADLY WEAPON,0416,32,F,H,102,SIDEWALK,200,KNIFE WITH BLADE 6INCHES OR LESS,IC,Invest Cont,230,,,,ATOLL AV,N GAULT,34.1993,-118.4203
301234567,01/15/2021 12:00:00 AM,01/14/2021 12:00:00 AM,1200,3,Southwest,0312,1,624,BATTERY - SIMPLE ASSAULT,,25,,B,108,PARKING LOT,,,CC,Comp Closed,624,,,,MAIN ST,,34.0522,-118.2437"""
    
    @pytest.fixture
    def invalid_csv_content(self):
        """Contenido CSV inválido para pruebas"""
        return """DR_NO,Date Rptd,AREA,AREA NAME
211507896,04/11/2021 12:00:00 AM,15,N Hollywood"""
    
    @pytest.fixture
    def temp_csv_file(self, sample_csv_content):
        """Crea un archivo CSV temporal para las pruebas"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(sample_csv_content)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    @pytest.fixture
    def invalid_temp_csv_file(self, invalid_csv_content):
        """Crea un archivo CSV inválido temporal"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(invalid_csv_content)
            temp_path = f.name
        
        yield temp_path
        
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    def test_validator_initialization(self, temp_csv_file):
        """Prueba la inicialización del validador"""
        validator = CrimeDataValidator(temp_csv_file)
        assert validator.csv_file_path == temp_csv_file
        assert validator.data == []
        assert validator.headers == []
    
    def test_load_data_success(self, temp_csv_file):
        """Prueba la carga exitosa de datos"""
        validator = CrimeDataValidator(temp_csv_file)
        result = validator.load_data()
        
        assert result == True
        assert len(validator.data) == 3  # Tres filas de datos
        assert len(validator.headers) > 0
        assert 'DR_NO' in validator.headers
    
    def test_load_data_file_not_found(self):
        """Prueba con archivo inexistente"""
        validator = CrimeDataValidator("archivo_inexistente.csv")
        
        with pytest.raises(FileNotFoundError):
            validator.load_data()
    
    def test_validate_headers_success(self, temp_csv_file):
        """Prueba validación exitosa de headers"""
        validator = CrimeDataValidator(temp_csv_file)
        validator.load_data()
        
        is_valid, missing = validator.validate_headers()
        assert is_valid == True
        assert missing == []
    
    def test_validate_headers_missing_columns(self, invalid_temp_csv_file):
        """Prueba validación con columnas faltantes"""
        validator = CrimeDataValidator(invalid_temp_csv_file)
        validator.load_data()
        
        is_valid, missing = validator.validate_headers()
        assert is_valid == False
        assert len(missing) > 0
        assert 'DATE OCC' in missing
        assert 'Crm Cd' in missing
    
    def test_validate_dr_no_unique(self, temp_csv_file):
        """Prueba validación de DR_NO únicos"""
        validator = CrimeDataValidator(temp_csv_file)
        validator.load_data()
        
        result = validator.validate_dr_no_unique()
        assert result == True
    
    def test_validate_coordinates(self, temp_csv_file):
        """Prueba validación de coordenadas"""
        validator = CrimeDataValidator(temp_csv_file)
        validator.load_data()
        
        coord_stats = validator.validate_coordinates()
        
        assert 'total_rows' in coord_stats
        assert 'valid_coordinates' in coord_stats
        assert 'invalid_coordinates' in coord_stats
        assert 'missing_coordinates' in coord_stats
        assert 'valid_percentage' in coord_stats
        
        assert coord_stats['total_rows'] == 3
        assert coord_stats['valid_coordinates'] >= 0
        assert coord_stats['valid_percentage'] >= 0
    
    def test_validate_victim_age(self, temp_csv_file):
        """Prueba validación de edades de víctimas"""
        validator = CrimeDataValidator(temp_csv_file)
        validator.load_data()
        
        age_stats = validator.validate_victim_age()
        
        assert 'valid_ages' in age_stats
        assert 'invalid_ages' in age_stats
        assert 'missing_ages' in age_stats
        
        # Verificar que al menos las edades válidas del ejemplo sean contadas
        assert age_stats['valid_ages'] >= 2  # 31 y 32 son válidas
    
    def test_validate_sex_values(self, temp_csv_file):
        """Prueba validación de valores de sexo"""
        validator = CrimeDataValidator(temp_csv_file)
        validator.load_data()
        
        result = validator.validate_sex_values()
        assert result == True
    
    def test_get_basic_stats(self, temp_csv_file):
        """Prueba obtención de estadísticas básicas"""
        validator = CrimeDataValidator(temp_csv_file)
        validator.load_data()
        
        stats = validator.get_basic_stats()
        
        assert 'total_rows' in stats
        assert 'total_columns' in stats
        assert 'columns' in stats
        assert 'sample_row' in stats
        
        assert stats['total_rows'] == 3
        assert stats['total_columns'] > 0
        assert isinstance(stats['columns'], list)
        assert isinstance(stats['sample_row'], dict)

# Pruebas de integración
class TestCrimeDataValidatorIntegration:
    """Pruebas de integración para validación completa"""
    
    def test_complete_validation_workflow(self):
        """Prueba el flujo completo de validación"""
        # Esta prueba requiere el archivo real
        csv_path = "data/Crime_Data_from_2020_to_Present.csv"
        
        if not os.path.exists(csv_path):
            pytest.skip(f"Archivo de datos no encontrado: {csv_path}")
        
        validator = CrimeDataValidator(csv_path)
        
        # Cargar datos
        assert validator.load_data() == True
        
        # Validar headers
        headers_valid, missing = validator.validate_headers()
        if not headers_valid:
            print(f"Columnas faltantes: {missing}")
        
        # Obtener estadísticas
        stats = validator.get_basic_stats()
        print(f"Total de filas: {stats['total_rows']}")
        print(f"Total de columnas: {stats['total_columns']}")
        
        # Validar coordenadas
        coord_stats = validator.validate_coordinates()
        print(f"Coordenadas válidas: {coord_stats['valid_percentage']:.2f}%")
        
        # El test pasa si podemos ejecutar todas las validaciones sin errores
        assert stats['total_rows'] > 0
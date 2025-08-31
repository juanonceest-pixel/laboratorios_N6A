"""
Pruebas unitarias para el módulo text_utils.
Incluye casos válidos, casos límite y manejo de errores.
"""

import pytest
from src.text_utils import clean_text, count_words, is_valid_email

class TestCleanText:
    """Pruebas para la función clean_text"""
    
    def test_clean_text_normal(self):
        """Prueba con texto normal"""
        result = clean_text("  Hola Mundo  ")
        assert result == "hola mundo"
    
    def test_clean_text_empty_string(self):
        """Prueba con cadena vacía"""
        result = clean_text("")
        assert result == ""
    
    def test_clean_text_only_spaces(self):
        """Prueba con solo espacios"""
        result = clean_text("   ")
        assert result == ""
    
    def test_clean_text_mixed_case(self):
        """Prueba con texto en mayúsculas y minúsculas"""
        result = clean_text("PyThOn Es GeNiAl")
        assert result == "python es genial"
    
    def test_clean_text_invalid_type(self):
        """Prueba con tipo de dato inválido"""
        with pytest.raises(TypeError):
            clean_text(123)
        
        with pytest.raises(TypeError):
            clean_text(None)

class TestCountWords:
    """Pruebas para la función count_words"""
    
    def test_count_words_normal(self):
        """Prueba con texto normal"""
        result = count_words("Hola mundo desde Python")
        assert result == 4
    
    def test_count_words_single_word(self):
        """Prueba con una sola palabra"""
        result = count_words("Python")
        assert result == 1
    
    def test_count_words_empty_string(self):
        """Prueba con cadena vacía"""
        result = count_words("")
        assert result == 0
    
    def test_count_words_only_spaces(self):
        """Prueba con solo espacios"""
        result = count_words("   ")
        assert result == 0
    
    def test_count_words_multiple_spaces(self):
        """Prueba con múltiples espacios entre palabras"""
        result = count_words("palabra1    palabra2     palabra3")
        assert result == 3
    
    def test_count_words_invalid_type(self):
        """Prueba con tipo de dato inválido"""
        with pytest.raises(TypeError):
            count_words(123)

class TestIsValidEmail:
    """Pruebas para la función is_valid_email"""
    
    def test_valid_emails(self):
        """Prueba con emails válidos"""
        valid_emails = [
            "test@example.com",
            "usuario@dominio.co",
            "admin@test.org",
            "user.name@domain.com.co"
        ]
        
        for email in valid_emails:
            assert is_valid_email(email) == True
    
    def test_invalid_emails(self):
        """Prueba con emails inválidos"""
        invalid_emails = [
            "test",                    # Sin @
            "@example.com",            # Sin parte local
            "test@",                   # Sin dominio
            "test@@example.com",       # Doble @
            "test@example",            # Sin punto en dominio
            "",                        # Cadena vacía
            "test @example.com"        # Con espacios
        ]
        
        for email in invalid_emails:
            assert is_valid_email(email) == False
    
    def test_email_with_spaces(self):
        """Prueba con emails que tienen espacios (se deben limpiar)"""
        assert is_valid_email("  test@example.com  ") == True
    
    def test_email_invalid_type(self):
        """Prueba con tipo de dato inválido"""
        with pytest.raises(TypeError):
            is_valid_email(123)
        
        with pytest.raises(TypeError):
            is_valid_email(None)
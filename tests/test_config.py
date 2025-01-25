from app.core.config import settings

def test_settings_load():
    assert hasattr(settings, 'DATABASE_URL')
    assert hasattr(settings, 'LATITUDE')
    assert hasattr(settings, 'LONGITUDE')
    assert settings.LATITUDE == 40.7128
    assert settings.LONGITUDE == -74.0060 
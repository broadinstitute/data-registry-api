"""
Tests for format detection and basic loading.
"""
import pytest
from pathlib import Path
from calr.loaders import detect_format

TEST_DATA_DIR = Path(__file__).parent.parent / 'test_data'


def test_detect_tse_format():
    """Test TSE format detection."""
    tse_file = TEST_DATA_DIR / 'calr_tse.csv'
    assert detect_format(tse_file) == 'tse'


def test_detect_calr_format():
    """Test CalR format detection."""
    calr_file = TEST_DATA_DIR / 'calr_test_data.csv'
    assert detect_format(calr_file) == 'calr'


def test_format_detection_error():
    """Test that unknown format raises ValueError."""
    # Create a temp file with unknown format
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("unknown,format,header\n")
        f.write("1,2,3\n")
        temp_path = Path(f.name)
    
    try:
        with pytest.raises(ValueError, match="Unable to detect format"):
            detect_format(temp_path)
    finally:
        temp_path.unlink()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

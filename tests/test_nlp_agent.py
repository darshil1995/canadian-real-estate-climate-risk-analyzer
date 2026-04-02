from unittest.mock import patch
from src.models.nlp_agent import analyze_red_flags


def test_analyze_red_flags_short_description():
    """Verify that short descriptions are skipped."""
    assert analyze_red_flags("Too short") == "N/A"


@patch('ollama.chat')
def test_analyze_red_flags_mock_output(mock_ollama):
    """Verify that the agent strips prefixes correctly."""
    # Mock a response that includes the 'fluff' we want to strip
    mock_ollama.return_value = {
        'message': {'content': 'Risk Keywords: Mold, Water Damage'}
    }

    result = analyze_red_flags("This house has a moldy basement and water leaks.")
    assert "Mold" in result
    assert "Risk Keywords:" not in result  # Ensure our cleaning logic works
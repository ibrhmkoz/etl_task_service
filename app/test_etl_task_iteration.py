from io import StringIO
from unittest.mock import MagicMock, patch

from app.etl_task_iteration import ETLTaskIteration


def test_etl_task_iteration_calls_all_components():
    # Given
    mock_source = MagicMock()
    mock_transformer = MagicMock()
    mock_sink = MagicMock()
    etl_task = ETLTaskIteration(mock_source, mock_transformer, mock_sink)

    # When
    etl_task()

    # Then
    mock_source.extract.assert_called_once()
    mock_transformer.transform.assert_called_once()
    mock_sink.load.assert_called_once()


def test_etl_task_iteration_handles_exception_and_clears_resources():
    # Given
    mock_source = MagicMock()
    mock_transformer = MagicMock()
    mock_sink = MagicMock()
    mock_transformer.transform.side_effect = Exception("Transformation error")
    etl_task = ETLTaskIteration(mock_source, mock_transformer, mock_sink)

    # When
    with patch('sys.stdout', new=StringIO()) as fake_out:
        etl_task()
        output = fake_out.getvalue()

    # Then
    assert "Error during ETL iteration: Transformation error" in output
    mock_source.close.assert_called_once()
    mock_sink.close.assert_called_once()
    mock_source.extract.assert_called_once()
    mock_sink.load.assert_not_called()

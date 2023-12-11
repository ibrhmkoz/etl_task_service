from io import StringIO
from unittest.mock import MagicMock, patch

from app.etl_task_iteration import ETLTaskIteration


# Tests
def test_etl_task_iteration_calls_all_components():
    mock_source = MagicMock()
    mock_transformer = MagicMock()
    mock_sink = MagicMock()

    etl_task = ETLTaskIteration(mock_source, mock_transformer, mock_sink)
    etl_task()

    mock_source.extract.assert_called_once()
    mock_transformer.transform.assert_called_once()
    mock_sink.load.assert_called_once()


def test_etl_task_iteration_handles_exception_and_clears_resources():
    mock_source = MagicMock()
    mock_transformer = MagicMock()
    mock_sink = MagicMock()

    # Set up the transform method to raise an exception
    mock_transformer.transform.side_effect = Exception("Transformation error")

    etl_task = ETLTaskIteration(mock_source, mock_transformer, mock_sink)

    # Capture the standard output
    with patch('sys.stdout', new=StringIO()) as fake_out:
        etl_task()
        assert "Error during ETL iteration: Transformation error" in fake_out.getvalue()

    # Verify that resources were cleared after exception
    mock_source.close.assert_called_once()
    mock_sink.close.assert_called_once()

    # Verify the extract method was called but load was not due to the exception
    mock_source.extract.assert_called_once()
    mock_sink.load.assert_not_called()

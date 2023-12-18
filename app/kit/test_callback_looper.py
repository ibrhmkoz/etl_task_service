# Tests
from unittest.mock import MagicMock

from app.kit.callback_looper import CallbackLooper


def test_callback_looper_calls_callback():
    # Given
    mock_callback = MagicMock()
    so_long_as_condition = [3, 2, 1]

    def so_long_as():
        return bool(so_long_as_condition.pop() if so_long_as_condition else False)

    looper = CallbackLooper(mock_callback, so_long_as, interval=0.1)

    # When
    looper.start_loop()

    # Then
    assert mock_callback.call_count == 3


def test_callback_looper_stops_early():
    # Given
    mock_callback = MagicMock()
    so_long_as_condition = [1]

    def so_long_as():
        return bool(so_long_as_condition.pop() if so_long_as_condition else False)

    looper = CallbackLooper(mock_callback, so_long_as, interval=0.1)

    # When
    looper.start_loop()

    # Then
    assert mock_callback.call_count == 1

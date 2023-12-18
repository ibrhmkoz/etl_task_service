import pytest

from app.kit.lib import pipe, retry


def multiply_by_two(x):
    return x * 2


def add_three(x):
    return x + 3


def concat_strings(a, b, c=""):
    return a + b + c


# Tests
def test_pipe_numeric_operations():
    func = pipe(multiply_by_two, add_three)
    assert func(5) == 13


def test_pipe_with_multiple_arguments():
    func = pipe(concat_strings)
    assert func("Hello, ", "World", c="!") == "Hello, World!"


def test_retry_successful_execution():
    successful_operation = lambda: "Success"
    retried_operation = retry(successful_operation, 3)
    assert retried_operation() == "Success"


def test_retry_eventually_succeeds():
    attempts = 0

    def sometimes_fails():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("Failure")
        return "Success"

    retried_operation = retry(sometimes_fails, 5)
    assert retried_operation() == "Success"


def test_retry_exceeds_max_retries():
    always_fails = lambda: (_ for _ in ()).throw(RuntimeError("Failure"))
    retried_operation = retry(always_fails, 2)
    with pytest.raises(RuntimeError) as exc_info:
        retried_operation()
    assert "Failure" in str(exc_info.value)
    assert "Operation failed after 2 retries. Errors: Failure; Failure" == str(exc_info.value)


def test_retry_zero_retries_allowed():
    operation = lambda: "Should not be executed"
    retried_operation = retry(operation, 0)
    assert retried_operation() is None

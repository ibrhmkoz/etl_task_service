from app.kit.lib import pipe


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

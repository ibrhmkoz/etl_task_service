def pipe(*functions):
    def piped_function(*args, **kwargs):
        result = args
        for function in functions:
            if isinstance(result, tuple):
                result = function(*result, **kwargs)
            else:
                result = function(result, **kwargs)
        return result

    return piped_function

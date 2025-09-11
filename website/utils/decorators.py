from functools import wraps


def replace_chars(parameter_name, old_char, new_char):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if parameter_name in kwargs:
                kwargs[parameter_name] = kwargs[parameter_name].replace(
                    old_char, new_char
                )

            result = f(*args, **kwargs)
            return result

        return decorated_function

    return decorator

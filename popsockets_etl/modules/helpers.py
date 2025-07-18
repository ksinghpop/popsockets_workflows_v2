import io
import sys
import functools

def capture_logs(func):
    """
    Decorator that captures stdout prints and returns them with the result.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        buffer = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = buffer
        try:
            result = func(*args, **kwargs)
            output = buffer.getvalue()
            return {
                "result": result,
                "logs": output
            }
        finally:
            sys.stdout = old_stdout
    return wrapper
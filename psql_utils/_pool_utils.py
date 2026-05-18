def is_closing_runtime_error(err: RuntimeError) -> bool:
    """Return True when a RuntimeError indicates a closing connection/pool."""
    return 'closing' in str(err)


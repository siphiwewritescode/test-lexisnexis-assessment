import logging


def get_logger(name: str) -> logging.Logger:
    """
    This will create and return a logger with a consistent format.
    Format: YYYY-MM-DD HH:MM:SS | level name | logger name | message
    Prevents double-logging by checking for existing handlers first. 
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
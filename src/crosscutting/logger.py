import logging


def get_logger(name: str) -> logging.Logger:
    """
    Create and configure a logger with the given name.

    This function sets up basic configuration for the logging system with a standard
    format and INFO level logging. The format includes timestamp, log level, and
    the message. It then returns a logger instance with the specified name.

    Args:
        name (str): The name of the logger to create. This is typically __name__
            when used within a module, but can be any string identifier.

    Returns:
        logging.Logger: A configured logger instance with the specified name.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(name)


logger = get_logger(__name__)

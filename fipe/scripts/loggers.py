import logging

# Create the Formatter
formatter = logging.Formatter(
    "%(asctime)s :-: %(levelname)s :-: %(name)s :-: %(message)s"
)


# Create Handlers and set the ACCORDING LEVEL
def get_stream_handler():
    stream_handler = logging.StreamHandler()
    # Add Formatter to Handlers
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    return stream_handler


# Create Logger and set its level
def get_logger(logger_name: str):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    # Add the Handlers to Logger
    logger.addHandler(get_stream_handler())
    return logger


if __name__ == "__main__":
    # Testing
    logger = get_logger("app_logger")
    logger.info("Hi from Logging!")

import logging
import os


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    os.makedirs("data/logs", exist_ok=True)
    log_file = "data/logs/uwss.log"

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # tránh nhân đôi handler khi gọi nhiều lần
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(level)

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        ch.setLevel(level)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger

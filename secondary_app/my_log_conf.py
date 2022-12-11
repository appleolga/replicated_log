log_config = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(asctime)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",

        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "default",
            "level": "DEBUG",
            "filename": "logs/secondary.log",
            "mode": "w"
        }
    },
    "loggers": {
        "rl_logger": {"handlers": ["default", "file"], "level": "DEBUG", "filename": 'log/secondary.log'},
    },
    "file_handler": {
            "level": "DEBUG",
            "filename": "/log/secondary.log",
            "class": "logging.FileHandler",
            "formatter": "standard"
    }
}

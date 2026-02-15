#!/usr/bin/env python3
import os
import sys
import json


# ========================================================================================================================================================================
# VARIÁVEIS DE AMBIENTE
# ========================================================================================================================================================================
SERVER_HOST = os.environ.get("SERVER_HOST")
SERVER_IP = os.environ.get("SERVER_IP")


# ========================================================================================================================================================================
# FORMATAÇÃO LOG
# ========================================================================================================================================================================
def main():
    
    logging_config_dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "customFormatter": {
                "format": '%(asctime)s | ' + SERVER_IP + ' | ' + SERVER_HOST + ' | uvicorn | %(levelname)s | {"message": "%(message)s"}'
            }
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "customFormatter",
                "stream": "ext://sys.stdout"
            }
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False
            },
            "uvicorn.access": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False
            }
        },
        "root": {
            "handlers": ["default"],
            "level": "INFO"
        }
    }


    config_path = "/app/logging_config.json"
    with open(config_path, "w") as fp:
        json.dump(logging_config_dict, fp)

  
    command = sys.argv[1:]
    
    if not command:
        raise ValueError("Nenhum comando especificado no CMD do Dockerfile.")

    # Injeta --log-config se não estiver presente
    if "--log-config" not in command:
        command += ["--log-config", config_path]

    os.execvp(command[0], command)


# ========================================================================================================================================================================
# MAIN
# ========================================================================================================================================================================
if __name__ == "__main__":
    main()
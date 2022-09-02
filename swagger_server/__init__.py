import configparser
import os
from pathlib import Path

current_dir = Path(__file__).resolve().parents[1]
server_dir = Path(__file__).resolve().parents[0]
cfg_path = f"{current_dir}/config/"
connexion_ini_path = f"{server_dir}/ini/"


def database_values(config, section_name="postgres"):
    config[section_name] = {}
    config[section_name]["host"] = os.getenv("POSTGRES_HOST", "127.0.0.1")
    config[section_name]["port"] = os.getenv("POSTGRES_PORT", "5432")
    config[section_name]["database"] = os.getenv("POSTGRES_DATABASE", "database")
    config[section_name]["username"] = os.getenv("POSTGRES_USERNAME", "postgres")
    config[section_name]["password"] = os.getenv("POSTGRES_PASSWORD", "postgres")


def connexion_values(config, section_name="connexion"):
    config[section_name] = {}
    config[section_name]["server"] = os.getenv("CONNEXION_SERVER", "")
    config[section_name]["debug"] = os.getenv("CONNEXION_DEBUG", "True")
    config[section_name]["port"] = os.getenv("CONNEXION_PORT", "5000")
    config[section_name]["keyfile"] = os.getenv("CONNEXION_KEY_FILE", "")
    config[section_name]["certfile"] = os.getenv("CONNEXION_CERT_FILE", "")


def sys_path_values(config, section_name="sys-path"):
    config[section_name] = {}
    config[section_name]["exposures"] = os.getenv("EXPOSURES", "./")
    config[section_name]["CONTROLLERS"] = os.getenv("CONTROLLERS", "./")


def create_ini_files():
    config = configparser.ConfigParser()
    database_values(config, section_name="postgres")

    keys_mapping = {
        "host": "POSTGRES_HOST",
        "port": "POSTGRES_PORT",
        "database": "POSTGRES_DATABASE",
        "username": "POSTGRES_USERNAME",
        "password": "POSTGRES_PASSWORD"
    }

    with open(f"{cfg_path}/database.ini", "w") as database_ini_file:
        config.write(database_ini_file)

    with open(f"{cfg_path}/database.cfg", "w") as database_cfg_file:
        for key, value in config.items("postgres"):
            line = f"export {keys_mapping[key]}={value}\n"
            database_cfg_file.write(line)

    connexion_values(config)
    sys_path_values(config)

    with open(f"{connexion_ini_path}/connexion.ini", "w") as connexion_ini_file:
        config.write(connexion_ini_file)


create_ini_files()



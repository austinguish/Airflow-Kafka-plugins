import configparser


def read_config(conf_file):
    config = configparser.ConfigParser()
    config.read(conf_file)
    return config

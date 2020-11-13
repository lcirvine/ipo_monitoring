import configparser

config = configparser.ConfigParser()
config.read('exchanges.ini')

exchanges = {}
for sect in config.sections():
    exchanges[sect] = {}
    for (k, v) in config.items(sect):
        exchanges[sect][k] = v





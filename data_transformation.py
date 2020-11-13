import configparser


def asx():
    print('ASX')


def euronext():
    print('Euronext')


def hk():
    print('HK')


def iposcoop():
    print('IPOScoop')


def korea():
    print('Korea')


def lse():
    print('LSE')


def madrid():
    print('Madrid')


def nasdaq():
    print('Nasdaq')


def nyse():
    print('NYSE')


def shanghai():
    print('Shanghai')


def shenzhen():
    print('Shenzhen')


def swiss():
    print('Swiss')


def tokyo():
    print('Tokyo')


def frankfurt():
    print('Frankfurt')


config = configparser.ConfigParser()
config.read('exchanges.ini')

exchanges = {}
for sect in config.sections():
    exchanges[sect] = {}
    for (k, v) in config.items(sect):
        exchanges[sect][k] = v


def dt_picker(exch, funct):
    exchanges[exch].get(funct)()
    # exchanges.get(exch['dt_funct'], lambda: 'invalid')()


for k, v in exchanges.items():
    dt_picker(k, exchanges[k]['dt_funct'])
    print(exchanges[k]['url'])


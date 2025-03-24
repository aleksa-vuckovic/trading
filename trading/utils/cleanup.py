from trading.providers import Yahoo

def cleanBadYahooInfos():
    key = 'BAD_YFINANCE_TICKER'

    providers = [Yahoo(it) for it in ['db', 'file']] #type: ignore
    for provider in providers:
        for key in provider.info_persistor.keys():
            info: dict = provider.info_persistor.read(key)
            print(info)

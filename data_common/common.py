def cxxtSymbol_to_exchangeSymbol(exchange, symbol):
    if exchange == "bybit" or exchange == "binance":
        isymbol = symbol.split(":")[0].replace(f"/", "")
    elif exchange == "okx":
        isymbol = symbol.split(":")[0].replace(f"/", "-")
    else:
        raise ValueError(f"exhange must be in bybit, okx, binance.")
    return isymbol

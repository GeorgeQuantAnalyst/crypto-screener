SELECT_OHLC_ROWS = """
    select date, open, high, low, close
    from '{}_{}_{}'
    order by date asc
"""
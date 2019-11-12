from ..wrappers import checker


@checker('dask df converted in pandas df')
def compute_df(df):
    return df.compute()
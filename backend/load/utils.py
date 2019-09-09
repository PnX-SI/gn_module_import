from ..wrappers import checker



@checker('dask dataframe computed')
def compute_df(df):
    return df.compute()
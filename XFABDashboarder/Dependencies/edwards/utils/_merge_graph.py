import os
import warnings
import pandas as pd


def merge_graph(data: dict | list,
                path: str,
                reverse: bool = False) -> None:
    """
    Merge same type of data output from dp function of multi systems.

    Parameters
    ----------
    data : Dictionary or list
        Each value is a pd.DataFrame or a model object that has
        a pd.DataFrame attribute 'vis_as_df_'. The specification of
        the pd.DataFrame can be found from function `create_vis_df`.
    path : str
    reverse : bool, optional
        The default is False.

    Returns
    -------
    None.

    """
    df = []
    if isinstance(data, dict):
        if all([isinstance(v, pd.DataFrame) for k, v in data.items()]):
            for k, v in data.items():
                df.append(v)
        else:
            for k, v in data.items():
                df.append(v.vis_as_df_)
    elif isinstance(data, list):
        if all([isinstance(v, pd.DataFrame) for v in data]):
            for v in data:
                df.append(v)
        else:
            for v in data:
                df.append(v.vis_as_df_)
    df = pd.concat(df)

    systems = list(df['system'].drop_duplicates().values)
    subplots = list(df['subplot'].drop_duplicates().values)
    mapping = pd.DataFrame({'system': systems,
                            'subplot': list(range(1, 1 + len(systems)))})

    if not os.path.exists(path):
        os.makedirs(path)

    # Filename is defined as subplot index plus
    # a. f'{description}' within 'title', if 'title' exists and not empty,
    #    'title' may have two formats
    #    <1> f'{description}'
    #    <2> f'{system_name}' + '{parameter_number}' + ' - ' + '{description}'
    # b. or parameter, otherwise
    for i, i_subplot in enumerate(subplots):
        df1 = df[df['subplot'] == i_subplot]
        df2 = df1.drop('subplot', axis=1, inplace=False)
        df3 = df2.merge(mapping, how='left', on='system')
        filename = df3.parameter[0]
        if 'title' in df3.columns:
            if len(df3.title.dropna()) > 0:
                if all([' - ' in x for x in df3.title.dropna()]):
                    y = [x.split(' - ')[1] for x in df3.title.dropna()]
                    filename = y[0]
                    if len(list(set(y))) > 1:
                        warnings.warn('Inconsistent titles!')
                else:
                    filename = df3.title.dropna()[0]
                    if len(list(set(list(df3.title.dropna())))) > 1:
                        warnings.warn('Inconsistent titles!')
        df3.to_parquet(f'{path}/{i_subplot}_{filename}.parquet')

    return None

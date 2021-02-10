import requests
import json
import pandas as pd 

def retreive_raw_data(per_page = 2235,
                      only = 'data',
                      tabs = ["overview", "returns", "fund-flows", 
                              "expenses", "esg", "dividends", 
                              "risk", "holdings", "taxes", 
                              "technicals", "analysis", "realtime-ratings"]
                  ):
    """
    Returns a raw dataframe containig public information of ETFdb's screener tool
    
    Args:
        per_page (int): number of records to display per page, default 2235 
        only (str): which information to retreive, metadata or data, default 'data'
        tabs (list of str): which website groups of information to download, default all
    
    Returns:
        data (pandas dataframe): raw dataframe containing information on ETFs just as it is received
    """

    data = pd.DataFrame()

    try:
        for tab in tabs:
            payload = __set_payload(per_page = per_page, only = only, tab = tab)
            json = __get_json(payload)
            json = __clean_json(json)
            data = __build_dataframe(json, data) 


    except ValueError as e:
        print(e.message)
    
    return data


def clean_dataframe(data):
    """
    Returns a cleaned dataframe.
    Columns with no useful information are dropped.
    When possible values are converted to datetime and float.

    Returns:
        data (pandas dataframe): cleaned dataframe containing public information on ETFs
    """

    data = __extract_from_dict(data)
    data = __drop_columns(data)
    data = __convert2float(data)
    data = __convert2datetime(data)

    return data

def download_clean_public_data():
    """
    Downloads a cleaned dataframe with all public information of ETFdb screener.

    Returns:
        data (pandas dataframe): cleaned dataframe containing public information on ETFs
    """

    return clean_dataframe(retreive_raw_data())

def __set_payload(tab, per_page, only):

    return {'per_page': per_page, 'only': only, 'tab': tab }


def __get_json(payload):

    url = 'https://etfdb.com/api/screener/'
    status = 0

    while(status != 200):
        try:
            r = requests.post(url, data=json.dumps(payload), json=True)
            if r.status_code == 200:
                print(str(payload['tab']) + ' downloaded successfully.')
        except:
            print('Connection error for ' + str(payload['tab']) + '. Trying again...')

        status = r.status_code

    return r.json()


def __clean_json(json_data):

    return json.loads(str(json_data).replace("' ", '" ')
                                    .replace(" '", ' "')
                                    .replace("['", '["')
                                    .replace("']", '"]')
                                    .replace('\\', '\\\\')
                                    .replace("',", '",')
                                    .replace("{'", '{"')
                                    .replace(":'", ':"')
                                    .replace("':", '":')
                                    .replace("'}", '"}'))


def __build_dataframe(json, data):

    def adapt_json(json, dt):
        dt = pd.DataFrame(json['data'])
        dt['symbol'] = pd.DataFrame(dt['symbol'].to_dict()).transpose()[['text']]
        return dt

    if data.size == 0:
        data = adapt_json(json, data)
    else:
        dt = adapt_json(json, data)
        cols_to_use = dt.columns.difference(data.columns)
        data = data.merge(dt[cols_to_use], left_index=True, right_index=True, how='outer')
    
    return data
    

def __extract_from_dict(data):

    def clean(col, data, name):
        return pd.DataFrame(data[col].to_dict()).transpose()[[name]]

    for col in data.columns:
        if 'text' in data[col][0]:
            data[col] = clean(col, data, 'text')
        elif 'type' in data[col][0]:
            data[col] = clean(col, data, 'type')

    return data


def __drop_columns(data):

    data = data.drop('head_to_head', axis=1)

    fields = ['restricted', 'N/A', 'View', 'Advanced']

    for col in data.columns:
        for field in fields:
            data[col] = data[col].replace(field, np.NaN)
        if data[col].isnull().all():
            data = data.drop(col, axis=1)
    
    return data


def __convert2float(data):

    symb = ['%', '$']

    for col in data.columns:
        for sy in symb:
            try:
                if sy in data[col][0]:
                    data[col] = data[col].str.replace(sy, '')
            except:
                index = data[col].first_valid_index().max()
                if sy in data.loc[index][col]:
                    data[col] = data[col].str.replace(sy, '') 

        data[col] = data[col].str.replace(',', '')
        try : 
            data[col] = data[col].astype('float')
        except:
            continue
    
    return data


def __convert2datetime(data):

    data['dividend_date'] = pd.to_datetime(data['dividend_date'], format = '%Y-%m-%d', errors = 'coerce')

    return data
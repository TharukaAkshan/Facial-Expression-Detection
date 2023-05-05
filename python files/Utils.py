import os
import sys

import pandas as pd
import pyodbc
import yaml
import numpy as np
import datetime
import logging

from multipledispatch import dispatch


# ==============================================================================================#
# ======================================== Common ==============================================#
# ==============================================================================================#
@dispatch(str)
def load_config(file_path):
    """
    Load credential form a YAML file

    Parameters
    ----------
    cred_file_path : String
        Path the credential file
    Returns
    -------
    Credentials : Dictionary
        Loaded credentials
    """
    conf = {}
    with open(file_path, "r") as conf_file:
        try:
            conf = yaml.safe_load(conf_file)
        except yaml.YAMLError as exc:
            print(exc)
            sys.exit(0)
    return conf


@dispatch(object, object)
def get_execution_time(start, end):
    """
    Generate the running time in the format of: HH:MM:SS

    Parameters
    ----------
    start : datetime.datetime
        Start time
    end : datetime.datetime
        End time
    returns
    -------
    Time difference : datetime.datetime
        Running time in the format of: HH:MM:SS

    """
    return str(datetime.timedelta(seconds=(end - start))).split(".")[0]


# ==============================================================================================#
# ===================================== DB Connetion ===========================================#
# ==============================================================================================#
@dispatch(str, str, str, str)
def get_mssql_db_connection(server, db_name, username, password):
    """
    Connect to a given database

    Parameters
    ----------
    server : string
        Database server IP
    db_name : string
        Intended database name
    username : string
        Login username
    password : string
        Login password
    returns
    -------
     connection : MSSQL db connection
        Connection object to the given db
    """
    return pyodbc.connect(
        'DRIVER=SQL Server;SERVER={};DATABASE={};UID={};PWD={}'.format(server, db_name, username, password))


@dispatch(str, str)
def get_mssql_db_connection(server, db_name):
    """
    Connect to a given database using windows authentication

    Parameters
    ----------
    server : string
        Database server IP
    db_name : string
        Intended database name
    returns
    -------
     connection : MSSQL db connection
        Connection object to the given db
    """
    return pyodbc.connect('Driver=SQL Server;SERVER={};DATABASE={};Trusted_Connection=yes;'.format(server, db_name))


# ==============================================================================================#
# ======================================= DB Read ==============================================#
# ==============================================================================================#
@dispatch(object, str)
def read_from_mssql_db(db_con, sql):
    """
    Read data from a given table

    Parameters
    ----------
    db_con : connection
        Database connection
    sql: string
        Custom select sql query
    returns
    -------
    data: pandas.DataFrame
        Loaded dataset
    """
    cursor = db_con.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()

    cols = []
    for i, _ in enumerate(cursor.description):
        cols.append(cursor.description[i][0])
    if len(data) > 0:
        return pd.DataFrame(np.array(data), columns=cols)
    else:
        return pd.DataFrame(columns=cols)


@dispatch(object, str, str, str, str)
def read_from_mssql_db(server, db_name, username, password, sql):
    """
    Overloading function which reads data from a given table

    Parameters
    ----------
    server : string
        Database server IP
    db_name : string
        Intended database name
    username : string
        Login username
    password : string
        Login password
    sql: string
        Custom select sql query
    returns
    -------
    data: pandas.DataFrame
        Loaded dataset
    """
    conn = conn = get_mssql_db_connection(server, db_name, username, password)
    data = read_from_mssql_db(conn, sql)
    conn.close()
    return data


@dispatch(object, str, str)
def read_from_mssql_db(server, db_name, sql):
    """
    Overloading function which reads data using windows authentication

    Parameters
    ----------
    server : string
        Database server IP
    db_name : string
        Intended database name
    sql: string
        Custom select sql query
    returns
    -------
    data: pandas.DataFrame
        Loaded dataset
    """
    conn = get_mssql_db_connection(server, db_name)
    data = read_from_mssql_db(conn, sql)
    conn.close()
    return data


# ==============================================================================================#
# ======================================= DB Write =============================================#
# ==============================================================================================#
@dispatch(object, str, list, object)
def write_to_mssql_db(db_con, table_name, column_list, value_df):
    """
    Write bulk rows to a given database connection

    Parameters
    ----------
    db_con : connection
        Database connection
    table_name : string
        Intended table name
    column_list : list
        List of column names of the intended table
    value_df : Pandas.DataFrame
        DataFrame of values
    returns
    -------
        None
    """
    if value_df.shape[0] == 0:
        raise ValueError("Data frmae is empty")
    cursor = db_con.cursor()
    cursor.executemany("INSERT INTO {} ({}) VALUES ({})".format(table_name,
                                                                ','.join(column_list),
                                                                ','.join(['?' for i in range(value_df.shape[1])])),
                       value_df.values.tolist())
    cursor.commit()


@dispatch(str, str, str, str, str, list, object)
def write_to_mssql_db(server, db_name, username, password, table_name, column_list, value_df):
    """
    Overloading function which writes bulk rows to a given database connection

    Parameters
    ----------
    server : string
        Database server IP
    db_name : string
        Intended database name
    username : string
        Login username
    password : string
        Login password
    table_name : string
        Intended table name
    column_list : list
        List of column names of the intended table
    value_df : Pandas.DataFrame
        DataFrame of values
    returns
    -------
        None
    """
    conn = get_mssql_db_connection(server, db_name, username, password)
    write_to_mssql_db(conn, table_name, column_list, value_df)
    conn.close()


@dispatch(str, str, str, list, object)
def write_to_mssql_db(server, db_name, table_name, column_list, value_df):
    """
    Write bulk rows to a given database using windows authentication

    Parameters
    ----------
    server : string
        Database server IP
    db_name : string
        Intended database name
    table_name : string
        Intended table name
    column_list : list
        List of column names of the intended table
    value_df : Pandas.DataFrame
        DataFrame of values
    returns
    -------
        None
    """
    conn = get_mssql_db_connection(server, db_name)
    write_to_mssql_db(conn, table_name, column_list, value_df)
    conn.close()


def get_logger(logging_path, logger_name, write_to_file=True, write_to_console=True, debug=False):
    """
    Write bulk rows to a given database using windows authentication

    Parameters
    ----------
    logging_path : string
        Path to write the log file
    logger_name : string
        Prefix name of the log file
    write_to_file : boolean
        Flag to indicate whether to write logs to a file or not. Default: True
    write_to_console : boolean
        Flag to indicate whether to write logs to console or not. Default: True
    debug : boolean
        Flag to indicate whether to enable debug logs or not. Default: False
    returns
    -------
        Logger object
    """
    log_path = os.path.join(logging_path,'{}_{}.log'.format(logger_name, datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S')))
    logFormatter = logging.Formatter(
        '%(asctime)s |%(levelname)-5s |%(filename)-8s |%(lineno)-4s|%(funcName)-15s |%(message)s', "%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger(log_path)
    logger.propagate = False
    logger.setLevel(logging.INFO)

    if not write_to_file and write_to_console:
        raise ValueError("Need to enable either file writing or console writing")

    if write_to_file:
        fileHandler = logging.FileHandler(log_path, mode='w')
        fileHandler.setFormatter(logFormatter)
        logger.addHandler(fileHandler)
    if write_to_console:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        logger.addHandler(consoleHandler)
    if debug:
        logger.setLevel(logging.DEBUG)
    return logger
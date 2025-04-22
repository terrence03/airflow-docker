import sqlite3
from pathlib import Path
import pandas as pd


def load_data(db_path: str, table_name: str, clause: str = None) -> pd.DataFrame:
    """Load data from sqlite3 database

    Parameters
    ----------
    db_path : str
        The file path of the sqlite3 database.
    table_name : str
        The name of the table in the database.
    clause : str, optional
        The condition to filter the data, by default None

    Returns
    -------
    pd.DataFrame
        The loaded data as a pandas DataFrame.
    """
    with sqlite3.connect(db_path) as conn:
        query = f"SELECT * FROM '{table_name}'"
        if clause:
            query += f" WHERE {clause}"
        data = pd.read_sql(query, conn)
    return data


def save_data(
    db_path: str, table_name: str, data: pd.DataFrame, check_exist: bool = True
) -> None:
    """Save data to sqlite3 database

    Parameters
    ----------
    db_path : str
        The file path of the sqlite3 database.
    table_name : str
        The name of the table in the database.
    data : pd.DataFrame
        The data to be saved.

    Notes
    -----
    The column names of the data should be the same as the column names of the table.
    Duplicate rows will be ignored.
    """
    assert Path(db_path).exists(), f"{db_path} does not exist."

    if check_exist:
        if data_is_exist(db_path, table_name, data):
            print(f"Data already exists in {table_name}, probably data not updated.")
            return

    with sqlite3.connect(db_path) as conn:
        assert table_name in [
            table[0]
            for table in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            )
        ], f"{table_name} does not exist in {db_path}."

        cur = conn.cursor()
        cur.executemany(
            f"""INSERT OR IGNORE INTO '{table_name}' ({"'"+"','".join(map(str, data.columns))+"'"}) \
                VALUES ({','.join(['?']*len(data.columns))})""",
            data.values,
        )
        conn.commit()
    conn.close()
    print(f"Data saved to {table_name}.")


def data_is_exist(db_path: str, table_name: str, data: pd.DataFrame) -> bool:
    """Check if data already exists in the database table,
    if any rows in the data already exist in the table, return True, otherwise return False.

    Parameters
    ----------
    db_path : str
        The file path of the sqlite3 database.
    table_name : str
        The name of the table in the database.
    data : pd.DataFrame
        The data to be checked.

    Returns
    -------
    bool
        True if any rows in the data already exist in the table, False otherwise.

    """
    with sqlite3.connect(db_path) as conn:
        db_last_row = pd.read_sql(
            f"SELECT * FROM '{table_name}' ORDER BY ROWID DESC LIMIT 1;", conn
        )
        db_last_row = db_last_row.astype(data.dtypes)
        _data = pd.concat([db_last_row, data], ignore_index=True)
        if _data.duplicated().any():
            return True
    return False

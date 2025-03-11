# %%
import sqlite3


def resort_oil_data_and_save_data(db_path: str, table_name: str, sort_by: str):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # build temp table
    c.execute(f"CREATE TABLE 'temp' AS SELECT * FROM '{table_name}' ORDER BY {sort_by}")
    c.execute(f"DROP TABLE '{table_name}'")
    c.execute(f"ALTER TABLE 'temp' RENAME TO '{table_name}'")

    c.execute("VACUUM")
    conn.commit()
    conn.close()


# %%
resort_oil_data_and_save_data(
    R"D:\OneDrive\WORK\Projects\monthly-oil-market-report\data\oilprice.db",
    "Weekly.Town",
    "week_id",
)

# %%

import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_int_trade_data_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create sequence for primary keys
    conn.sql("DROP SEQUENCE IF EXISTS int_trade_data_sequence;")
    conn.sql("CREATE SEQUENCE int_trade_data_sequence START 1;")

    # Create IntTradeData table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "inttradedata" (
            id INTEGER PRIMARY KEY DEFAULT nextval('int_trade_data_sequence'),
            trade_id INTEGER REFERENCES tradetable(id),
            hts_id INTEGER REFERENCES htstable(id),
            country_id INTEGER REFERENCES countrytable(id),
            data BIGINT DEFAULT 0,
            unit1_id INTEGER REFERENCES unittable(id),
            qty_1 BIGINT DEFAULT 0,
            unit2_id INTEGER REFERENCES unittable(id),
            qty_2 BIGINT DEFAULT 0,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def init_jp_trade_data_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create sequence for primary key
    conn.sql("DROP SEQUENCE IF EXISTS jp_trade_data_sequence;")
    conn.sql("CREATE SEQUENCE jp_trade_data_sequence START 1;")

    # Create JPTradeData table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "jptradedata" (
            id INTEGER PRIMARY KEY DEFAULT nextval('jp_trade_data_sequence'),
            trade_id INTEGER REFERENCES tradetable(id),
            hts_id INTEGER DEFAULT 1 REFERENCES htstable(id),
            country_id INTEGER REFERENCES countrytable(id),
            district_id INTEGER REFERENCES districttable(id),
            sitc_id INTEGER REFERENCES sitctable(id),
            naics_id INTEGER REFERENCES naicstable(id),
            data INTEGER DEFAULT 0,
            end_use_i INTEGER,
            end_use_e INTEGER,
            unit1_id INTEGER REFERENCES unittable(id),
            qty_1 BIGINT DEFAULT 0,
            unit2_id INTEGER REFERENCES unittable(id),
            qty_2 BIGINT DEFAULT 0,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def init_country_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS country_sequence;")
    conn.sql("CREATE SEQUENCE country_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "countrytable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('country_sequence'),
            cty_code TEXT,
            country_name TEXT
        );
        """
    )


def init_hts_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS hts_sequence;")
    conn.sql("CREATE SEQUENCE hts_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "htstable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('hts_sequence'),
            hts_code TEXT,
            hts_short_desc TEXT,
            hts_long_desc TEXT,
            agri_prod BOOLEAN
        );
        """
    )
    # conn.sql("INSERT INTO htstable VALUES(1, 'N/A', 'N/A', 'N/A', True)")


def init_sitc_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS sitc_sequence;")
    conn.sql("CREATE SEQUENCE sitc_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "sitctable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('sitc_sequence'),
            sitc_code TEXT,
            sitc_short_desc TEXT,
            sitc_long_desc TEXT
        );
        """
    )


def init_naics_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS naics_sequence;")
    conn.sql("CREATE SEQUENCE naics_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "naicstable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('naics_sequence'),
            naics_code TEXT,
            naics_description TEXT
        );
        """
    )


def init_trade_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS trade_sequence;")
    conn.sql("CREATE SEQUENCE trade_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "tradetable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('trade_sequence'),
            trade TEXT
        );
        """
    )
    conn.sql("INSERT INTO tradetable VALUES (1, 'Imports')")
    conn.sql("INSERT INTO tradetable VALUES (2, 'Exports')")


def init_district_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS district_sequence;")
    conn.sql("CREATE SEQUENCE district_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "districttable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('district_sequence'),
            district_code TEXT,
            district_desc TEXT
        );
        """
    )


def init_unit_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS unit_sequence;")
    conn.sql("CREATE SEQUENCE unit_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "unittable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('unit_sequence'),
            unit_code TEXT
        );
        """
    )


if __name__ == "__main__":
    db_path = "data.duckdb"
    init_country_table(db_path)
    init_hts_table(db_path)
    init_sitc_table(db_path)
    init_naics_table(db_path)
    init_district_table(db_path)
    init_unit_table(db_path)
    init_int_trade_data_table(db_path)
    init_trade_table(db_path)


if __name__ == "__main__":
    db_path = "data.ddb"

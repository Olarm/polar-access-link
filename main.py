import requests
import tomllib
import psycopg
import asyncio

from accesslink import AccessLink


TOKEN_FILENAME = "usertokens.yml"



def get_db_conn_string():
    config = get_config()
    return(f"""
        dbname={config["db"]}
        user={config["user"]}
        password={config["password"]}
    """)


async def insert_exercise():
    conn_str = get_db_conn_string()
    async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
        async with aconn.cursor() as acur:
            await acur.execute(
                "INSERT INTO test (num, data) VALUES (%s, %s)",
                (100, "abc'def"))
            await acur.execute("SELECT * FROM test")
            await acur.fetchone()
            # will return (1, 100, "abc'def")
            async for record in acur:
                print(record)


async def create_tables():
    conn_str = get_db_conn_string()
    async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
        async with aconn.cursor() as acur:
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS exercises (pk SERIAL PRIMARY KEY, start_time timestamp with time zone data jsonb);
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS recharge (pk SERIAL PRIMARY KEY, date date, data jsonb, )  
            """)



def get_config():
    path = "config.toml"
    with open(path, "rb") as f:
        config = tomllib.load(f)

    return config


def get_accesslink():
    config = get_config()
    accesslink = AccessLink(client_id=config['client_id'],
                        client_secret=config['client_secret'])
    return accesslink


def run():
    al = get_config()


if __name__ == "__main__":
    run()
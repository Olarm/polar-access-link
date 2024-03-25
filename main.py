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


async def insert_exercises(al, access_token, acur):
    exercises = al.get_exercises(access_token)
    for exercise in exercises:
        polar_id = exercise.get("id")
        start_time = exercise.get("start_time")
        await acur.execute(
            "INSERT INTO exercises (polar_id, start_time, data) VALUES (%s, %s, %s) on conflict do nothing",
            (polar_id, start_time, exercise))



async def get_all():
    al = get_accesslink()
    config = get_config()
    access_token = config.get("access_token")
    conn_str = get_db_conn_string()
    async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
        async with aconn.cursor() as acur:
            insert_exercises(al, access_token, acur)



async def create_tables():
    conn_str = get_db_conn_string()
    async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
        async with aconn.cursor() as acur:
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS exercises (
                    pk SERIAL PRIMARY KEY, 
                    polar_id varchar(20) not null unique, 
                    start_time timestamp with time zone not null, 
                    data jsonb not null unique
                );
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS recharge (pk SERIAL PRIMARY KEY, date date unique not null, data jsonb not null unique)  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS sleep (pk SERIAL PRIMARY KEY, date date unique not null, data jsonb not null unique)  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS cardio_load_level (pk SERIAL PRIMARY KEY, date date unique not null, data jsonb not null unique)  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS bedtime (pk SERIAL PRIMARY KEY, period_start_time timestamp with time zone not null unique, data jsonb not null unique)  
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
    create_tables
    al = get_config()


if __name__ == "__main__":
    run()
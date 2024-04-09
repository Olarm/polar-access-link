import tomllib
import json
import psycopg
import asyncio
import datetime
import logging

from accesslink import AccessLink
from polar_flow_scraper import login, get_yesterday, get_day


logging.basicConfig(
    filename='polar.log', 
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


TOKEN_FILENAME = "usertokens.yml"



def get_db_conn_string():
    config = get_config()
    return(f"""
        dbname={config["db"]["db"]}
        user={config["db"]["user"]}
        password={config["db"]["password"]}
        host={config["db"]["host"]}
    """)


async def insert_exercises(al, access_token, acur):
    exercises = al.get_exercises(access_token)
    for exercise in exercises:
        polar_id = exercise.get("id")
        start_time = datetime.datetime.fromisoformat(exercise.get("start_time"))
        await acur.execute(
            #"INSERT INTO exercises (polar_id, start_time, data) VALUES (%s, %s, %s) on conflict do nothing",
            #(polar_id, start_time, exercise))
            """INSERT INTO 
                exercises (
                    polar_id, 
                    start_time, 
                    data
                ) 
                VALUES (%s, %s, %s) 
            ON CONFLICT DO NOTHING""",
            (polar_id, start_time, json.dumps(exercise))
        )


async def insert_sleep(al, access_token, acur):
    sleeps = al.get_sleep(access_token).get("nights")
    for sleep in sleeps:
        sleep_date = datetime.date.fromisoformat(sleep.get("date"))
        await acur.execute("""
            INSERT INTO
                sleep (
                    date,
                    data
                )
                VALUES (%s, %s)
            ON CONFLICT DO NOTHING""",
            (sleep_date, json.dumps(sleep))
        )


async def insert_recharge(al, access_token, acur):
    recharges = al.get_recharge(access_token).get("recharges")
    for recharge in recharges:
        recharge_date = datetime.date.fromisoformat(recharge.get("date"))
        await acur.execute("""
            INSERT INTO
                recharge (
                    date,
                    data
                )
                VALUES (%s, %s)
            ON CONFLICT DO NOTHING""",
            (recharge_date, json.dumps(recharge))
        )


async def insert_cardio_load(al, access_token, acur):
    cardio_loads = al.get_cardio_load(access_token)
    for cardio_load in cardio_loads:
        cardio_date = datetime.date.fromisoformat(cardio_load.get("date"))
        await acur.execute("""
            INSERT INTO
                cardio_load (
                    date,
                    data
                )
                VALUES (%s, %s)
            ON CONFLICT (date)
                DO UPDATE
                    SET data = %s
            """,
            (cardio_date, json.dumps(cardio_load), json.dumps(cardio_load))
        )


async def insert_activity(config, acur):
    for i in range(1,5):
        day = datetime.datetime.today() - datetime.timedelta(days=i)
        data = get_day(config["username"], config["password"], day)
        try:
            await acur.execute("""
                INSERT INTO 
                    activity (
                        date,
                        active_time,
                        steps,
                        kilocalories,
                        inactivity_stamps,
                        sleep_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (date)
                    DO UPDATE
                        SET 
                            active_time = %s,
                            steps = %s,
                            kilocalories = %s,
                            inactivity_stamps = %s,
                            sleep_time = %s
            """,
                (
                data['date'],
                data['active time tracked'], 
                data['steps counted'],
                data['kilocalories burned'],
                data['inactivity stamps'],
                data['Sleep time'],
                data['active time tracked'], 
                data['steps counted'],
                data['kilocalories burned'],
                data['inactivity stamps'],
                data['Sleep time']
                )
            )
        except Exception as e:
            logger.error(f"Caught: \n{e}\ndata keys:\n{list(data.keys())}")


async def get_all():
    al = get_accesslink()
    config = get_config()
    access_token = config.get("access_token")
    conn_str = get_db_conn_string()
    async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
        async with aconn.cursor() as acur:
            await insert_exercises(al, access_token, acur)
            await insert_sleep(al, access_token, acur)
            await insert_recharge(al, access_token, acur)
            await insert_cardio_load(al, access_token, acur)
            await insert_activity(config, acur)



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
                CREATE TABLE IF NOT EXISTS cardio_load (pk SERIAL PRIMARY KEY, date date unique not null, data jsonb not null unique)  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS bedtime (pk SERIAL PRIMARY KEY, period_start_time timestamp with time zone not null unique, data jsonb not null unique)  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS nasalspray (pk SERIAL PRIMARY KEY, timestamp timestamp with time zone not null unique, usage VARCHAR(1))  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS activity (
                    pk SERIAL PRIMARY KEY, 
                    date date unique not null, 
                    active_time int not null,
                    steps int not null,
                    kilocalories int not null,
                    inactivity_stamps int not null,
                    sleep_time int not null
                )  
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


async def main():
    await create_tables()
    await get_all()


if __name__ == "__main__":
    asyncio.run(main())
import tomllib
import json
import psycopg
import asyncio
import datetime
import logging
import tcxparser
import requests
import os

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
ACCESSLINK_URL = "https://www.polaraccesslink.com/v3"



def get_db_conn_string():
    config = get_config()
    return(f"""
        dbname={config["db"]["db"]}
        user={config["db"]["user"]}
        password={config["db"]["password"]}
        host={config["db"]["host"]}
        port={config["db"]["port"]}
    """)


async def get_exercises_tcx(acur, access_link, access_token):
    headers = {
        "Accept": "application/vnd.garmin.tcx+xml",
        "Authorization": f"Bearer {access_token}"
    }
    exercises = access_link.get_exercises(access_token)
    for ex in exercises:
        start_time = ex["start_time"]
        start_time = start_time.replace("T", "_")
        start_time = start_time.replace(":", "-")
        filename = start_time + ".tcx"
        url = f"{ACCESSLINK_URL}/exercises/{ex['id']}/tcx"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Bad response: {response.text}")
            continue
        tcx_data = response.text

        output_dir = "tcx"
        out_exists = os.path.exists(output_dir)
        if not out_exists:
            #create a new directory because it does not exist
            os.makedirs(output_dir)
            print("Created directory %s" % output_dir)

        file_path = os.path.join(output_dir, filename)
        outfile = open(file_path, 'w')
        outfile.write(tcx_data)
        outfile.close()

        await insert_exercise_tcx(acur, ex, file_path)


async def insert_exercise_tcx(acur, exercise, filename):
    tcx = tcxparser.TCXParser(filename)
    timestamp_with_tz = datetime.datetime.fromisoformat(tcx.started_at)

    try:
        await acur.execute("""
            INSERT INTO exercises_tcx (
                polar_id,
                filename,
                start_time, 
                distance,
                hr_avg,
                hr_max,
                hr_min,
                training_load,
                duration,
                sport_type,
                calories,
                cardio_load,
                ascent,
                descent
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT do nothing
            """,
            (
                exercise["id"],
                filename,
                timestamp_with_tz,
                float(tcx.distance),
                tcx.hr_avg,
                tcx.hr_max,
                tcx.hr_min,
                exercise["training_load"],
                float(tcx.duration),
                tcx.activity_type,
                tcx.calories,
                exercise["training_load_pro"]["cardio-load"],
                float(tcx.ascent),
                float(tcx.descent)
            )
        )
    except Exception as e:
        logger.error(f"Caught: \n{e}")
        print(f"Exercise tcx caught: {e}")


async def insert_exercises(al, access_token, acur):
    exercises = al.get_exercises(access_token)
    for exercise in exercises:
        polar_id = exercise.get("id")
        start_time = datetime.datetime.fromisoformat(exercise.get("start_time"))
        await acur.execute(
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
        polar_user = sleep.get("polar_user")
        sleep_start_time = sleep.get("sleep_start_time")
        sleep_end_time = sleep.get("sleep_end_time")
        device_id = sleep.get("device_id")
        continuity = sleep.get("continuity")
        continuity_class = sleep.get("continuity_class")
        light_sleep = sleep.get("light_sleep")
        deep_sleep = sleep.get("deep_sleep")
        rem_sleep = sleep.get("rem_sleep")
        unrecognized_sleep_stage = sleep.get("unrecognized_sleep_stage")
        sleep_score = sleep.get("sleep_score")
        total_interruption_duration = sleep.get("total_interruption_duration")
        sleep_charge = sleep.get("sleep_charge")
        sleep_goal = sleep.get("sleep_goal")
        sleep_rating = sleep.get("sleep_rating")
        short_interruption_duration = sleep.get("short_interruption_duration")
        long_interruption_duration = sleep.get("long_interruption_duration")
        sleep_cycles = sleep.get("sleep_cycles")
        group_duration_score = sleep.get("group_duration_score")
        group_solidity_score = sleep.get("group_solidity_score")
        group_regeneration_score = sleep.get("group_regeneration_score")
        await acur.execute("""
            INSERT INTO sleep (
                polar_user, date, sleep_start_time, sleep_end_time, device_id, continuity, 
                continuity_class, light_sleep, deep_sleep, rem_sleep, unrecognized_sleep_stage, 
                sleep_score, total_interruption_duration, sleep_charge, sleep_goal, sleep_rating, 
                short_interruption_duration, long_interruption_duration, sleep_cycles, 
                group_duration_score, group_solidity_score, group_regeneration_score
            )                           
                VALUES (
                    %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s 
                )
            ON CONFLICT DO NOTHING
            RETURNING id""",
            (
                polar_user, sleep_date, sleep_start_time, sleep_end_time, device_id, continuity, 
                continuity_class, light_sleep, deep_sleep, rem_sleep, unrecognized_sleep_stage, 
                sleep_score, total_interruption_duration, sleep_charge, sleep_goal, sleep_rating, 
                short_interruption_duration, long_interruption_duration, sleep_cycles, 
                group_duration_score, group_solidity_score, group_regeneration_score
            )
        )
        inserted_row = await acur.fetchone()
        if inserted_row is None:
            continue
        sleep_id = inserted_row[0]
        await insert_hypnogram(sleep.get("hypnogram"), sleep_id, acur)
        await insert_sleep_hr(sleep.get("heart_rate_samples"), sleep_id, acur)


async def insert_hypnogram(hypnogram, sleep_id, acur):
    for key, value in hypnogram.items():
        await acur.execute("""
            INSERT INTO hypnogram (sleep_id, time, stage)
            VALUES (%s, %s, %s)
            ON CONFLICT (sleep_id, time) DO NOTHING;
        """,
        (sleep_id, key, value)
        )


async def insert_sleep_hr(hypnogram, sleep_id, acur):
    for key, value in hypnogram.items():
        await acur.execute("""
            INSERT INTO sleep_heart_rate (sleep_id, time, heart_rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (sleep_id, time) DO NOTHING;
        """,
        (sleep_id, key, value)
        )


async def insert_sleep_old(al, access_token, acur):
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


async def insert_recharge_hrv(hrv, recharge_id, acur):
    for key, value in hrv.items():
        await acur.execute("""
            INSERT INTO polar_hrv_samples (recharge_id, time, hrv)
            VALUES (%s, %s, %s)
            ON CONFLICT (recharge_id, time) DO NOTHING;
        """,
        (recharge_id, key, value)
        )


async def insert_recharge_breathing(breathing, recharge_id, acur):
    for key, value in breathing.items():
        await acur.execute("""
            INSERT INTO polar_breathing_samples (recharge_id, time, breathing_rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (recharge_id, time) DO NOTHING;
        """,
        (recharge_id, key, value)
        )


async def insert_recharge(al, access_token, acur):
    user_id = 1
    recharges = al.get_recharge(access_token).get("recharges")
    for recharge in recharges:
        await acur.execute("""
            INSERT INTO polar_recharge (
                user_id,
                polar_user,
                date,
                heart_rate_avg,
                beat_to_beat_avg,
                heart_rate_variability_avg,
                breathing_rate_avg,
                nightly_recharge_status,
                ans_charge,
                ans_charge_status
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT do nothing
            RETURNING id""",
            (
                user_id,
                recharge.get("polar_user"),
                recharge.get("date"),
                recharge.get("heart_rate_avg"),
                recharge.get("beat_to_beat_avg"),
                recharge.get("heart_rate_variability_avg"),
                recharge.get("breathing_rate_avg"),
                recharge.get("nightly_recharge_status"),
                recharge.get("ans_charge"),
                recharge.get("ans_charge_status")
            )
        )
        inserted_row = await acur.fetchone()
        if inserted_row is None:
            continue
        recharge_id = inserted_row[0]
        await insert_recharge_hrv(recharge.get("hrv_samples"), recharge_id, acur)
        await insert_recharge_breathing(recharge.get("breathing_samples"), recharge_id, acur)



async def insert_recharge_old(al, access_token, acur):
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
    for i in range(1,8):
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
                data.get('Sleep time', None),
                data['active time tracked'], 
                data['steps counted'],
                data['kilocalories burned'],
                data['inactivity stamps'],
                data.get('Sleep time', None),
                )
            )
        except Exception as e:
            logger.error(f"Caught: \n{e}\ndata keys:\n{list(data.keys())}")


def sync_insert_heart_rate(al, access_token, cur, days=1):
    print("here")
    if days < 1:
        print(f"days: {days}")
        return
    for i in range(1,days+1):
        day = datetime.datetime.today() - datetime.timedelta(days=i)
        day = day.date()
        print(f"getting {day}")
        data = al.get_continuous_heart_rate(access_token, day)
        date = data["date"]
        hr_data = []
        for i in data["heart_rate_samples"]:
            ts = datetime.datetime.strptime(date+i["sample_time"], "%Y-%m-%d%H:%M:%S")
            hr_data.append((ts, i["heart_rate"]))

        query = f"""
            INSERT INTO heart_rate (timestamp, heart_rate)
            VALUES (%s, %s)
            ON CONFLICT (timestamp)
            DO NOTHING;
        """
        cur.executemany(query, hr_data)


async def insert_heart_rate(al, access_token, acur, days=1):
    if days < 1:
        return
    for i in range(1,days):
        day = datetime.datetime.today() - datetime.timedelta(days=i)
        data = al.get_continuous_heart_rate(access_token, day)
        hr_data = data["heart_rate_samples"]

        query = f"""
            INSERT INTO heart_rate
            VALUES (%s, %s)
            ON CONFLICT (timestamp)
            DO NOTHING;
        """
        await acur.executemany(query, hr_data)


def get_single(call, **kwargs):
    al = get_accesslink()
    config = get_config()
    access_token = config.get("access_token")
    conn_str = get_db_conn_string()
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            match call:
                case "insert_heart_rate":
                    print("getting heart rate")
                    sync_insert_heart_rate(al, access_token, cur, days=kwargs.get("days", 1))


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
            await get_exercises_tcx(acur, al, access_token)


async def create_sleep(acur):
    await acur.execute("""
        CREATE TABLE if not exists sleep  (
            id SERIAL PRIMARY KEY,
            polar_user character varying(255),
            date date NOT NULL unique,
            sleep_start_time timestamp with time zone NOT NULL,
            sleep_end_time timestamp with time zone NOT NULL,
            device_id character varying(50) NOT NULL,
            continuity numeric NOT NULL,
            continuity_class integer NOT NULL,
            light_sleep integer NOT NULL,
            deep_sleep integer NOT NULL,
            rem_sleep integer NOT NULL,
            unrecognized_sleep_stage integer NOT NULL,
            sleep_score integer NOT NULL,
            total_interruption_duration integer NOT NULL,
            sleep_charge integer NOT NULL,
            sleep_goal integer NOT NULL,
            sleep_rating integer NOT NULL,
            short_interruption_duration integer NOT NULL,
            long_interruption_duration integer NOT NULL,
            sleep_cycles integer NOT NULL,
            group_duration_score numeric NOT NULL,
            group_solidity_score numeric NOT NULL,
            group_regeneration_score numeric NOT NULL
        );
    """)


async def create_hypnogram(acur):
    await acur.execute("""
        CREATE TABLE if not exists hypnogram (
            sleep_id INT references sleep(id),
            time time without time zone NOT NULL,
            stage integer NOT NULL,
            unique(sleep_id, time)
    );
    """)


async def create_sleep_hr(acur):
    await acur.execute("""
        CREATE TABLE if not exists sleep_heart_rate (
            sleep_id INT references sleep(id),
            time time without time zone NOT NULL,
            heart_rate integer NOT NULL,
            unique(sleep_id, time)
        );
    """)


async def create_polar_recharge(acur):
    await acur.execute("""
        CREATE TABLE if not exists polar_recharge (
            id SERIAL PRIMARY KEY,
            user_id integer,
            polar_user character varying(255),
            date date NOT NULL unique,
            heart_rate_avg integer NOT NULL,
            beat_to_beat_avg integer NOT NULL,
            heart_rate_variability_avg integer NOT NULL,
            breathing_rate_avg numeric NOT NULL,
            nightly_recharge_status integer NOT NULL,
            ans_charge numeric NOT NULL,
            ans_charge_status integer NOT NULL
        );
    """)


async def create_polar_hrv(acur):
    await acur.execute("""
        CREATE TABLE if not exists polar_hrv_samples (
            recharge_id INTEGER references polar_recharge(id),
            time time without time zone NOT NULL,
            hrv integer NOT NULL,
            unique(recharge_id, time)
        );
    """)


async def create_polar_breathing(acur):
    await acur.execute("""
        CREATE TABLE if not exists polar_breathing_samples (
            recharge_id INTEGER references polar_recharge(id),
            time time without time zone NOT NULL,
            breathing_rate numeric NOT NULL,
            unique(recharge_id, time)
        );
    """)


async def create_tables():
    conn_str = get_db_conn_string()
    async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
        async with aconn.cursor() as acur:
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS exercises_tcx (
                    polar_id varchar(20) not null unique, 
                    filename text not null unique,
                    start_time timestamp with time zone not null, 
                    distance float not null,
                    hr_avg integer not null,
                    hr_max integer not null,
                    hr_min integer not null,
                    training_load float not null,
                    duration float not null,
                    sport_type text not null,
                    calories integer not null,
                    cardio_load float not null,
                    ascent float not null,
                    descent float not null
                );
            """)
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
            #await acur.execute("""
            #    CREATE TABLE IF NOT EXISTS sleep (pk SERIAL PRIMARY KEY, date date unique not null, data jsonb not null unique)  
            #""")
            await create_sleep(acur)
            await create_hypnogram(acur)
            await create_sleep_hr(acur)
            await create_polar_recharge(acur)
            await create_polar_hrv(acur)
            await create_polar_breathing(acur)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS cardio_load (pk SERIAL PRIMARY KEY, date date unique not null, data jsonb not null unique)  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS bedtime (pk SERIAL PRIMARY KEY, period_start_time timestamp with time zone not null unique, data jsonb not null unique)  
            """)
            await acur.execute("""
                CREATE TABLE IF NOT EXISTS heart_rate (timestamp timestamp not null unique, heart_rate integer not null)
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
import datetime
import json
import pandas as pd
import psycopg2
import re
import requests
import telebot
import warnings

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta, datetime
from io import StringIO

warnings.filterwarnings("ignore")



default_args = {
    'owner': 'klip',
    'email': ['klip@klip.ru'],
    'start_date': datetime(2024, 3, 3),
    'depends_on_past': False,
    'wait_for_downstream': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag_params = {
    'dag_id': 'bot_streams_sender',
    'catchup': False,
    'schedule_interval': '*/10 * * * *',
    'default_args': default_args,
    'max_active_runs': 1,
    'concurrency': 0,
    'tags': ['bot', 'etl', 'streams'],
}



with open(f"main_config.json") as json_file:
    main_config = json.load(json_file)



# SQL base func
def load_df_bd(layer, table, df_data):
    '''
    Загрузка DF в БД
    '''
    
    # Загрузка DataFrame в PostgreSQL
    conn = psycopg2.connect(dbname=main_config["postgres"]["dbname"], user=main_config["postgres"]["user"],
                        password=main_config["postgres"]["password"], host=main_config["postgres"]["host"], port=main_config["postgres"]["port"],
                           options=f'-c search_path={layer}')
    cursor = conn.cursor()
    
    # Сохранение DataFrame в строку в формате CSV
    output = StringIO()
    df_data.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    
    columns = df_data.columns.tolist()
    
    # Открытие транзакции
    conn.autocommit = False
    try:
        # Копирование данных из StringIO в таблицу в PostgreSQL
        cursor.copy_from(output, f'{table}', null='', columns=columns)
        conn.commit()
        print(f"Датафрейм в таблицу {layer}.{table} загружен")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        print(f"!!!!!Датафрейм в таблицу {layer}.{table} не загружен")
    finally:
        cursor.close()
        conn.close()
    
    # очистим ОЗУ от треша
    output.close()  # Закрыть объект StringIO, освобождая ресурсы, связанные с ним.
    del output  # Удалить ссылку на объект, позволяя сборщику мусора Python освободить память.
    
    cursor.close()
    conn.close()

def read_df_bd(query, layer):
    '''
    Чтение таблицы в DF из БД
    '''
    conn = psycopg2.connect(dbname=main_config["postgres"]["dbname"], user=main_config["postgres"]["user"],
                        password=main_config["postgres"]["password"], host=main_config["postgres"]["host"], port=main_config["postgres"]["port"],
                           options=f'-c search_path={layer}')
    cursor = conn.cursor()
    
    query = f"{query}"
    df = pd.read_sql_query(query, conn)

    
    cursor.close()
    conn.close()

    return df




# E (ETL)
def get_page_soup(url: str):
    r = requests.get(url)
    r.encoding = "utf-8"
    if not r.ok:
        return None
    soup = BeautifulSoup(r.text, "lxml")
    return soup

def get_live_stream(name, url, big_stream):
  """
  Функция принимает url канала и отправяет данные по нему
  """
  channel_url = url + '/live'

  content = requests.get(channel_url).text
  ENCODED = str(content).encode("ascii", "ignore")

  if 'hqdefault_live.jpg' in ENCODED.decode():
      soup = get_page_soup(channel_url)

      link_names = []

      for link in soup.find_all(href=True):
          if re.findall(r"https://www.youtube.com/watch", link["href"].lower()) == ["https://www.youtube.com/watch"]:
              link_names.append(link["href"])

      for i in str(soup).split(','):
        if 'originalViewCount' in i:
          live_count = i.replace('"originalViewCount":"', '').replace('"}}', '')

      get_info = []

      get_info.append(name)
      get_info.append(f"{live_count}")
      get_info.append(f"https://youtu.be/{link_names[0][32:]}")
      get_info.append(big_stream)
      return get_info

  else:
      pass

def youtube_streams():
    '''
    извленения данных для стримов с ютуб
    '''
    data = read_df_bd("select * from raw.bot_streams_live_streams_dic", "raw")

    data = data[data["platform"] == "youtube"]
    data["big_stream"] = data["big_stream"].fillna('no')
    data = data[data["actual"] == 'yes']

    list_name = data["name"].values.flatten().tolist()
    list_link = data["link"].values.flatten().tolist()
    big_stream = data["big_stream"].values.flatten().tolist()

    try:
        live_list = []

        for i, j, k in zip(list_name, list_link, big_stream):
            live_list.append(get_live_stream(i, j, k))

        print(f"{datetime.today()} : стримы все получены")
    except:
        print(f"{datetime.today()} : стримы не получены")

    live_list_online = []

    for i in live_list:
        if i != None:
            live_list_online.append(i[:3])

    df_stream = pd.DataFrame(live_list_online, columns=["name", "online", "link"]).sort_values(by="online")
    df_stream["online"] = df_stream["online"].astype("int")
    df_stream = df_stream.sort_values(by="online", ascending=False)
    df_stream["datetime"] = datetime.now()

    load_df_bd("raw", "bot_streams_hist_regular", df_stream)

def twitch_streams():
    '''
    извленения данных для стримов с твитч
    '''
    # ## get online tiwtch
    data_twitch = read_df_bd("select * from raw.bot_streams_live_streams_dic", "raw")

    data_twitch = data_twitch[data_twitch["platform"] == "twitch"]
    list_name = data_twitch["big_stream"].values.flatten().tolist()

    client_id = main_config["twitch"]["client_id"]
    client_secret = main_config["twitch"]["client_secret"]
    body = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    r = requests.post('https://id.twitch.tv/oauth2/token', body)

    keys = r.json()
    headers = {
        'Client-ID': client_id,
        'Authorization': 'Bearer ' + keys['access_token']
    }

    def streamer_info(streamer_name):
        stream = requests.get(f'https://api.twitch.tv/helix/streams?user_login={streamer_name}', headers=headers)
        stream_data = stream.json()
        return stream_data

    online_list = []

    for i in list_name:
        try:
            online_list.append(streamer_info(f'{i}')["data"][0]["viewer_count"])
        except:
            online_list.append(0)
        
    df_online_twich = pd.DataFrame({
    "big_stream": list_name,
    "online": online_list
    })

    df_online_twich = df_online_twich[df_online_twich ["online"]> 0]
    df_online_twich_done = df_online_twich.merge(data_twitch, how="left", on = "big_stream")
    df_online_twich_done = df_online_twich_done[["name", "online", "link"]]
    df_online_twich_done = df_online_twich_done.sort_values(by="online", ascending=False)
    df_online_twich_done["datetime"] = datetime.now()

    load_df_bd("raw", "bot_streams_hist_regular", df_online_twich_done)



# T (ETL)
def load_transform_hist_data():
    # ## извлечение diff online

    df_stream_group = read_df_bd("""
    with rounded_table_ as (
    select 
        *,
        case 
            when link like '%youtu%' then 'youtube'
            when link like '%twitch%' then 'twitch'
        end platform,
        date_trunc('hour', datetime) + interval '1 minute' * (extract(minute from datetime)::integer / 10 * 10) rounded_date,
        dense_rank () over (order by date_trunc('hour', datetime) + interval '1 minute' * (extract(minute from datetime)::integer / 10 * 10) desc) rwn
    from 
        raw.bot_streams_hist_regular
    where 1=1
        and date(datetime) >= date(now()) - interval '2' day
    )
    , last_ as (
    select
        "name"
        , link
        , platform
        , rounded_date
        , max(online) online
    from
        rounded_table_
    where 1=1
        and rwn = 1
    group by
        1, 2, 3, 4
    order by 
        3 desc, 5 desc
    )
    , pre_last_ as (
    select
        "name"
        , link
        , platform
        , rounded_date
        , max(online) online
    from
        rounded_table_
    where 1=1
        and rwn = 2
    group by
        1, 2, 3, 4
    order by 
        3 desc, 5 desc
    )
    select
        l."name"
        , l.online
        , coalesce(l.online - pl.online, 0) online_diff
        , l.link
        , l.platform
        , now() datetime	
    from
        last_ l 
        left join pre_last_ pl on l.link = pl.link
    order by 
        5 desc, 2 desc
    """, "raw")

    load_df_bd("raw", "bot_streams_hist_regular_diff", df_stream_group)



# L (ETL) and TG
def message_send(s):
    """
    Функция принимает ID телеграм-канала и сообщение для отправки.
    Отправляет сообщение.
    """
    
    tg_message = s
    bot = telebot.TeleBot(main_config["telegram"]["token"])
    chatid = main_config["telegram"]["notification_tables"]
    
    msg = bot.send_message(
        chat_id=chatid,
        text=tg_message,
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )
    bot.last_message_sent = msg.chat.id, msg.message_id
    return bot.last_message_sent

def message_to_tg():
    df_stream_group = read_df_bd("""
            select
                    *
                from (
                    select 
                        *,
                        dense_rank () over (order by datetime desc) rwn
                    from 
                        raw.bot_streams_hist_regular_diff
                    where 1=1
                        and date(datetime) >= date(now()) - interval '2' day
                        and online > 5
                ) t1
                where rwn = 1
                order by 5 desc, 2 desc
                                        """,
            "raw")
    df_stream_group = df_stream_group[["name", "online", 'online_diff', "link"]]


    live_list_online = str(read_df_bd("""
            select 
                case
                    when extract(epoch from age(now(), max(datetime))) / 60 < 10 then 1
                    else 0
                end flag
            from 
                raw.bot_streams_hist_regular_diff
                                        """,
            "raw")["flag"][0])

    live_list_online = int(live_list_online)

    df_ci_bound = read_df_bd("""
        with pre_ as (
        select
            t1.*,
            t2.platform,
            t2.link channel_link
        from 
            raw.bot_streams_hist_regular t1 
            left join raw.bot_streams_live_streams_dic t2
            on t1."name" = t2."name"
        where 1=1
            and datetime <= now()
            and datetime >= now() - interval '15' day
        )
        , avg_last_month_ as (
        select
            "name", platform, 
            avg(online) avg_online,
            (stddev(online) / sqrt(count(*))) * 1.96 margin_of_error,
            avg(online) - (stddev(online) / sqrt(count(*))) * 1.96 CI_lower_bound,
            avg(online) + (stddev(online) / sqrt(count(*))) * 1.96 CI_upper_bound
        from
            pre_
        group by
            "name", platform
        )
        , last_online_ as (
        select 
            t1.*,
            dense_rank () over (order by datetime desc) rwn
        from 
            raw.bot_streams_hist_regular_diff t1 
            left join raw.bot_streams_live_streams_dic t2
            on t1."name" = t2."name"
        where 1=1
            and date(datetime) >= date(now()) - interval '2' day
        )
        select
            lo."name",
            lo.online, 
            alm.avg_online,
            1 - avg_online / online rate_online,
            case 
                when ci_upper_bound <  online then 1
                else 0
            end flg_top_online,
            margin_of_error, ci_lower_bound, ci_upper_bound,
            cast(round(cast((1 - ci_upper_bound / online) * 100 as numeric), 0) as int) rate_online_ci
        from
            last_online_ lo left join avg_last_month_ alm on lo."name" = alm."name"
        where 1=1
            and rwn = 1
        group by 1,2,3,4,5,6,7,8,9""",
                "raw")

    df_stream_group = df_stream_group.merge(df_ci_bound[["name", "rate_online_ci"]], how="left", on="name")

    df_stream_group = df_stream_group[['rate_online_ci', 'name', 'online', 'online_diff', 'link',]]
    df_stream_group["rate_online_ci"] = df_stream_group["rate_online_ci"].fillna(-500)
    df_stream_group["rate_online_ci"] = df_stream_group["rate_online_ci"].astype(int)


    df_stream_group = df_stream_group[df_stream_group["online"] > 15]
    df_stream_group = df_stream_group[df_stream_group["rate_online_ci"] > -350]

    # ## формирования cообщения
    if live_list_online > 0:
        # делим DF
        youtube_df = df_stream_group[df_stream_group["link"].str.contains("youtu.be")]
        twitch_df = df_stream_group[df_stream_group["link"].str.contains("twitch.tv")]


        # Функция для формирования сообщения из DataFrame
        def format_message(df):
            messages = []
            for _, row in df.iterrows():
                if (row["rate_online_ci"] > -50) and (row["rate_online_ci"] < 30):
                    sign = "🔸 "
                elif row["rate_online_ci"] <= -50:
                    sign = "✖️ "
                else:
                    sign = "✔️ "

                if row["online_diff"] > 0:
                    symbol = "🟢 +"
                elif row["online_diff"] < 0:
                    symbol = "🔴 "
                else:  # row["online_diff"] == 0
                    symbol = "🟡 "
                message = f"{sign} [{row['name']}]({row['link']}) [[{row['online']}]] {symbol}{row['online_diff']}"
                messages.append(message)
            return "\n".join(messages)
        
        # Формирование итогового сообщения
        tg_message = "⭐️ Online Streams: ⭐️\n\n"
        tg_message += format_message(youtube_df)
        tg_message += "\n\n"  # Разделитель между YouTube и Twitch
        tg_message += format_message(twitch_df)
                    
    if live_list_online == 0:
        tg_message = "❌ Not Online Streams:"

    # ## last msg
    msg_id_last = str(read_df_bd("""select 
        chat_message_id 
    from (
        select 
            datetime, chat_message_id,
            row_number () over (order by datetime desc) rwn
        from 
            raw.bot_streams_chat_message_id_hist
        where 1=1
        group by
            datetime, chat_message_id
    ) t1
    where 1=1
        and rwn = 1""",
            "raw")["chat_message_id"][0])

    bot = telebot.TeleBot(main_config["telegram"]["token"])
    bot.delete_message(*tuple(msg_id_last.replace("(", '').replace(")", '').split(', ')))

    msg = message_send(tg_message)

    z_list = []
    z_list.append(tg_message)

    data_msg = pd.DataFrame(
        {
        "datetime" : [datetime.today()],
        "chat_message_id" : [msg],
        "message" : [z_list],
        }
    )

    data_msg["message"] = data_msg["message"].astype("str")

    load_df_bd("raw", "bot_streams_chat_message_id_hist", data_msg)



with DAG(**dag_params) as dag:

    task_youtube_streams = PythonOperator(
        task_id='task_youtube_streams',
        python_callable=youtube_streams
    )

    task_twitch_streams = PythonOperator(
        task_id='task_twitch_streams',
        python_callable=twitch_streams
    )

    task_load_transform_hist_data = PythonOperator(
        task_id='task_load_transform_hist_data',
        python_callable=load_transform_hist_data
    )

    task_message_to_tg = PythonOperator(
        task_id='task_message_to_tg',
        python_callable=message_to_tg
    )

    [task_youtube_streams, task_twitch_streams] >> task_load_transform_hist_data >> task_message_to_tg
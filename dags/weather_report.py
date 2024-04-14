from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,1,1),
    'retries': 0
}

dag = DAG('pycharm_test_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")


def write_weather_2_pickle():
    import requests
    import pandas as pd
    import json

    lat = 43.11
    lon = 44.31
    key = "a5b69d44e48d36567029d2b5a5d7c477"

    template = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}" \
        .format(lat, lon, key)
    req = requests.get(template)
    data = json.loads(req.text)
    print("readed data")

    temp = pd.DataFrame(data={'main': [data['weather'][0]['main']],
                              'description': [data['weather'][0]['description']],
                              'temp': [data["main"]["temp"] - 273.15],
                              'dt': [data["dt"]],
                              'wind': [data["wind"]["speed"]]})
    temp.dt = pd.to_datetime(temp.dt, unit='s', origin='unix')

    try:
        df = pd.read_pickle("/mnt/c/Users/train/airflow/my_data.pkl")
        df = df._append(temp)
        df.to_pickle("/mnt/c/Users/train/airflow/my_data.pkl")
        print('success import')
    except:
        temp.to_pickle("/mnt/c/Users/train/airflow/my_data.pkl")
        print('failed import')

def report():
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    from datetime import datetime


    df = pd.read_pickle("/mnt/c/Users/train/airflow/my_data.pkl")
    sns.lineplot(data=df, x='dt', y='temp')
    plt.xticks(rotation=45)
    print('correct plot')
    name = f"/mnt/c/Users/train/airflow/report.png".replace(' ', '-')
    plt.savefig(name)

t1 = PythonOperator(task_id='get_weather_data',
                    dag=dag,
                    python_callable=write_weather_2_pickle)

t2 = PythonOperator(task_id='plot',
                    dag=dag,
                    python_callable=report)
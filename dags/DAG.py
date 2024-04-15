from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

def extract_data():
    df = pd.read_html('https://meteoinfo.ru/forecasts/russia/moscow-area/moscow')
    w = df[3]
    w= pd.DataFrame(w)
    return w
def transform():
    w=extract_data()
    w=w[[0,1,3,4]]
    w.columns = ['Дата', 'Время суток', 'Температура', 'Погода']
    return w
def to_excel():
    w=transform()
    w.to_excel('/opt/airflow/dags/weather.xlsx', index=False)
    return w
def prepare_plot():
    w=transform()
    ddd = w
    ddd['Температура'] = ddd['Температура'].str.extract(r'^(-?\d+)\..*')
    dd = ddd[ddd['Время суток'] == 'День']
    dd['Температура'] = dd['Температура'].apply(pd.to_numeric)
    return dd
def prepare_plot2():
    w=transform()
    ddd = w
    ddd['Температура'] = ddd['Температура'].str.extract(r'^(-?\d+)\..*')
    nd = ddd[ddd['Время суток'] == 'Ночь']
    nd['Температура'] = nd['Температура'].apply(pd.to_numeric)
    nd['Температура'] = nd['Температура'].explode()
    return nd
def plot():
    dd=prepare_plot()
    nd=prepare_plot2()
    dd= pd.DataFrame(dd)
    nd= pd.DataFrame(nd)
    plt.plot(dd['Дата'], dd['Температура'], label='Дневная температура')
    plt.plot(nd['Дата'], nd['Температура'], label='Ночная температура')
    plt.ylabel('Температура')
    plt.legend()
    plt.xticks(rotation=45)
    plt.savefig('/opt/airflow/dags/plot.png')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
}

dag = DAG(
    'weather_data',
    default_args=default_args,
    description='A simple DAG to extract and plot weather data for Moscow',
    schedule_interval='@daily',
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)
transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)
to_excel_task = PythonOperator(
    task_id='to_excel',
    python_callable=to_excel,
    dag=dag,
)
prepare_plot_task = PythonOperator(
    task_id='prepare_plot',
    python_callable=prepare_plot,
    dag=dag,
)
prepare_plot_task2 = PythonOperator(
    task_id='prepare_plot2',
    python_callable=prepare_plot2,
    dag=dag,
)
plot_task = PythonOperator(
    task_id='plot',
    python_callable=plot,
    dag=dag,
)
extract_data_task >> transform_task >> to_excel_task
transform_task >> prepare_plot_task >> plot_task
transform_task >> prepare_plot_task2 >> plot_task

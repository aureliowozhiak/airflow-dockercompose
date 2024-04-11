
import json
import pendulum
from airflow.decorators import dag, task
import requests
import sqlite3

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 4, 11, tz="UTC"),
    catchup=False,
    tags=['api', 'taskflow', 'rick_and_morty', 'etl'],
)
def rick_and_morty_taskflow_api_etl():
    @task()
    def extract_character():
        endpoint = "https://rickandmortyapi.com/api/character"
        response = []

        tmp_response = requests.get(endpoint)

        response.append(tmp_response.json())

        while tmp_response.json()['info']['next'] and tmp_response.status_code == 200:
            tmp_response = requests.get(tmp_response.json()['info']['next'])
            response.append(tmp_response.json())

        return response

    @task()
    def transform(response: list):
        character_data = []
        for page in response:
            for character in page['results']:
                character_data.append({
                    'id': character['id'],
                    'name': character['name'],
                    'status': character['status'],
                    'species': character['species'],
                    'type': character['type'],
                    'gender': character['gender'],
                    'origin': character['origin']['name'],
                    'quantity_appeared': len(character['episode'])
                })
        return character_data


    @task()
    def load(character_data: list):
        with open('/opt/airflow/dags/character_data.csv', 'w') as f:
            f.write('id,name,status,species,type,gender,origin,quantity_appeared\n')
            for character in character_data:
                f.write(f"{character['id']},{character['name']},{character['status']},{character['species']},{character['type']},{character['gender']},{character['origin']},{character['quantity_appeared']}\n")
        
        #for character in character_data:
        #    print(f"Character name: {character['name']}, Episodes appeared: {character['quantity']}")

    @task()
    def load_in_sqlite(character_data: list):
        conn = sqlite3.connect('/opt/airflow/dags/character_data.db')
        c = conn.cursor()

        c.execute('''DROP TABLE IF EXISTS character_data''')

        c.execute('''CREATE TABLE character_data
                     (id int, name text, status text, species text, type text, gender text, origin text, quantity_appeared int)''')

        for character in character_data:
            c.execute("INSERT INTO character_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (character['id'], character['name'], character['status'], character['species'], character['type'], character['gender'], character['origin'], character['quantity_appeared']))

        conn.commit()
        conn.close()

    

    character_data = extract_character()
    transformed_data = transform(character_data)
    load(transformed_data)
    load_in_sqlite(transformed_data)

rick_and_morty_etl_dag = rick_and_morty_taskflow_api_etl()
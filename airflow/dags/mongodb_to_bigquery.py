import logging
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator, is_venv_installed
from datetime import timedelta

log = logging.getLogger(__name__)

def preprocess_and_insert_to_bigquery():
    import pandas as pd
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    from pymongo import MongoClient

    # Get the absolute path to the service account JSON file
    service_account_path = "./data/service-account.json"

    # Connexion à MongoDB
    print("Connecting to MongoDB...")
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['test']
    collection = db['posts']

    # Récupération des données depuis MongoDB
    print("Fetching data from MongoDB...")
    data = list(collection.find({}))

    # Création d'un DataFrame pandas à partir des données MongoDB
    print("Creating DataFrame from MongoDB data...")
    df = pd.DataFrame(data)

    # Renommer la colonne '@PostTypeId' en 'PostTypeId' pour la correspondance avec BigQuery
    df = df.rename(columns={'@PostTypeId': 'PostTypeId'})

    # Connexion à BigQuery
    print("Connecting to BigQuery...")
    bq_client = bigquery.Client.from_service_account_json(service_account_path)
    dataset_id = 'movies_database'
    table_id = 'posts'

    # Check if the dataset exists, if not, create it
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)

    # Check if the table exists, if not, create it
    table_ref = dataset_ref.table(table_id)
    try:
        bq_client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField('PostTypeId', 'INTEGER'),
            bigquery.SchemaField('ViewCount', 'INTEGER'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)
        
    # Charger les données BigQuery existantes

    print("Fetching existing data from BigQuery...")
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    try:
        existing_data = bq_client.list_rows(table_ref).to_dataframe()
    except NotFound:
        existing_data = pd.DataFrame()

    # Fusionner les nouvelles données avec les données existantes pour mettre à jour ViewCount
    if not existing_data.empty:
        merged_data = pd.merge(existing_data, df, on='PostTypeId', how='outer')
        merged_data['ViewCount'] = merged_data['ViewCount'].fillna(0) + merged_data['@ViewCount'].fillna(0)
        updated_data = merged_data[['PostTypeId', 'ViewCount']].fillna(0)
    else:
        updated_data = df[['PostTypeId', '@ViewCount']].rename(columns={'@ViewCount': 'ViewCount'})

    # Enregistrer les données mises à jour dans BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print("Loading updated data into BigQuery...")
    job = bq_client.load_table_from_dataframe(updated_data, f'{dataset_id}.{table_id}', job_config=job_config)
    job.result()  # Attendre que le job soit terminé
    print("Job completed successfully")

with DAG(
    dag_id="bigquery_data_transfer",
    schedule=timedelta(minutes=30), # 30 minutes 
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    if not is_venv_installed():
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")

    # Tâche Airflow pour la fonction de pré-agrégation et d'insertion dans BigQuery
    insert_task = PythonVirtualenvOperator(
        task_id='preprocess_and_insert_to_bigquery',
        requirements=["pymongo", "pandas", "google-cloud-bigquery"],
        python_callable=preprocess_and_insert_to_bigquery,
        dag=dag,
    )

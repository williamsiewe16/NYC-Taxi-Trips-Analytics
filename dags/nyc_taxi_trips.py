from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain, chain_linear
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import snowflake.connector
import os
from tqdm import tqdm
import logging

# Configure the logging system
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    filename='example.log',  # Specify the log file name
    filemode='a'  # Set the file mode to append (create the file if it doesn't exist)
)

# Create a logger
logger = logging.getLogger(__name__)


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc_taxi_trips"],
)
def nyc_taxi_trips():

    #@task.external_python(python='/usr/local/airflow/py_venv/bin/python')

    @task.branch(task_id="check_if_file_exists_in_S3")
    def check_if_file_exists_in_S3(bucket_name, object_name):
        downstream_tasks = ["ingest_raw_data_into_snowflake.upload_file_from_api_to_S3","ingest_raw_data_into_snowflake.notify_admin_file_exists"]
        
        # Créez une session S3
        s3_client = boto3.client('s3')
        
        try:
            # Tentez de récupérer les métadonnées de l'objet
            s3_client.head_object(Bucket=bucket_name, Key=object_name)
            print(f"The object {object_name} already exists in the bucket {bucket_name}.")
            return downstream_tasks[1]
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                #print(f"The object {object_name} does not exist in the bucket {bucket_name}.")
                return downstream_tasks[0]
            else:
                print(f"Error checking if object exists: {e}")
                return False

    @task(task_id="upload_file_from_api_to_S3")
    def upload_file_from_api_to_S3(api_url, bucket_name, object_name):
        s3_client = boto3.client('s3')

        try:
            # Requête pour obtenir le fichier depuis l'API
            response = requests.get(api_url, stream=True)
            response.raise_for_status()

            # Obtenez la taille totale du fichier pour la barre de progression
            total_size = int(response.headers.get('content-length', 0))

            # Utiliser tqdm pour afficher la progression
            progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=object_name)

            # Fonction de rappel pour mettre à jour la barre de progression
            def upload_progress(bytes_amount):
                progress_bar.update(bytes_amount)

            # Envoyer directement le flux de données vers S3
            s3_client.upload_fileobj(response.raw, bucket_name, object_name, Callback=upload_progress)

            progress_bar.close()
            print(f"Successfully uploaded {object_name} to {bucket_name}")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"AWS credentials error: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    @task(task_id="notify_admin_file_exists")
    def notify_admin_file_exists():
        logger.info("File already exists in S3 bucket")


    @task(task_id="copy_into_snowflake", trigger_rule=TriggerRule.ONE_SUCCESS)
    def copy_into_snowflake():
        database = os.getenv('SNOWFLAKE_DATABASE')
        schema = os.getenv('SNOWFLAKE_SCHEMA')

        # Configuration de la connexion Snowflake
        conn = snowflake.connector.connect(
            user = os.getenv('SNOWFLAKE_USER'),
            password = os.getenv('SNOWFLAKE_PASSWORD'),
            account = os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
            database = os.getenv('SNOWFLAKE_DATABASE'),
            schema = os.getenv('SNOWFLAKE_SCHEMA'),
            role = os.getenv('SNOWFLAKE_ROLE')
        )

        # Créez un curseur
        cur = conn.cursor()

        try:
            # Commande COPY INTO
            trip_source_table = os.getenv('SNOWFLAKE_TRIP_SOURCE_TABLE')
            trip_source_stage_path = os.getenv('SNOWFLAKE_TRIP_SOURCE_STAGE_PATH')
            copy_sql = f"""
                    COPY INTO {database}.{schema}.{trip_source_table}
                    FROM (
                        SELECT
                            $1:VendorID::INTEGER AS VENDORID,
                            $1:tpep_pickup_datetime::VARCHAR::TIMESTAMP_NTZ AS TPEP_PICKUP_DATETIME,
                            $1:tpep_dropoff_datetime::VARCHAR::TIMESTAMP_NTZ AS TPEP_DROPOFF_DATETIME,
                            $1:passenger_count::INTEGER AS PASSENGER_COUNT,
                            $1:trip_distance::DOUBLE AS TRIP_DISTANCE,
                            $1:PULocationID::INTEGER AS PULOCATIONID,
                            $1:DOLocationID::INTEGER AS DOLOCATIONID,
                            $1:RatecodeID::INTEGER AS RATECODEID,
                            $1:store_and_fwd_flag::STRING AS STORE_AND_FWD_FLAG,
                            $1:payment_type::INTEGER AS PAYMENT_TYPE,
                            $1:fare_amount::DOUBLE AS FARE_AMOUNT,
                            $1:extra::DOUBLE AS EXTRA,
                            $1:mta_tax::DOUBLE AS MTA_TAX,
                            $1:improvement_surcharge::DOUBLE AS IMPROVEMENT_SURCHARGE,
                            $1:tip_amount::DOUBLE AS TIP_AMOUNT,
                            $1:tolls_amount::DOUBLE AS TOLLS_AMOUNT,
                            $1:total_amount::DOUBLE AS TOTAL_AMOUNT,
                            $1:congestion_surcharge::DOUBLE AS CONGESTION_SURCHARGE,
                            $1:Airport_fee::DOUBLE AS AIRPORT_FEE,
                            CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
                        FROM @{database}.{schema}.{trip_source_stage_path} T
                    )
                """
            
            # Exécution de la commande COPY INTO
            logger.info(copy_sql)
            cur.execute(copy_sql)
            
            # Confirmer le succès
            print("Données copiées avec succès depuis le stage vers la table.")
            
        finally:
            # Fermez le curseur et la connexion
            cur.close()
            conn.close()

    @task_group
    def ingest_raw_data_into_snowflake():

        # Get the current date
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month

        month -= 5
        if month <= 0:
            month += 12
            year -= 1
        
        # Get file name
        file_name = f'yellow_tripdata_{year}-{month:02d}.parquet'
        logger.info(file_name)

        # URL du fichier à télécharger
        file_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}'
        
        # Nom du fichier local pour stocker le téléchargement
        #local_file_name = f'include/tmp/{file_name}'

        # Infos AWS
        s3_bucket_name = 'nyc-taxi-trips-bucket'
        s3_file_name = f'yellow_taxi/{file_name}'

        chain(
            check_if_file_exists_in_S3(s3_bucket_name,s3_file_name),
            [notify_admin_file_exists(), upload_file_from_api_to_S3(file_url,s3_bucket_name,s3_file_name)],
            copy_into_snowflake()
        )
    
    @task(task_id="get_nyc_trip_sources")
    def get_nyc_trip_sources():
        print("go")
    

    chain(
        ingest_raw_data_into_snowflake(),
        get_nyc_trip_sources()
    )

# Appeler le dag
nyc_taxi_trips()
import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import os
import sys
from tqdm import tqdm


        
def download_file(url, local_filename):
# Vérifier si le répertoire existe, sinon le créer
    if not os.path.exists(os.path.dirname(local_filename)):
        os.makedirs(os.path.dirname(local_filename))

    # Vérifier si le fichier existe déjà
    if os.path.exists(local_filename):
        print(f"File '{local_filename}' already exists.")
        return

    # Effectuer la requête GET pour obtenir le fichier
    response = requests.get(url, stream=True)
    # Vérifier si la requête a réussi
    if response.status_code == 200:
        # Ouvrir le fichier local en mode écriture binaire
        with open(local_filename, 'wb') as f:
            total_length = response.headers.get('content-length')
            if total_length is None: # Pas de taille disponible
                f.write(response.content)
            else:
                dl = 0
                total_length = int(total_length)
                for data in response.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    done = int(50 * dl / total_length)
                    # Afficher le pourcentage de téléchargement en temps réel
                    sys.stdout.write("\r[%s%s] %.2f%%" % ('=' * done, ' ' * (50-done), 100 * dl / total_length))
                    sys.stdout.flush()
        print("\nDownload complete")
    else:
        print("Failed to download file")


def upload_file_to_s3_from_api(api_url, bucket_name, object_name):
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



def upload_file_to_s3(file_name, bucket, object_name=None):
    # Créez une session S3
    s3_client = boto3.client('s3')

    # Obtenez la taille du fichier
    file_size = os.path.getsize(file_name)

    # Créez une instance tqdm pour afficher la progression
    progress_bar = tqdm(total=file_size, unit='B', unit_scale=True, desc=file_name)

    # Fonction de rappel pour mettre à jour la barre de progression
    def upload_progress(chunk):
        progress_bar.update(chunk)

    try:
        # Téléchargez le fichier avec le rappel pour la progression
        s3_client.upload_file(file_name, bucket, object_name, Callback=upload_progress)
    except Exception as e:
        print(f"Error uploading file: {e}")
    finally:
        progress_bar.close()


def main():

    file_name = 'yellow_tripdata_2024-01.parquet'

    # URL du fichier à télécharger
    file_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}'
    
    # Nom du fichier local pour stocker le téléchargement
    local_file_name = f'include/tmp/{file_name}'

    # Infos AWS
    s3_bucket_name = 'nyc-taxi-trips-bucket'
    s3_file_name = f'yellow_taxi/{file_name}'

    if not check_if_file_exists_in_S3(s3_bucket_name,s3_file_name):

        upload_file_to_s3_from_api(file_url,s3_bucket_name,s3_file_name)
        
        # Téléchargement du fichier
        #download_file(file_url, local_file_name)

        # upload vers S3
        #upload_file_to_s3(local_file_name, s3_bucket_name, s3_file_name)

if __name__ == "__main__":
    main()

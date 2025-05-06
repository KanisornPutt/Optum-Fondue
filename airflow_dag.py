from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_qdrant import FastEmbedSparse, QdrantVectorStore, RetrievalMode
from langchain_community.embeddings import HuggingFaceEmbeddings
from qdrant_client import QdrantClient
from airflow.models import Variable
import sqlite3
from tqdm import tqdm
import os
import pandas as pd
import requests
import re

# --- Default args ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),  # change as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
dag = DAG(
    'traffy_etl_pipeline',
    default_args=default_args,
    description='Fetch and clean Traffy data',
    schedule='@daily',
    catchup=False,
)

def clean_comment(comment):
    if type(comment) != str:
        return ""
    comment = ''.join([c for c in comment if c.isalpha() or c.isspace() or '\u0E00' <= c <= '\u0E7F'])
    comment = comment.replace('\n', ' ')
    comment = comment.replace('\r', ' ')
    comment = re.sub(' +', ' ', comment)
    return comment

# ---- Utility Functions -----
DB_PATH = '/opt/data/traffy.db'

def insert_into_db(df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        try :
            cursor.execute('''
                INSERT OR IGNORE INTO traffy (
                    ticket_id, type, organization, comment, photo, photo_after,
                    longitude, latitude, address, subdistrict, district, province,
                    timestamp, state, star, count_reopen, last_activity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['ticket_id'], row['type'], row['organization'], row['comment'],
                row['photo'], row['photo_after'], row['longitude'], row['latitude'],
                row['address'], row['subdistrict'], row['district'], row['province'],
                row['timestamp'], row['state'], row['star'], row['count_reopen'],
                row['last_activity']
            ))
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            print(f"Row data: {row.to_dict()}")
    
    conn.commit()
    conn.close()

# ----- Pipeline Functions -----
def fetch_data(**context):
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    WEB_URL = "https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1"
    params = {
        "output_format": "json",
        "start": yesterday,
        "end": yesterday
    }

    response = requests.get(WEB_URL, params=params)
    data = response.json()

    df = pd.DataFrame([{
        'ticket_id': p.get('ticket_id'),
        'type': p.get('type'),
        'organization': p.get('org'),
        'comment': p.get('description'),
        'photo': p.get('photo_url'),
        'photo_after': p.get('after_photo'),
        # 'coords': f.get('geometry', {}).get('coordinates', [None, None]),
        'longitude': f.get('geometry', {}).get('coordinates', [None, None])[0],
        'latitude': f.get('geometry', {}).get('coordinates', [None, None])[1],
        'address': p.get('address'),
        'subdistrict': p.get('subdistrict'),
        'district': p.get('district'),
        'province': p.get('province'),
        'timestamp': p.get('timestamp'),
        'state': p.get('state'),
        'star': p.get('star'),
        'count_reopen': p.get('count_reopen'),
        'last_activity': p.get('last_activity'),
    } for f in data["features"] for p in [f["properties"]]])
 
    df['organization'] = df['organization'].apply(lambda x: ",".join(x) if type(x) is list else x)
    df['type'] = df['type'].apply(lambda x: ",".join(x) if type(x) is list else x)
    
    insert_into_db(df)

    context['ti'].xcom_push(key='raw_df', value=df.to_json(orient='records'))
    print("Fetched data:")
    print(df[['type', 'comment', 'address', 'timestamp']].head())

def clean_data(**context):
    raw_json = context['ti'].xcom_pull(task_ids='fetch_data', key='raw_df')
    if raw_json is None:
        raise ValueError("No raw_df data found in XCom. Make sure fetch_data runs successfully.")
    
    df = pd.read_json(raw_json)

    df["type_str"] = df["type"].astype(str).str.strip("{}").str.replace("'", "", regex=False)

    df = df.dropna(subset=['comment'])

    df['comment'] = df['comment'].apply(clean_comment)
    df['address'] = df['address'].apply(clean_comment)

    df['context'] = df.apply(
        lambda row: f'ชนิด: {row["type_str"]} : "{row["comment"]}" ที่อยู่ "{row["address"]}"',
        axis=1
    )
    context['ti'].xcom_push(key='cleaned_df', value=df.to_json(orient='records'))
    print("cleaned data:")
    print(df[['type_str', 'comment', 'address', 'context']].head())

def embedding_data(**context):
    cleaned_json = context['ti'].xcom_pull(task_ids='clean_data', key='cleaned_df')
    if cleaned_json is None:
        raise ValueError("No cleaned_df data found in XCom. Make sure clean_data runs successfully.")
    
    df = pd.read_json(cleaned_json)

    all_docs = []
    for index, row in df.iterrows():
        context_text = row["context"]
        problem_type = row["type_str"]
        address = row["address"]
        ticket_id = row["ticket_id"]

        if context_text:
            doc = Document(
                page_content=str(context_text),
                metadata={
                    "problem_type": problem_type,
                    "address": address,
                    "ticket_id": ticket_id
                }
            )
            all_docs.append(doc)

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=2000,
        chunk_overlap=200,
        add_start_index=True
    )
    all_splits = text_splitter.split_documents(all_docs)

    context['ti'].xcom_push(key='all_splits', value=[doc.dict() for doc in all_splits])

    print(f"Generated {len(all_splits)} document chunks.")

def save_to_qdrant(**context):
    
    qdrant_api_key = Variable.get("qdrant_api_key")
    collection_name = Variable.get("collection_name")
    qdrant_url = Variable.get("qdrant_url")
    
    splits_raw = context['ti'].xcom_pull(task_ids='embedding_data', key='all_splits')
    if splits_raw is None:
        raise ValueError("No all_splits found in XCom. Make sure embedding_data runs successfully.")

    
    all_splits = [Document(**doc) for doc in splits_raw]
    sparse_embeddings = FastEmbedSparse(model_name="Qdrant/bm25")
    model_kwargs = {'trust_remote_code': True}
    dense_embeddings = HuggingFaceEmbeddings(model_name="KanisornPutta/TrentIsNotLeavingBERT",model_kwargs=model_kwargs)

    client = QdrantClient(
        url=qdrant_url,
        api_key=qdrant_api_key
    )

    qdrant = QdrantVectorStore(
        client=client,
        collection_name=collection_name,
        embedding=dense_embeddings,
        sparse_embedding=sparse_embeddings,
        retrieval_mode=RetrievalMode.HYBRID,
        vector_name="dense",
        sparse_vector_name="sparse",
    )

    print("Adding documents to Qdrant...")
    for doc in tqdm(all_splits, desc="Uploading documents"):
        qdrant.add_documents(documents=[doc])

    print("All documents uploaded to Qdrant.")

# --- Operators ---
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

embedding_task = PythonOperator(
    task_id='embedding_data',
    python_callable=embedding_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_to_qdrant',
    python_callable=save_to_qdrant,
    dag=dag,
)

fetch_task >> clean_task >> embedding_task >> save_task
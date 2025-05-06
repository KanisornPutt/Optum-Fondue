import pandas as pd
import sqlite3
import os

DB_PATH = './data/traffy.db'
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def create_traffy_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS traffy (
        ticket_id TEXT PRIMARY KEY,
        type TEXT,
        organization TEXT,
        comment TEXT,
        photo TEXT,
        photo_after TEXT,
        longitude REAL,
        latitude REAL,
        address TEXT,
        subdistrict TEXT,
        district TEXT,
        province TEXT,
        timestamp TEXT,
        state TEXT,
        star INTEGER,
        count_reopen INTEGER,
        last_activity TEXT
    )
    ''')
    conn.commit()
    conn.close()
    
def insert_into_db(df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for _, row in df.iterrows():
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
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    try:
        if not os.path.exists(DB_PATH):
            print("Database does not exist. Creating a new one.")
            create_traffy_table()
            print("Inject traffy fondue data into the database")
            df = pd.read_csv("./dataset/traffy.csv")
            insert_into_db(df)
            print("Succesfully initialized the database.")
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Error initializing the database. Please check the data and try again.")
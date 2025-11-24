from datawarehouse.data_modification import insert_rows_to_db, update_rows, delete_rows
from datawarehouse.data_loading import load_data_from_api, 
from datawarehouse.data_utils import et_video_ids_from_db, et_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids_from_db   
from datawarehouse.data_transformation import transform_data
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import logging

logging = logging.getLogger(__name__)
table = "youtube_video_stats"


def default_args():
    return {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    }   

@dag(
    default_args=default_args(),   
    schedule_interval='@daily',
    catchup=False,
    tags=['data_warehouse'],
)

@task()
def staging_table():
    schema_name="staging"
    conn, cursor = None, None
    try:
        conn, cursor = get_conn_cursor()
        yt_data = load_data_from_api()
        create_schema(schema_name)
        create_table(schema_name=schema_name, table_name=table)
        logging.info("Staging schema and table created successfully")
        table_ids = get_video_ids_from_db(schema_name=schema_name, table_name=table)

        for row in yt_data:
            if row["video_id"] not in table_ids:
                insert_rows_to_db(cursor, conn, yt_data, schema_name=schema_name, table_name=table)
                logging.info(f"Inserted video ID {row['video_id']} into staging table.")
            else:
                if row["video_id"] in table_ids:
                    update_rows(cursor, conn, yt_data, schema_name=schema_name, table_name=table)
                    logging.info(f"Updated video ID {row['video_id']} in staging table.")

                else:
                    insert_rows_to_db(cursor, conn, yt_data, schema_name=schema_name, table_name=table)
                    logging.info(f"Inserted video ID {row['video_id']} into staging table.")

            ids_in_json = {row["video_id"] for row in yt_data} 
            ids_in_db = set(table_ids)
            ids_to_delete = ids_in_json - ids_in_db  
            for video_id in ids_to_delete:
                delete_query = f"DELETE FROM {schema_name}.{table} WHERE video_id = %s;"
                cursor.execute(delete_query, (video_id,))
                conn.commit()
                logging.info(f"Deleted video ID {video_id} from staging table.")    

    except Exception as e:      
        logging.error(f"Error in staging_table task: {e}")
    finally:    
        if conn and cursor:
            close_conn_cursor(conn, cursor)


def core_table():   
    schema_name="core"
    conn, cursor = None, None
    try:
        conn, cursor = get_conn_cursor()
        yt_data = load_data_from_api()
        create_schema(schema_name)
        create_table(schema_name=schema_name, table_name=table)
        logging.info("Data Warehouse schema and table created successfully")
        table_ids = get_video_ids_from_db(schema_name=schema_name, table_name=table)
        current_ids = set()
        query_srage = f"SELECT video_id FROM staging.{table};"
        data = cursor.execute(query_srage).fetchall()

        for row in data:
            current_ids.add(row["video_id"])

            if len(table_ids) == 0:
                trow = transform_data(row)
                insert_rows_to_db(cursor, conn, [trow], schema_name=schema_name, table_name=table)
                logging.info(f"Inserted video ID {row['video_id']} into core table.")
            else: 
                trow = transform_data(row) 
                if trow["video_id"] in table_ids:
                    update_rows(cursor, conn, [trow], schema_name=schema_name, table_name=table)
                    logging.info(f"Updated video ID {row['video_id']} into core table.")   
                else:
                    insert_rows_to_db(cursor, conn, [trow], schema_name=schema_name, table_name=table)
                    logging.info(f"Inserted video ID {row['video_id']} into core table.")
            ids_to_delete = set(table_ids) - current_ids  
            if ids_to_delete:  
                delete_rows(cursor, conn, schema_name, ids_to_delete)
                logging.info(f"Deleted video IDs {ids_to_delete} from core table.") 

    except Exception as e:      
        logging.error(f"Error in core_table task: {e}")
    finally:    
        if conn and cursor:
            close_conn_cursor(conn, cursor)


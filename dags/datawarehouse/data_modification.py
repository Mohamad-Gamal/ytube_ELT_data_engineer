import logging

from dags.api.video_stats import video_ids
from dags.datawarehouse.data_utils import table
from dags.datawarehouse.data_utils import get_conn_cursor, close_conn_cursor
from dags.datawarehouse.data_utils import create_schema, create_table
from dags.datawarehouse.data_utils import get_video_ids_from_db
from dags.datawarehouse.data_loading import transform_data


logging = logging.getLogger(__name__)
table = "data_warehouse.youtube_video_stats"

def insert_rows_to_db(cursor, conn,data, schema_name, table_name="youtube_video_stats"):
    conn, cursor = get_conn_cursor()

    try:
        if schema_name == "staging":
            table_name = f"{schema_name}.{table_name}"
            video_id = 'video_id'


            table_full_name = f"{schema_name}.{table_name}"
            insert_query = f"""
            INSERT INTO {table_full_name} (video_id, video_title, publishedAt, duration, viewCount, likeCount, commentCount)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO NOTHING;
            """
            try:
                for item in data:
                    cursor.execute(insert_query, (
                        item["video_id"],
                        item["video_title"],
                        item["publishedAt"],
                        item["duration"],
                        item["viewCount"],
                        item["likeCount"],
                        item["commentCount"]
                    ))
                conn.commit()
                logging.info("Data insertion successful")
            except Exception as e:
                logging.error(f"Error inserting data: {e}")
                conn.rollback()
            finally:
                close_conn_cursor(conn, cursor)
            
        else:
            table_name = f"{schema_name}.{table_name}"  
            table_full_name = f"{schema_name}.{table_name}"
            insert_query = f"""
            INSERT INTO {table_full_name} (video_id, video_title, upload_date, duration, video_type, views, likes, comments)     
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO NOTHING;
            """
            try:
                for item in data:
                    cursor.execute(insert_query, (
                        item["video_id"],
                        item["video_title"],
                        item["upload_date"],
                        item["duration"],
                        item["video_type"],
                        item["viewCount"],
                        item["likeCount"],
                        item["commentCount"]
                    ))
                conn.commit()
                logging.info("Data insertion successful")
            except Exception as e:
                logging.error(f"Error inserting data: {e}")
                conn.rollback()
            finally:
                close_conn_cursor(conn, cursor)

    except Exception as e:
        logging.error(f"Error in insert_rows_to_db: {e}")
        close_conn_cursor(conn, cursor)   


def update_rows(cursor, conn, schema, row):
    try: 
        # staging
        if schema == "staging":
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "viewCount"
            comments_count = "commentCount"
        # core
        else:
            video_id = "video_id"
            upload_date = "upload_Date"
            video_title = "video_title"
            video_views = "views"
            likes_count = "views"
            comments_count = "comments"

        update_query = f"""
        UPDATE {schema}.youtube_video_stats 
        SET {video_title} = %s,
            {video_views} = %s,
            {likes_count} = %s,
            {comments_count} = %s
        WHERE {video_id} = %s AND "upload_Date" = %({upload_date})s;;
        """ 
        cursor.execute(update_query, (
            row[video_title],
            row[video_views],
            row[likes_count],
            row[comments_count],
            row[video_id],
            row[upload_date]
        ))
        conn.commit()
        logging.info(f"Row with video_id {row[video_id]} updated successfully in {schema} schema.")     
    except Exception as e:
        logging.error(f"Error updating row with video_id {row[video_id]}: {e}")
        conn.rollback() 

def delete_rows(cursor, conn, schema, ids_to_delete):
    try:
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""
        delete_query = f"""
        DELETE FROM {schema}.youtube_video_stats 
        WHERE video_id in %s;
        """
        cursor.execute(delete_query, (ids_to_delete,))
        conn.commit()
        logging.info(f"Row with video_id {ids_to_delete} deleted successfully from {schema} schema.")
    except Exception as e:
        logging.error(f"Error deleting row with video_id {ids_to_delete}: {e}")
        conn.rollback()
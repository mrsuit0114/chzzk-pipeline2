# Chzzk DB Handler

from typing import Optional

import psycopg2
from loguru import logger

from src.common.config import DBConfig
from src.common.models import ChatLog, VideoLog


class ChzzkDBHandler:
    def __init__(self, config: DBConfig):
        self.config = config
        self.conn = None

    def __enter__(self):
        """connect to db when with statement starts"""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """disconnect from db when with statement ends"""
        self._close()

    def _connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                dbname=self.config.dbname,  # database name
                user=self.config.user,  # PostgreSQL user name
                password=self.config.password,  # user password
                host=self.config.host,  # host
                port=self.config.port,  # port (default port is 5432)
            )

    def _close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _execute_query(self, query: str, params: Optional[list[dict]] = None, commit: bool = False):
        """Execute a non-SELECT SQL query with optional parameters.

        This method handles the execution of INSERT, UPDATE, DELETE, and other non-SELECT queries.
        It automatically manages database connections and transaction rollbacks in case of errors.

        Args:
            query (str): The SQL query to execute
            params (Optional[list[dict]]): List of parameter dictionaries for the query.
                Each dictionary should contain the parameters for a single query execution.
                If None, the query will be executed without parameters.
            commit (bool): Whether to commit the transaction after execution.
                Defaults to False. Set to True for queries that need immediate persistence.

        Raises:
            RuntimeError: If database connection cannot be established
            Exception: For other unexpected errors
        """
        if not self.conn:
            self._connect()
            if not self.conn:
                raise RuntimeError("Database connection is not established.")

        with self.conn.cursor() as cur:
            try:
                if params:
                    cur.executemany(query, params)
                else:
                    cur.execute(query)

                if commit:
                    self.conn.commit()

            except Exception as e:
                self.conn.rollback()
                logger.error(f"⚠️ Database query execution failed: {e}")
                raise e

    def _select_query(self, query: str, params: Optional[dict] = None):
        """Execute a SELECT SQL query and return the results as a list of dictionaries.

        This method handles SELECT queries and automatically converts the results
        into a list of dictionaries where keys are column names and values are the
        corresponding row values.

        Args:
            query (str): The SELECT SQL query to execute
            params (Optional[dict]): Dictionary of parameters for the query.
                If None, the query will be executed without parameters.

        Returns:
            list[dict]: A list of dictionaries representing the query results.
                Each dictionary contains column names as keys and row values as values.

        Raises:
            RuntimeError: If database connection cannot be established or no data is returned
        """
        if not self.conn:
            self._connect()
            if not self.conn:
                raise RuntimeError("Database connection is not established.")

        with self.conn.cursor() as cur:
            cur.execute(query, params or {})
            if cur.description is None:
                raise RuntimeError("No data returned from the query.")
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def insert_video_data_bulk(self, video_data_list: list[VideoLog]):
        """Insert multiple video records into the database in a single transaction.

        This method efficiently inserts multiple video records by using bulk insert
        operations. It handles the conversion of VideoLog objects to database records.

        Args:
            streamer_idx (int): The unique identifier of the streamer
            video_data_list (list[VideoLog]): List of VideoLog objects to insert

        Raises:
            Exception: If the insert operation fails
            RuntimeError: If database connection cannot be established
        """
        query = """
        INSERT INTO videos (streamer_idx, video_id, category, created_at, video_url)
        VALUES (%(streamer_idx)s, %(video_id)s, %(category)s, %(created_at)s, %(video_url)s)
        """

        insert_values = [video.to_dict() for video in video_data_list]
        try:
            self._execute_query(query, insert_values, commit=True)
        except Exception as e:
            logger.error(f"Error inserting video data: {e}")
            raise e

    def insert_chat_data_bulk(self, chat_data_list: list[ChatLog]):
        """Insert multiple chat records into the database in a single transaction.

        This method efficiently inserts multiple chat records by using bulk insert
        operations. It handles the conversion of ChatLog objects to database records.

        Args:
            video_idx (int): The unique identifier of the video
            chat_data_list (list[ChatLog]): List of ChatLog objects to insert

        Raises:
            Exception: If the insert operation fails
            RuntimeError: If database connection cannot be established
        """
        query = """
        INSERT INTO chats (video_idx, chat_text, chat_time, user_id_hash, pay_amount, os_type)
        VALUES (%(video_idx)s, %(chat_text)s, %(chat_time)s, %(user_id_hash)s, %(pay_amount)s, %(os_type)s)
        """

        insert_values = [chat.to_dict() for chat in chat_data_list]

        self._execute_query(query, insert_values, commit=True)

    def get_existing_video_data_video_ids(self, streamer_idx: int) -> set[int]:
        """Retrieve a set of existing video IDs for a specific streamer.

        This method queries the database to get all video IDs that have already been
        recorded for a given streamer.

        Args:
            streamer_idx (int): The unique identifier of the streamer

        Returns:
            set[int]: A set of video IDs that already exist in the database

        Raises:
            Exception: If the query fails
            RuntimeError: If database connection cannot be established
        """
        query = """
        SELECT video_id
        FROM videos
        WHERE streamer_idx = %(streamer_idx)s
        """
        result = self._select_query(query, params={"streamer_idx": streamer_idx})
        return {row["video_id"] for row in result}

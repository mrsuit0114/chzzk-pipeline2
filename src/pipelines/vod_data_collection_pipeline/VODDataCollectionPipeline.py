import random
import time
from datetime import datetime
from typing import Optional

from loguru import logger

from src.common.ChzzkDBHandler import ChzzkDBHandler
from src.common.config import load_file_manager_config
from src.common.FileManager import FileManager
from src.common.models import VideoLog
from src.pipelines.vod_data_collection_pipeline.ChzzkChatCrawler import ChzzkChatCrawler
from src.pipelines.vod_data_collection_pipeline.ChzzkChatProcessor import ChzzkChatProcessor
from src.pipelines.vod_data_collection_pipeline.config import VODDataCollectionPipelineConfig


class VODDataCollectionPipeline:
    """Pipeline for collecting and processing VOD (Video On Demand) data from CHZZK.

    This pipeline handles the entire workflow of collecting video and chat data from CHZZK,
    including crawling, processing, and storing the data. It manages the coordination
    between different components like crawlers, processors, and storage handlers.

    The pipeline follows these main steps:
    1. Collect video data manually (as CHZZK API is not available)
    2. Store video data in the database
    3. Crawl chat data for each video
    4. Process and store the chat data
    """

    def __init__(self, config: VODDataCollectionPipelineConfig, db_handler: ChzzkDBHandler):
        """Initialize the VODDataCollectionPipeline with configuration.

        Args:
            config (VODDataCollectionPipelineConfig): Configuration containing settings
                for chat API, chat processing, and other pipeline components.
        """
        self.crawler = ChzzkChatCrawler(config.chat_api_config)
        self.processor = ChzzkChatProcessor(config.chat_processor_config)
        self.db_handler = db_handler
        self._file_manager: Optional[FileManager] = None

    @property
    def file_manager(self) -> FileManager:
        if self._file_manager is None:
            error_msg = "File manager is not set. Call set_file_manager() before using pipeline methods."
            logger.error(error_msg)
            raise ValueError(error_msg)
        return self._file_manager

    @file_manager.setter
    def file_manager(self, streamer_idx: int):
        self._file_manager = FileManager(load_file_manager_config(), streamer_idx)

    def store_video_logs(self, streamer_idx: int):
        """Store video logs to database for unprocessed videos.

        This method:
        1. Gets list of video files from file system
        2. Gets list of already stored video IDs from database
        3. Processes only new videos (present in file system but not in database)
        4. Stores processed video logs in bulk

        Args:
            streamer_idx (int): Streamer index to process videos for
        """

        video_data_paths = self.file_manager.get_video_data_paths()
        if not video_data_paths:
            logger.info(f"No video data found for streamer {streamer_idx}")
            return

        stored_video_ids = self.db_handler.get_video_ids(streamer_idx)
        video_logs = []

        for path in video_data_paths:
            media_metadata = self.file_manager.extract_metadata_from_path(path)
            if media_metadata.video_id not in stored_video_ids:
                video_log = VideoLog(
                    streamer_idx=streamer_idx,
                    video_url=str(path),
                    video_id=media_metadata.video_id,
                    category=media_metadata.category or "",
                    created_at=datetime.strptime(str(media_metadata.created_at), "%Y%m%d"),
                )
                video_logs.append(video_log)

        if video_logs:
            self.db_handler.insert_video_data_bulk(video_logs)
            logger.info(f"Stored {len(video_logs)} new video logs for streamer {streamer_idx}")

    def _crawl_chat_data_for_video(self, video_id: int, base_sleep_time: float = 0.5) -> bool:
        """Crawl chat data for a single video.

        Args:
            video_id (int): The ID of the video to crawl
            base_sleep_time (float): Base sleep time between API calls in seconds. Defaults to 0.5.

        Returns:
            bool: True if crawling was successful, False otherwise
        """

        next_player_message_time = 0
        retry_count = 0
        max_retries = 3

        while next_player_message_time is not None:
            try:
                data = self.crawler.request_chzzk_chats(video_id, next_player_message_time)
                if not data:
                    if retry_count < max_retries:
                        retry_count += 1
                        logger.warning(f"Retrying ({retry_count}/{max_retries}) for video_id: {video_id}")
                        continue
                    logger.error(f"❌ Failed to crawl chat data for video_id: {video_id} after {max_retries} retries")
                    return False

                video_chats, next_player_message_time = self.processor.parse_video_chats(data)
                self.file_manager.append_chats_to_jsonl(video_chats, video_id)
                retry_count = 0  # Reset retry count on success
                time.sleep(base_sleep_time * random.uniform(0.5, 1.5))
            except Exception as e:
                logger.error(f"❌ Error crawling chat data for video_id {video_id}: {e}")
                return False

        return True

    def crawl_chat_data(self, streamer_idx: int):
        """Crawl chat data for all videos of a streamer.

        This method orchestrates the chat data collection process for all videos
        associated with a specific streamer. It:
        1. Identifies videos that need chat data collection
        2. Skips videos that already have chat data
        3. Crawls chat data for each remaining video
        4. Tracks and reports progress and success rates

        Args:
            streamer_idx (int): The unique identifier of the streamer whose videos'
                chat data should be collected.

        Note:
            - The method uses pagination to handle large amounts of chat data
            - Progress is logged for each video and at the end of the process
            - Failed crawls are logged but don't stop the overall process
            - chat data files are preserved and not overwritten
        """
        stored_video_ids = self.db_handler.get_video_ids(streamer_idx)
        chat_data_video_ids = {
            self.file_manager.extract_metadata_from_path(path).video_id
            for path in self.file_manager.get_chat_data_paths()
        }

        video_ids_to_process = stored_video_ids - chat_data_video_ids
        logger.info(f"Starting chat data crawl for {video_ids_to_process} videos of streamer {streamer_idx}")

        processed_videos = 0
        successful_crawls = 0

        for video_id in video_ids_to_process:
            processed_videos += 1
            logger.info(f"Crawling chat data for video_id {video_id} [{processed_videos}/{len(video_ids_to_process)}]")
            if self._crawl_chat_data_for_video(video_id):
                successful_crawls += 1
                logger.info(f"✅ Successfully crawled chat data for video_id: {video_id}")
            else:
                logger.error(f"❌ Failed to crawl chat data for video_id: {video_id}")

        logger.info(
            f"Chat data crawl completed for streamer {streamer_idx}. "
            f"Successfully processed {successful_crawls}/{len(video_ids_to_process)} videos"
        )

    def _store_chat_logs_for_video(self, video_id: int, streamer_idx: int, batch_size: int = 1000):
        """Store chat logs for a single video.

        Args:
            video_id (int): ID of the video to store chat logs for
            streamer_idx (int): Streamer index for searching safely video_idx
            batch_size (int, optional): batch size for storing chat logs. Defaults to 1000.
        """
        video_idx = self.db_handler.get_video_idx(video_id, streamer_idx)
        for chats in self.file_manager.load_chats_from_jsonl_batch(video_id, batch_size):
            chat_logs = self.processor.extract_chat_logs(chats, video_idx)
            self.db_handler.insert_chat_data_bulk(chat_logs)
        logger.info(f"✅ Successfully stored chat logs for video_id, video_idx: {video_id}, {video_idx}")

    def store_chat_logs(self, streamer_idx: int):
        """Store chat logs for videos that need processing.

        This method:
        1. Gets video_id set of videos from database
        2. Gets video_id set of videos with chat data files
        3. Gets video_id set of videos with processed chats in db
        4. Processes and stores chat data for videos that:
           - Exist in database
           - Have chat data files
           - Don't have processed chats

        Args:
            streamer_idx (int): Streamer index to process chats for
        """
        stored_video_ids = self.db_handler.get_video_ids(streamer_idx, has_chat_data=False)
        chat_data_video_ids = {
            self.file_manager.extract_metadata_from_path(path).video_id
            for path in self.file_manager.get_chat_data_paths()
        }
        processed_chats_video_ids = self.db_handler.get_video_ids(streamer_idx, has_chat_data=True)

        # Calculate videos that need processing
        video_ids_to_process = (
            chat_data_video_ids  # Have chat data files
            & (stored_video_ids - processed_chats_video_ids)  # Not yet processed
        )

        if not video_ids_to_process:
            logger.info(f"No videos need chat processing for streamer {streamer_idx}")
            return

        logger.info(f"Processing chat data for {len(video_ids_to_process)} videos")
        for video_id in video_ids_to_process:
            self._store_chat_logs_for_video(video_id, streamer_idx)

    def run(self, streamer_idx: int):
        self.file_manager = streamer_idx
        self.store_video_logs(streamer_idx)
        self.crawl_chat_data(streamer_idx)
        self.store_chat_logs(streamer_idx)

import random
import time

from loguru import logger

from src.common.ChzzkDBHandler import ChzzkDBHandler
from src.common.config import load_db_config, load_file_manager_config
from src.common.FileManager import FileManager
from src.pipelines.vod_data_collection_pipeline.ChzzkChatCrawler import ChzzkChatCrawler
from src.pipelines.vod_data_collection_pipeline.ChzzkChatProcessor import ChzzkChatProcessor
from src.pipelines.vod_data_collection_pipeline.ChzzkVideoProcessor import ChzzkVideoProcessor
from src.pipelines.vod_data_collection_pipeline.config import (
    VODDataCollectionPipelineConfig,
)


class VODDataCollectionPipeline:
    """Pipeline for collecting and processing VOD (Video On Demand) data from CHZZK.

    This pipeline handles the entire workflow of collecting video and chat data from CHZZK,
    including crawling, processing, and storing the data. It manages the coordination
    between different components like crawlers, processors, and storage handlers.

    The pipeline follows these main steps:
    0. Collecting video data is now done on handmade because of not available on CHZZK API
    1. store video data in the database
    2. Crawl chat data for each video
    3. Process and store the chat data
    """

    def __init__(self, config: VODDataCollectionPipelineConfig):
        """Initialize the VODDataCollectionPipeline with configuration.

        Args:
            config (VODDataCollectionPipelineConfig): Configuration containing settings
                for chat API, chat processing, and other pipeline components.
        """
        self.crawler = ChzzkChatCrawler(config.chat_api_config)
        self.processor = ChzzkChatProcessor(config.chat_processor_config)
        self.db_handler = ChzzkDBHandler(load_db_config())
        self.video_processor = ChzzkVideoProcessor()

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
        file_manager = FileManager(load_file_manager_config(), streamer_idx)

        video_data_paths = file_manager.get_existing_video_data_paths()
        if not video_data_paths:
            logger.info(f"No video data found for streamer {streamer_idx}")
            return

        stored_video_ids = self.db_handler.get_existing_video_data_video_ids(streamer_idx)

        video_logs = []
        for path in video_data_paths:
            video_log = self.video_processor.process(path, streamer_idx)
            if video_log.video_id not in stored_video_ids:
                video_logs.append(video_log)

        if video_logs:
            self.db_handler.insert_video_data_bulk(streamer_idx, video_logs)
            logger.info(f"Stored {len(video_logs)} new video logs for streamer {streamer_idx}")

    def _crawl_chat_data_for_video(
        self, video_id: int, file_manager: FileManager, base_sleep_time: float = 0.5
    ) -> bool:
        """Crawl chat data for a single video.

        Args:
            video_id (int): The ID of the video to crawl
            file_manager (FileManager): File manager instance for saving data
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
                file_manager.append_chats_to_jsonl(video_chats, video_id)
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
            - Existing chat data files are preserved and not overwritten
        """
        file_manager = FileManager(load_file_manager_config(), streamer_idx)
        stored_video_ids = self.db_handler.get_existing_video_data_video_ids(streamer_idx)
        existing_chat_data_video_ids = file_manager.get_existing_chat_data_video_ids()

        total_videos = len(stored_video_ids)
        processed_videos = 0
        successful_crawls = 0

        logger.info(f"Starting chat data crawl for {total_videos} videos of streamer {streamer_idx}")

        for video_id in stored_video_ids:
            processed_videos += 1
            if video_id in existing_chat_data_video_ids:
                logger.info(f"Skipping video_id {video_id} (already exists) [{processed_videos}/{total_videos}]")
                continue

            logger.info(f"Crawling chat data for video_id {video_id} [{processed_videos}/{total_videos}]")
            if self._crawl_chat_data_for_video(video_id, file_manager):
                successful_crawls += 1
                logger.info(f"✅ Successfully crawled chat data for video_id: {video_id}")

        logger.info(
            f"Chat data crawl completed for streamer {streamer_idx}. "
            f"Successfully processed {successful_crawls}/{total_videos} videos"
        )

    def store_chat_logs(self, streamer_idx: int):
        # 이미 저장된 chats는 db에 저장하지 않도록 해야함
        # streamer_idx를 갖는 videos db의 video_id를 확인
        # chats폴더에 해당 video_id를 갖는 Chats이 있는지 확인
        # 없으면 chat_log를 추출하여 db에 저장
        pass

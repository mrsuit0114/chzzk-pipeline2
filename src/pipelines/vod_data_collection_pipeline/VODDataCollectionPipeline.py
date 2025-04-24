from loguru import logger

from src.common.ChzzkDBHandler import ChzzkDBHandler
from src.common.config import load_db_config, load_file_manager_config
from src.common.FileManager import FileManager
from src.pipelines.vod_data_collection_pipeline.ChzzkChatCrawler import ChzzkChatCrawler
from src.pipelines.vod_data_collection_pipeline.ChzzkChatProcessor import ChzzkChatProcessor
from src.pipelines.vod_data_collection_pipeline.ChzzkVideoProcessor import ChzzkVideoProcessor
from src.pipelines.vod_data_collection_pipeline.config import (
    VODDataCollectionPipelineConfig,
    load_vod_data_collection_pipeline_config,
)


class VODDataCollectionPipeline:
    def __init__(self, config: VODDataCollectionPipelineConfig):
        self.crawler = ChzzkChatCrawler(config.chat_api_config)
        self.processor = ChzzkChatProcessor(config.chat_processor_config)
        self.db_handler = ChzzkDBHandler(load_db_config())
        self.video_processor = ChzzkVideoProcessor()

    def store_video_logs(self, streamer_idx: int):
        """
        store video logs to db not processed video

        Args:
            streamer_idx (int): streamer index
        """
        file_manager = FileManager(load_file_manager_config(), streamer_idx)

        video_data_paths = file_manager.get_existing_video_data_paths()
        if not video_data_paths:
            logger.info(f"No video data found for streamer {streamer_idx}")
            return

        processed_video_ids = self.db_handler.get_existing_video_data_video_ids(streamer_idx)

        video_logs = []
        for path in video_data_paths:
            video_log = self.video_processor.process(path, streamer_idx)
            if video_log.video_id not in processed_video_ids:
                video_logs.append(video_log)

        if video_logs:
            self.db_handler.insert_video_data_bulk(streamer_idx, video_logs)


if __name__ == "__main__":
    config = load_vod_data_collection_pipeline_config()
    pipeline = VODDataCollectionPipeline(config)
    pipeline.store_video_logs(streamer_idx=1)

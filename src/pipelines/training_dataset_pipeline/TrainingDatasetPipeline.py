from typing import Optional

from loguru import logger

from src.common.config import load_file_manager_config
from src.common.FileManager import FileManager
from src.pipelines.training_dataset_pipeline.AudioProcessor import AudioProcessor
from src.pipelines.training_dataset_pipeline.config import TrainingDatasetPipelineConfig


class TrainingDatasetPipeline:
    def __init__(self, config: TrainingDatasetPipelineConfig):
        self.config = config
        self.audio_processor = AudioProcessor(config.audio_processor_config)
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

    def extract_audio_from_video(self, streamer_idx: int):
        """Extract audio from video and save it to file standardized by processor. Now it's mono and 16khz.

        It:
        1.

        Args:
            streamer_idx (int): Streamer index
        """
        file_manager = FileManager(load_file_manager_config(), streamer_idx)
        video_paths = file_manager.get_video_data_paths()
        audio_video_ids = {
            file_manager.extract_metadata_from_path(path).video_id for path in file_manager.get_audio_data_paths()
        }

        for video_path in video_paths:
            video_id = file_manager.extract_metadata_from_path(video_path).video_id
            if video_id in audio_video_ids:  # the audio already exists
                logger.info(f"Audio already exists for video {video_id}")
                continue
            audio_data, target_sr = self.audio_processor.extract_and_standardize_audio(video_path)
            media_metadata = file_manager.extract_metadata_from_path(video_path)
            file_manager.save_audio_data(audio_data, media_metadata, target_sr)

            logger.info(f"Extracted audio from video {video_id}")

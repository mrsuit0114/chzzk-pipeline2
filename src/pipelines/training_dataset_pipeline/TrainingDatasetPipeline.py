from typing import Optional

from loguru import logger

from src.common.config import load_file_manager_config
from src.common.FileManager import FileManager
from src.pipelines.training_dataset_pipeline.AudioProcessor import AudioProcessor
from src.pipelines.training_dataset_pipeline.config import TrainingDatasetPipelineConfig
from src.pipelines.training_dataset_pipeline.VADSegmentExtractor import VADSegmentExtractor


class TrainingDatasetPipeline:
    def __init__(self, config: TrainingDatasetPipelineConfig):
        self.config = config
        self.audio_processor = AudioProcessor(config.audio_config)
        self.vad_segment_extractor = VADSegmentExtractor(config.vad_segment_extractor_config)
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

    def extract_audios(self):
        """Extract audio from video and save it to file standardized by processor. Now it's mono and 16khz.
        If there are already audio files, it skips the extraction.
        """
        video_paths = self.file_manager.get_video_data_paths()
        audio_video_ids = {
            self.file_manager.extract_metadata_from_path(path).video_id
            for path in self.file_manager.get_audio_data_paths()
        }

        for video_path in video_paths:
            media_metadata = self.file_manager.extract_metadata_from_path(video_path)
            if media_metadata.video_id in audio_video_ids:
                continue
            audio_data, target_sr = self.audio_processor.extract_and_standardize_audio(video_path)
            self.file_manager.save_audio_data(audio_data, media_metadata, target_sr)

            logger.info(f"Extracted audio from video {media_metadata.video_id}")

    def extract_vad_segments(self):
        """Extract vad segments from audio and save it to file.
        If there are already vad segments files, it skips the extraction.
        """
        audio_paths = self.file_manager.get_audio_data_paths()
        vad_segment_video_ids = {
            self.file_manager.extract_metadata_from_path(path).video_id
            for path in self.file_manager.get_vad_segments_data_paths()
        }

        for audio_path in audio_paths:
            media_metadata = self.file_manager.extract_metadata_from_path(audio_path)
            if media_metadata.video_id in vad_segment_video_ids:
                continue
            speech_timestamps_ms = self.vad_segment_extractor.extract_vad_segments_from_audio(
                audio_path, self.config.audio_config.sample_rate
            )
            self.file_manager.save_vad_segments(speech_timestamps_ms, media_metadata)

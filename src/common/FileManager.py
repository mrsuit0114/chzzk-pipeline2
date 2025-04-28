# Chzzk Storage Handler But now local file system

import json
from pathlib import Path
from typing import Any, Generator

from loguru import logger

from src.common.config import FileManagerConfig


class FileManager:
    def __init__(self, config: FileManagerConfig, streamer_idx: int):
        """
        Initialize FileManager with configuration and streamer index.

        Args:
            config (FileManagerConfig): Configuration containing path settings
            streamer_idx (int): Index of the streamer to manage files for
        """
        self.config = config
        self._data_paths = config.get_data_paths(streamer_idx)

        self._verify_and_create_pahts()

    def _verify_and_create_pahts(self):
        """Verify and create necessary directories for file management.

        This method:
        1. Checks if each required directory exists
        2. Creates missing directories with parent directories if needed
        3. Logs the status of each directory

        Raises:
            Exception: If directory creation fails
        """
        for path in self._data_paths.__dict__.values():
            path_obj = Path(path)
            if not path_obj.exists():
                try:
                    logger.info(f"directory not exist: {path_obj} created")
                    path_obj.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logger.error(f"Error creating directory: {e}")
                    raise e
            else:
                logger.info(f"directory exist: {path_obj}")

    def extract_video_id_from_path(self, path: Path) -> int:
        """Extract video ID from path.

        Args:
            path (Path): Path to the video file
        """
        return int(path.stem.split("_")[-1])

    def _get_chat_file_path(self, video_id: int) -> Path:
        """Get path for chat file.

        Args:
            video_id (int): ID of the video

        Returns:
            Path: Path to the chat file
        """
        return self._data_paths.chat_data_dir / self.config.CHAT_FILE_FORMAT.format(video_id=video_id)

    def append_chats_to_jsonl(self, chats: list[dict[str, Any]], video_id: int):
        """Append chats to jsonl file for video_id

        Args:
            chats (list[dict[str,Any]]): VideoChatData.video_chats
            video_id (int): ID of the video to which the chats belong

        Raises:
            e: If there's an error writing to the file
        """
        file_path = self._get_chat_file_path(video_id)
        try:
            with open(file_path, "a") as f:
                for chat in chats:
                    f.write(json.dumps(chat) + "\n")
        except Exception as e:
            logger.error(f"Error appending chats to jsonl file: {e}")
            raise e

    def load_chats_from_jsonl_batch(
        self, video_id: int, batch_size: int = 1000
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Load chats from jsonl file in batches.

        Args:
            video_id (int): ID of the video to which the chats belong
            batch_size (int): Number of chats to load in each batch. Defaults to 1000.

        Yields:
            list[dict[str, Any]]: Batch of video chats

        Raises:
            e: If there's an error reading the file
        """
        file_path = self._get_chat_file_path(video_id)
        try:
            with open(file_path) as f:
                batch = []
                for line in f:
                    batch.append(json.loads(line))
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if batch:  # Yield remaining chats
                    yield batch
        except Exception as e:
            logger.error(f"Error loading chats from jsonl file: {e}")
            raise e

    def get_video_data_paths(self) -> set[Path]:
        """Get paths of all video files.

        Returns:
            set[Path]: Set of video file paths
        """
        try:
            return set(self._data_paths.video_data_dir.glob("*.mp4"))
        except Exception as e:
            logger.error(f"Error getting video data paths: {e}")
            raise e

    def get_chat_data_paths(self) -> set[Path]:
        """Get paths of all chat data files.

        Returns:
            set[Path]: Set of chat data file paths
        """
        try:
            return set(self._data_paths.chat_data_dir.glob("*.jsonl"))
        except Exception as e:
            logger.error(f"Error getting chat data paths: {e}")
            raise e

    def get_audio_data_paths(self) -> set[Path]:
        """Get paths of all audio files.

        Returns:
            set[Path]: Set of audio file paths (mp3, wav)
        """
        try:
            return set(self._data_paths.audio_data_dir.glob("*.mp3|*.wav"))
        except Exception as e:
            logger.error(f"Error getting audio data paths: {e}")
            raise e

# Chzzk Storage Handler But now local file system

import json
from pathlib import Path
from typing import Any

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
        self._paths = config.get_paths(streamer_idx)

        self._verify_and_create_pahts()

    def _verify_and_create_pahts(self):
        for path in self._paths.__dict__.values():
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

    def _get_chat_file_path(self, video_id: int) -> Path:
        """Get path for chat file.

        Args:
            video_id (int): ID of the video

        Returns:
            Path: Path to the chat file
        """
        return self._paths.chat_data_dir / self.config.CHAT_FILE_FORMAT.format(video_id=video_id)

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

    def load_chats_from_jsonl(self, video_id: int) -> list[dict[str, Any]]:
        """load chats from jsonl file for video_id

        Args:
            video_id (int): ID of the video to which the chats belong

        Raises:
            e: If there's an error reading the file

        Returns:
            list[dict[str, Any]]: VideoChatData.video_chats
        """
        file_path = self._get_chat_file_path(video_id)
        try:
            with open(file_path) as f:
                return [json.loads(line) for line in f]
        except Exception as e:
            logger.error(f"Error loading chats from jsonl file: {e}")
            raise e

    def get_existing_video_data_paths(self) -> set[Path]:
        """Get paths of all existing video files.

        Returns:
            set[Path]: Set of existing video file paths
        """
        try:
            return set(self._paths.video_data_dir.glob("*.mp4"))
        except Exception as e:
            logger.error(f"Error getting existing video data paths: {e}")
            raise e

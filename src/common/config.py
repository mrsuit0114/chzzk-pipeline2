import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StreamerPaths:
    chat_data_dir: Path
    chat_content_only_dir: Path
    audio_data_dir: Path
    video_data_dir: Path


@dataclass(frozen=True)
class FileManagerConfig:
    base_dir: Path
    CHATS_DIR_NAME: str = "chats"
    CHAT_CONTENTS_DIR_NAME: str = "chatcontents"
    AUDIOS_DIR_NAME: str = "audios"
    DATA_ROOT_DIR_NAME: str = "data"
    VIDEOS_DIR_NAME: str = "videos"

    # File name formats
    CHAT_FILE_FORMAT: str = "chats_{video_id}.jsonl"
    VIDEO_FILE_FORMAT: str = "{data}_{category}_{video_id}.mp4"
    AUDIO_FILE_FORMAT: str = "{date}_{category}_{video_id}.mp3"

    def get_paths(self, streamer_idx: int) -> StreamerPaths:
        data_root = self.base_dir / self.DATA_ROOT_DIR_NAME
        streamer_idx_str = str(streamer_idx)
        return StreamerPaths(
            chat_data_dir=data_root / streamer_idx_str / self.CHATS_DIR_NAME,
            chat_content_only_dir=data_root / streamer_idx_str / self.CHAT_CONTENTS_DIR_NAME,
            audio_data_dir=data_root / streamer_idx_str / self.AUDIOS_DIR_NAME,
            video_data_dir=data_root / streamer_idx_str / self.VIDEOS_DIR_NAME,
        )


def load_file_manager_config():
    config = FileManagerConfig(base_dir=Path(__file__).parent.parent.parent)
    return config


@dataclass(frozen=True)
class DBConfig:
    dbname: str
    user: str
    password: str
    host: str
    port: str


def load_db_config():
    config = DBConfig(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
    )
    return config

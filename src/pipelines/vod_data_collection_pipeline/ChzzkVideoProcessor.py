# 비디오 데이터의 오디오 데이터 추출, video_data_path로부터 video_log 생성

from datetime import datetime
from pathlib import Path

from src.common.models import VideoLog


class ChzzkVideoProcessor:
    def __init__(self):
        pass

    def process(self, video_data_path: Path, streamer_idx: int) -> VideoLog:
        """
        return VideoLog from video_data_path, ex) some_dir/{YYYYMMDD}_{category}_{video_id}.mp4
        Args:
            video_data_path (Path): path of video file
            streamer_idx (int): streamer index

        Returns:
            VideoLog: VideoLog from video_data_path
        """
        parts = video_data_path.stem.split("_")
        created_at = datetime.strptime(parts[0], "%Y%m%d")
        video_id = int(parts[-1])
        category = "_".join(parts[1:-1])

        return VideoLog(
            streamer_idx=streamer_idx,
            video_id=video_id,
            category=category,
            created_at=created_at,
            video_url=str(video_data_path),
        )

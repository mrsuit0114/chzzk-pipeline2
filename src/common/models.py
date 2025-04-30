from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class VideoChatData:
    """
    data['content']를 입력으로 받음
    """

    content: dict[str, Any]  # content 내부 구조를 감싸는 딕셔너리

    @property
    def next_player_message_time(self) -> int:
        return self.content.get("nextPlayerMessageTime", 0)

    @property
    def previous_video_chats(self) -> list[dict[str, Any]]:
        return self.content.get("previousVideoChats", [])

    @property
    def video_chats(self) -> list[dict[str, Any]]:
        return self.content.get("videoChats", [])


@dataclass
class ChatLog:
    video_idx: int
    content: str
    timestamp: int
    user_id_hash: str
    pay_amount: int
    os_type: str

    def __init__(self, chat: dict[str, Any], video_idx: int):
        self.video_idx = video_idx
        self.content = chat["content"]
        self.timestamp = chat["timestamp"]
        self.user_id_hash = chat["user_id_hash"]
        self.pay_amount = chat["pay_amount"]
        self.os_type = chat["os_type"]


@dataclass
class VideoLog:
    streamer_idx: int
    video_id: int
    category: str
    created_at: datetime
    video_url: str

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
    player_msg_time: int
    user_id_hash: str
    pay_amount: int
    os_type: str

    def __init__(self, chat: dict[str, Any], video_idx: int):
        self.video_idx = video_idx
        self.content = chat["content"]
        self.player_msg_time = chat["player_msg_time"]
        self.user_id_hash = chat["user_id_hash"]
        self.pay_amount = chat["pay_amount"]
        self.os_type = chat["os_type"]

    def to_dict(self):
        return {
            "video_idx": self.video_idx,
            "chat_text": self.content,
            "chat_time": self.player_msg_time,
            "user_id_hash": self.user_id_hash,
            "pay_amount": self.pay_amount,
            "os_type": self.os_type,
        }


@dataclass
class VideoLog:
    streamer_idx: int
    video_id: int
    category: str
    created_at: datetime
    video_url: str

    def to_dict(self):
        return {
            "streamer_idx": self.streamer_idx,
            "video_id": self.video_id,
            "category": self.category,
            "created_at": self.created_at,
            "video_url": self.video_url,
        }

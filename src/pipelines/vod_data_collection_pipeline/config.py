"""
Configuration settings for VOD data collection pipeline
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class ChatAPIConfig:
    _base_url: str
    _headers: dict[str, str]
    _chat_endpoint: str

    def get_chats_url_of_video_id(self, video_id: int) -> str:
        return f"{self._base_url}/{video_id}/{self._chat_endpoint}"

    def get_headers(self):
        return self._headers


def load_chat_api_config():
    config = ChatAPIConfig(
        _base_url=os.environ["VIDEOCHATS_BASE_URL"],
        _headers={"User-Agent": os.environ["USER_AGENT"]},
        _chat_endpoint="chats",
    )
    return config


@dataclass(frozen=True)
class ChzzkChatProcessorConfig:
    message_type_chat_code: int = 1
    message_type_donation_code: int = 10
    message_status_normal_type: str = "NORMAL"
    donation_type: str = "CHAT"


def load_chzzk_chat_processor_config():
    config = ChzzkChatProcessorConfig()
    return config


@dataclass(frozen=True)
class VODDataCollectionPipelineConfig:
    chat_processor_config: ChzzkChatProcessorConfig
    chat_api_config: ChatAPIConfig


def load_vod_data_collection_pipeline_config():
    config = VODDataCollectionPipelineConfig(
        chat_processor_config=load_chzzk_chat_processor_config(),
        chat_api_config=load_chat_api_config(),
    )
    return config

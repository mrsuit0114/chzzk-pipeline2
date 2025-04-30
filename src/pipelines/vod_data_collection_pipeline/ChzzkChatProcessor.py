# crawler에서 받은 raw data를 처리하며, 처리된 데이터를 chat_log 형식으로 변환하는 클래스
import json
from typing import Any, Optional

from loguru import logger

from src.common.models import ChatLog, VideoChatData
from src.pipelines.vod_data_collection_pipeline.config import ChzzkChatProcessorConfig


class ChzzkChatProcessor:
    """Processor for CHZZK chat data.

    This class processes raw chat data from the CHZZK API, filtering and transforming it
    into a standardized format. It handles different types of chat messages (normal chat,
    donations, subscriptions, etc.) and extracts relevant information.

    Attributes:
        config (ChzzkChatProcessorConfig): Configuration for chat message types and status codes
    """

    def __init__(self, config: ChzzkChatProcessorConfig):
        """Initialize the ChzzkChatProcessor with configuration.

        Args:
            config (ChzzkChatProcessorConfig): Configuration containing message type codes
                and status types for filtering chat messages.
        """
        self.config = config

    def _parse_chat_data(self, chat: dict[str, Any]) -> dict[str, Any] | None:
        """Parse a single chat message and extract relevant information.

        This method processes a raw chat message, extracting and validating its content.
        It filters out system messages, blind chats, and mission donations, keeping only
        normal chats and regular donations.

        Args:
            chat (dict[str, Any]): Raw chat message data from the API

        Returns:
            dict[str, Any] | None: Processed chat data if the message is valid,
                None if the message should be filtered out

        Note:
            Valid messages are:
            - Normal chats (messageTypeCode == 1)
            - Regular donations (donationType == "CHAT")
            - Messages with normal status (messageStatusType == "NORMAL")
        """
        # 기본 field
        msg_type_code = chat.get("messageTypeCode")  # 1, 10, 11, 30 일반, 후원, 구독, 시스템
        msg_status_type = chat.get("messageStatusType")
        content = chat.get("content", "")
        player_msg_time = chat["playerMessageTime"]
        user_id_hash = chat["userIdHash"]

        # extras 파싱
        extras = json.loads(chat["extras"])
        donation_type = extras.get("donationType")  # 일반 채팅의 경우 없을 수 있음
        pay_amount = extras.get("payAmount", 0)  # 일반 채팅의 경우
        os_type = extras.get("osType", "not_pc")  # PC가 아닌 후원은 없는 것으로 보임

        # 필터링 조건 검사
        is_valid = msg_status_type == self.config.message_status_normal_type and (
            msg_type_code == self.config.message_type_chat_code or donation_type == self.config.donation_type
        )

        if not is_valid:
            return None

        return {
            "content": content,
            "timestamp": player_msg_time,
            "user_id_hash": user_id_hash,
            "pay_amount": pay_amount,
            "os_type": os_type,
        }

    def _extract_chat_log(self, chat: dict[str, Any], video_idx: int) -> Optional[ChatLog]:
        """Convert a parsed chat message into a ChatLog object.

        Args:
            chat (dict[str, Any]): Raw chat message data
            video_idx (int): The ID of the video this chat belongs to

        Returns:
            Optional[ChatLog]: ChatLog object if the message is valid, None otherwise
        """
        chat_log_dict = self._parse_chat_data(chat)
        if chat_log_dict is None:
            return None
        return ChatLog(chat_log_dict, video_idx)

    def extract_chat_logs(self, chats: list[dict[str, Any]], video_idx: int) -> list[ChatLog]:
        """Process a list of chat messages and convert them to ChatLog objects.

        Args:
            chats (list[dict[str, Any]]): List of raw chat messages
            video_idx (int): The ID of the video these chats belong to

        Returns:
            list[ChatLog]: List of processed ChatLog objects
        """
        chat_logs = []
        for chat in chats:
            chat_log = self._extract_chat_log(chat, video_idx)
            if chat_log is not None:
                chat_logs.append(chat_log)
        return chat_logs

    def parse_video_chats(self, data: dict[str, Any]) -> tuple[list[dict[str, Any]], Optional[int]]:
        """Extract chat messages and next timestamp from API response data.

        This method processes the raw API response to extract the list of chat messages
        and the timestamp for the next batch of messages.

        Args:
            data (dict[str, Any]): Raw response data from the API

        Returns:
            tuple[list[dict[str, Any]], Optional[int]]: A tuple containing:
                - List of chat messages
                - Timestamp for the next batch of messages (None if no more messages)

        Raises:
            ValueError: If the response data is missing required content
        """
        content = data.get("content")
        if not content:
            logger.error("⚠️ No content in data.")
            raise ValueError("Content is missing in the response data.")
        video_chat_data = VideoChatData(content)
        return video_chat_data.video_chats, video_chat_data.next_player_message_time

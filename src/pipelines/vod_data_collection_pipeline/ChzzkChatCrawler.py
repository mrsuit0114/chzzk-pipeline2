from typing import Any, Optional

import requests
from loguru import logger

from src.pipelines.vod_data_collection_pipeline.config import ChatAPIConfig


class ChzzkChatCrawler:
    """A crawler for fetching chat data from the CHZZK API.

    This class handles the HTTP communication with the CHZZK API to fetch chat data
    for specific videos. It manages API requests, error handling, and response parsing.

    Attributes:
        chat_api_config (ChatAPIConfig): Configuration for API endpoints and headers
    """

    def __init__(self, chat_api_config: ChatAPIConfig):
        """Initialize the ChzzkChatCrawler with API configuration.

        Args:
            chat_api_config (ChatAPIConfig): Configuration containing API endpoints,
                headers, and other necessary settings for making requests to the CHZZK API.
        """
        self.chat_api_config = chat_api_config

    def request_chzzk_chats(self, video_id: int, player_message_time: int) -> Optional[dict[str, Any]]:
        """Fetch chat data from CHZZK API for a specific video and timestamp.

        This method makes an HTTP GET request to the CHZZK API to fetch chat data.
        It handles a maximum of 200 chat messages per request because of the API limit, and the timestamp
        parameter is used to paginate through the chat history.

        Args:
            video_id (int): The unique identifier of the video to fetch chats for.
            player_message_time (int): The timestamp in milliseconds indicating the
                point in the video to fetch chats from. This is used for pagination.

        Returns:
            Optional[dict[str, Any]]: The parsed JSON response containing chat data if
                the request is successful, None if the request fails.
        """
        url = self.chat_api_config.get_chats_url_of_video_id(video_id)
        params = {"playerMessageTime": player_message_time}

        try:
            response = requests.get(url, headers=self.chat_api_config.get_headers(), params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed: {e}")
            return None

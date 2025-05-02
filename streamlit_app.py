# streamlit에서는 vad_segments의 파일을 선택하고
# merge_threshold, 0~100사이의 값 - vod의 start_time, end_time를 기준으로 어느 구간을 채팅의 이전, 이후로 표시할지 결정, 맥락seconds로 어디까지를 해당 vod의 이전 이후로 판단할지 결정
# 적용버튼을 클릭하면 해당 파일에 맞는 audio플레이어를 띄워주고
# merge_threshold에 따른 최종 구간이 구간마다 표시되고 시간을 클릭하여 해당 구간을 재생하거나 아래로 펼쳐져서 구간에 포함되는 이전맥락, 이후맥락 채팅을 확인할 수 있도록 표시

import json

import streamlit as st

from src.common.ChzzkDBHandler import ChzzkDBHandler
from src.common.config import load_db_config, load_file_manager_config
from src.common.FileManager import FileManager
from src.pipelines.training_dataset_pipeline.config import load_vad_segment_extractor_config
from src.pipelines.training_dataset_pipeline.VADSegmentExtractor import VADSegmentExtractor


def get_chats_in_range(video_id: int, start_ms: int, end_ms: int, db_handler: ChzzkDBHandler) -> list[dict]:
    """Get chats within the specified time range from database."""
    query = """
    SELECT content, timestamp
    FROM chats c
    JOIN videos v ON c.video_idx = v.video_idx
    WHERE v.video_id = %(video_id)s
    AND c.timestamp BETWEEN %(start_ms)s AND %(end_ms)s
    ORDER BY timestamp
    """
    return db_handler._select_query(query, params={"video_id": video_id, "start_ms": start_ms, "end_ms": end_ms})


def format_timestamp(ms: int) -> str:
    """Format milliseconds to HH:MM:SS format."""
    total_seconds = ms // 1000
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def get_segment_length(start_ms: int, end_ms: int) -> float:
    """Get segment length in seconds."""
    return (end_ms - start_ms) / 1000


def main():
    st.title("VAD Segments Viewer")

    # Initialize components
    file_manager = FileManager(load_file_manager_config(), streamer_idx=1)  # streamer_idx는 필요에 따라 변경
    vad_extractor = VADSegmentExtractor(load_vad_segment_extractor_config())
    db_handler = ChzzkDBHandler(load_db_config())

    # Get VAD segment files
    vad_files = list(file_manager._data_paths.vad_segments_dir.glob("*.json"))
    if not vad_files:
        st.error("No VAD segment files found!")
        return

    # File selection
    selected_file = st.selectbox("Select VAD segment file", options=vad_files, format_func=lambda x: x.name)

    # Load VAD segments
    with open(selected_file) as f:
        vad_data = json.load(f)

    # Extract metadata
    media_metadata = file_manager.extract_metadata_from_path(selected_file)
    video_id = media_metadata.video_id

    # Get corresponding audio file
    audio_file = next(file_manager._data_paths.audio_data_dir.glob(f"*_{video_id}.mp3"), None)
    if not audio_file:
        st.error("Audio file not found!")
        return

    # Input parameters
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        merge_threshold_ms = st.number_input("Merge threshold (ms)", min_value=0, max_value=5000, value=1000, step=100)
    with col2:
        context_seconds = st.number_input("Context (seconds)", min_value=0, max_value=100, value=10, step=5)
    with col3:
        min_length = st.number_input("Min length (s)", min_value=0.0, max_value=100.0, value=1.0, step=0.5)
    with col4:
        max_length = st.number_input("Max length (s)", min_value=0.0, max_value=100.0, value=10.0, step=0.5)

    # Apply button
    if st.button("Apply"):
        # Get merged segments
        merged_segments = vad_extractor.merge_segments(vad_data["speech_timestamps_ms"], merge_threshold_ms)

        # Filter segments by length
        filtered_segments = [
            (start_ms, end_ms)
            for start_ms, end_ms in merged_segments
            if min_length <= get_segment_length(start_ms, end_ms) <= max_length
        ]

        # Display audio player
        st.audio(str(audio_file))

        # Display segments
        st.subheader(f"Speech Segments (showing {len(filtered_segments)} of {len(merged_segments)} segments)")
        for i, (start_ms, end_ms) in enumerate(filtered_segments, 1):
            segment_length = get_segment_length(start_ms, end_ms)
            with st.expander(
                f"Segment {i}: {format_timestamp(start_ms)} - {format_timestamp(end_ms)} ({segment_length:.1f}s)"
            ):
                context_ms = context_seconds * 1000
                before_start = max(0, start_ms - context_ms)
                after_end = end_ms + context_ms

                # Get chats for this segment and context from DB
                with db_handler:
                    # Get before context chats
                    before_chats = get_chats_in_range(video_id, before_start, start_ms - 1, db_handler)

                    # Get segment chats
                    segment_chats = get_chats_in_range(video_id, start_ms, end_ms, db_handler)

                    # Get after context chats
                    after_chats = get_chats_in_range(video_id, end_ms + 1, after_end, db_handler)

                # Display context chats
                if before_chats:
                    st.markdown(
                        '<p style="color: #4B8BBE; font-size: 1.2em; font-weight: bold; margin: 10px 0;">Before Context:</p>',
                        unsafe_allow_html=True,
                    )
                    for chat in before_chats:
                        st.text(f"{format_timestamp(chat['timestamp'])}: {chat['content']}")

                # Display segment chats
                st.markdown(
                    '<p style="color: #FF4B4B; font-size: 1.2em; font-weight: bold; margin: 10px 0;">Segment Chats:</p>',
                    unsafe_allow_html=True,
                )
                for chat in segment_chats:
                    st.text(f"{format_timestamp(chat['timestamp'])}: {chat['content']}")

                # Display after context chats
                if after_chats:
                    st.markdown(
                        '<p style="color: #4B8BBE; font-size: 1.2em; font-weight: bold; margin: 10px 0;">After Context:</p>',
                        unsafe_allow_html=True,
                    )
                    for chat in after_chats:
                        st.text(f"{format_timestamp(chat['timestamp'])}: {chat['content']}")

                # Play button for this segment
                if st.button(f"Play Segment {i}", key=f"play_{i}"):
                    st.audio(str(audio_file), start_time=start_ms / 1000)


if __name__ == "__main__":
    main()

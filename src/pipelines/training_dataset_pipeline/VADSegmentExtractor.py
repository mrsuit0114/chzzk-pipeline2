# vad로 구간을 나누고 processed > vad_segments 폴더에 (created_at)_(category)_(video_id).json 파일로 저장
# json은 merge_threshold_ms와 segments: list[tuple[start_time, end_time]] 형식으로 저장
# vda segments를 기준에 맞춰 저장하고 streamlit에서 구간에 맞게 채팅을 띄울 수 있도록 해야겠네

from pathlib import Path

from loguru import logger

from src.pipelines.training_dataset_pipeline.config import VADSegmentExtractorConfig


class VADSegmentExtractor:
    def __init__(self, config: VADSegmentExtractorConfig):
        """Initialize VADSegmentExtractor. now it's silero vad model.

        Args:
            config (VADSegmentExtractorConfig): config for vad segment extractor

        Note:
            It fits only silero vad model by provided silero utils.
        """
        self.config = config

    def extract_vad_segments_from_audio(self, audio_path: Path, sample_rate: int) -> list[tuple[int, int]]:
        """Extract vad segments from audio.

        Args:
            audio_path (Path): path to audio file
            sample_rate (int): sample rate of audio file

        Returns:
            list[tuple[int, int]]: list of speech timestamps in milliseconds
        """

        vad_model, vad_utils = self.config.load_vad_model()
        (get_speech_timestamps, _, read_audio, _, _) = vad_utils

        wav = read_audio(audio_path, sample_rate)
        ms_per_sample = 1000 / sample_rate

        logger.info(f"Extracting VAD segments from audio {audio_path}")
        speech_timestamps = get_speech_timestamps(wav, vad_model, sampling_rate=sample_rate)
        speech_timestamps_ms = [
            (int(ts["start"] * ms_per_sample), int(ts["end"] * ms_per_sample)) for ts in speech_timestamps
        ]
        logger.info(f"Extracted VAD segments from audio {audio_path}")
        return speech_timestamps_ms

    def merge_segments(
        self,
        speech_timestamps_ms: list[tuple[int, int]],
        merge_threshold_ms: int,
        min_length_ms: float,
        max_length_ms: float,
    ) -> list[tuple[int, int]]:
        """Merge segments if the difference between the end of the current segment and the start of the next segment is less than the merge threshold.

        Args:
            speech_timestamps_ms (list[tuple[int, int]]): list of speech timestamps in milliseconds
            merge_threshold_ms (int): threshold for merging segments

        Returns:
            list[tuple[int, int]]: list of merged speech timestamps in milliseconds
        """
        if not speech_timestamps_ms:
            return []
        merged_segments = []
        curr_start, curr_end = speech_timestamps_ms[0]

        for start, end in speech_timestamps_ms[1:]:
            if start - curr_end <= merge_threshold_ms:
                curr_end = end
            else:
                if min_length_ms <= curr_end - curr_start <= max_length_ms:
                    merged_segments.append((curr_start, curr_end))
                curr_start, curr_end = start, end

        if min_length_ms <= curr_end - curr_start <= max_length_ms:
            merged_segments.append((curr_start, curr_end))
        return merged_segments

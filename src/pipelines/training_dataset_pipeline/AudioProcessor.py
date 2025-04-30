# 추출할 때 16khz로 추출할 것 - 용량, 모델 입력, 속도
# 스테레오이므로 어떤 채널을 쓸지도 고려해야겠는데
# audio data path를 받아서 추출까지만
# 추출은 몰라도 학습 때는 torchaudio를 사용
# 정규화 해야 다른 오디오에 대해서도 사용 가능
import io
import subprocess
from pathlib import Path

import librosa
import numpy as np

from src.pipelines.training_dataset_pipeline.config import AudioProcessorConfig


class AudioProcessor:
    def __init__(self, audio_processor_config: AudioProcessorConfig):
        self.audio_processor_config = audio_processor_config

    def _extract_audio(self, video_path: Path):
        """Extract audio from video using ffmpeg.

        Args:
            video_path (Path): Path to video file

        Raises:
            RuntimeError: If there's an error extracting audio from video

        Returns:
            tuple[np.ndarray, int]: audio data and sample rate
        """
        try:
            command = [
                "ffmpeg",
                "-i",
                str(video_path),
                "-vn",  # 비디오 스트림 제외
                "-acodec",
                "libmp3lame",  # MP3 코덱 사용
                "-q:a",
                "2",  # 품질 설정 (0-9, 낮을수록 품질 좋음)
                "-f",
                "mp3",  # MP3 형식으로 출력
                "-",  # 표준 출력으로 전송
            ]

            # 명령어 실행 및 출력 캡처
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            audio_data, stderr = process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                raise RuntimeError(f"오디오 추출 중 오류 발생: {error_msg}")

            # 메모리의 바이트 데이터를 librosa로 로드
            audio_io = io.BytesIO(audio_data)
            y, sr = librosa.load(audio_io, sr=None, mono=False)  # 모노 변환은 별도 단계로 처리

            return y, sr

        except Exception as e:
            raise RuntimeError(f"오디오 추출 실패: {str(e)}")

    def _convert_to_mono(self, audio_data):
        """Convert audio data to mono.

        Args:
            audio_data (np.ndarray): audio data

        Returns:
            np.ndarray: mono audio data
        """
        if audio_data.ndim == 1 or audio_data.shape[0] == 1:
            return audio_data

        return np.mean(audio_data, axis=0)

    def _resample(self, audio_data, original_sr):
        """Resample audio data to target sample rate.

        Args:
            audio_data (np.ndarray): audio data
            original_sr (int): original sample rate

        Returns:
            tuple[np.ndarray, int]: resampled audio data and target sample rate
        """
        target_sr = self.audio_processor_config.sample_rate
        if original_sr == target_sr:
            return audio_data, target_sr

        resampled = librosa.resample(audio_data, orig_sr=original_sr, target_sr=target_sr)
        return resampled, target_sr

    def extract_and_standardize_audio(self, video_path: Path):
        """Extract audio from video and standardize it. Now it's mono and 16khz.

        Args:
            video_path (Path): Path to video file

        Returns:
            tuple[np.ndarray, int]: standardize audio data and sample rate
        """
        audio_data, original_sr = self._extract_audio(video_path)
        audio_data = self._convert_to_mono(audio_data)
        audio_data, target_sr = self._resample(audio_data, original_sr)
        return audio_data, target_sr

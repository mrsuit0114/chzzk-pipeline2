from dataclasses import dataclass

import torch


@dataclass(frozen=True)
class MediaMetadata:
    video_id: int
    category: str | None
    created_at: int | None


@dataclass(frozen=True)
class AudioConfig:
    sample_rate: int = 16000


def load_audio_config():
    config = AudioConfig()
    return config


@dataclass(frozen=True)
class VADSegmentExtractorConfig:
    def load_vad_model(self):
        model, utils = torch.hub.load(
            repo_or_dir="snakers4/silero-vad", model="silero_vad", force_reload=False, onnx=False
        )
        return model, utils


def load_vad_segment_extractor_config():
    config = VADSegmentExtractorConfig()
    return config


@dataclass(frozen=True)
class TrainingDatasetPipelineConfig:
    audio_config: AudioConfig
    vad_segment_extractor_config: VADSegmentExtractorConfig


def load_training_dataset_pipeline_config():
    config = TrainingDatasetPipelineConfig(
        audio_config=load_audio_config(),
        vad_segment_extractor_config=load_vad_segment_extractor_config(),
    )
    return config

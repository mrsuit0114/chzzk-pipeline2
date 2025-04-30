from dataclasses import dataclass


@dataclass(frozen=True)
class MediaMetadata:
    video_id: int
    category: str | None
    created_at: int | None


@dataclass(frozen=True)
class AudioProcessorConfig:
    sample_rate: int


def load_audio_processor_config():
    config = AudioProcessorConfig(sample_rate=16000)
    return config


@dataclass(frozen=True)
class TrainingDatasetPipelineConfig:
    audio_processor_config: AudioProcessorConfig


def load_training_dataset_pipeline_config():
    config = TrainingDatasetPipelineConfig(
        audio_processor_config=load_audio_processor_config(),
    )
    return config

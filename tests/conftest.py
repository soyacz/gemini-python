import pytest

from gemini_python import GeminiConfiguration


@pytest.fixture
def config():
    """Default gemini test configuration"""
    return GeminiConfiguration(seed=1234)


@pytest.fixture
def simple_schema_config():
    """GeminiConfiguration with only 1 pk and 1 ck and 1 column"""
    return GeminiConfiguration(
        seed=1234,
        min_columns=1,
        max_columns=1,
        min_clustering_keys=1,
        max_clustering_keys=1,
        min_partition_keys=1,
        max_partition_keys=1,
    )

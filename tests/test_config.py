from cosmic_rays.config import Settings


def test_default_pipeline_contract_is_preserved():
    cfg = Settings()
    assert cfg.stream_topic == "topic_stream"
    assert cfg.results_topic == "topic_results"
    assert cfg.topic_partitions == 14
    assert cfg.spark_master == "spark://pd-master:7077"

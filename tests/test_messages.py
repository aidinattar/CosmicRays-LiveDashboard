import numpy as np

from cosmic_rays.messages import (
    add_to_cumulative_message,
    empty_result_message,
    initialise_cumulative_message,
)


def _sample_message(epoch=1, count=2):
    message = empty_result_message()
    message["epoch_id"] = epoch
    message["time"] = 0.5
    message["hits"] = count
    message["CH0"]["total_hits"] = count
    message["CH0"]["histo_CH"] = {"bin_edges": [3], "bin_counts": [count]}
    return message


def test_result_message_has_stable_wire_shape():
    message = empty_result_message()
    assert set(message) == {"time", "epoch_id", "hits", "CH0", "CH1", "CH2", "CH3"}
    assert set(message["CH0"]) == {"total_hits", "histo_CH", "histo_ORB", "histo_SC"}


def test_cumulative_histogram_preserves_sparse_bin_indices():
    cumulative = initialise_cumulative_message(_sample_message(epoch=1, count=2))
    assert cumulative["CH0"]["histo_CH"]["bin_counts"][3] == 2
    assert len(cumulative["CH0"]["histo_CH"]["bin_counts"]) == 128

    add_to_cumulative_message(cumulative, _sample_message(epoch=2, count=4))
    assert cumulative["CH0"]["histo_CH"]["bin_counts"][3] == 6
    assert cumulative["hits"] == 6
    assert cumulative["epoch_id"] == 2
    assert np.isclose(cumulative["time"], 1.0)

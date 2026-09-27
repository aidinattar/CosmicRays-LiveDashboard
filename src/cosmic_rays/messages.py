"""Result-message helpers shared by computation and dashboard code."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

import numpy as np

HistogramMessage = Dict[str, Any]


def empty_result_message() -> HistogramMessage:
    """Return a fresh result message with the historical wire format."""
    chamber = {
        "total_hits": 0,
        "histo_CH": {"bin_edges": [], "bin_counts": []},
        "histo_ORB": {"bin_edges": [], "bin_counts": []},
        "histo_SC": {"bin_edges": [], "bin_counts": []},
    }
    return {
        "time": 0.0,
        "epoch_id": 0,
        "hits": 0,
        **{f"CH{i}": deepcopy(chamber) for i in range(4)},
    }


def initialise_cumulative_message(message: HistogramMessage) -> HistogramMessage:
    """Convert sparse histogram messages to fixed 128-bin cumulative arrays."""
    result = deepcopy(message)
    for chamber in ("CH0", "CH1", "CH2", "CH3"):
        for feature in ("histo_CH", "histo_ORB", "histo_SC"):
            edges = np.arange(128)
            counts = np.zeros(128, dtype=int)
            new_edges = np.asarray(
                [int(value) for value in result[chamber][feature]["bin_edges"]],
                dtype=int,
            )
            if len(new_edges):
                counts[new_edges] += np.asarray(
                    result[chamber][feature]["bin_counts"], dtype=int
                )
            result[chamber][feature]["bin_edges"] = edges
            result[chamber][feature]["bin_counts"] = counts
    return result


def add_to_cumulative_message(
    previous: HistogramMessage, current: HistogramMessage
) -> HistogramMessage:
    """Add one sparse batch result to an existing cumulative result in place."""
    previous["time"] += current["time"]
    previous["epoch_id"] = current["epoch_id"]
    previous["hits"] += current["hits"]

    for chamber in ("CH0", "CH1", "CH2", "CH3"):
        previous[chamber]["total_hits"] += current[chamber]["total_hits"]
        for feature in ("histo_CH", "histo_ORB", "histo_SC"):
            new_edges = np.asarray(
                [int(value) for value in current[chamber][feature]["bin_edges"]],
                dtype=int,
            )
            if len(new_edges):
                previous[chamber][feature]["bin_counts"][new_edges] += np.asarray(
                    current[chamber][feature]["bin_counts"], dtype=int
                )
    return previous

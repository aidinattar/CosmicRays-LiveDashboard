"""Standalone Matplotlib live dashboard for processed Kafka results."""

from __future__ import annotations

import argparse
import json
from collections import deque
from typing import Deque, Dict, Tuple

import matplotlib.pyplot as plt
import numpy as np
from kafka import KafkaConsumer
from matplotlib.gridspec import GridSpec

from cosmic_rays.config import settings
from cosmic_rays.messages import add_to_cumulative_message, initialise_cumulative_message


HISTOGRAMS = (
    ("histo_CH", "TDC channel", "Active TDC channels per chamber"),
    ("histo_ORB", "Active channels / orbit", "Active channels per orbit"),
    (
        "histo_SC",
        "TDC channel",
        "Active TDC channels with scintillator coincidence",
    ),
)


def create_figure():
    fig, axes = plt.subplots(nrows=4, ncols=4, sharey="row", figsize=(15, 17))
    for axis in axes[3, :]:
        axis.remove()

    gs = GridSpec(4, 4, figure=fig)
    time_axis = fig.add_subplot(gs[3, :])
    fig.suptitle("Cosmic Rays · Live Detector Monitor", fontsize=24, y=0.995)
    fig.subplots_adjust(hspace=0.55, top=0.93, bottom=0.06)
    return fig, axes, time_axis


def _apply_axes_limits(axes, mode: str) -> None:
    x_limits = ((0, 63), (64, 127), (0, 63), (64, 127))
    for column, limits in enumerate(x_limits):
        axes[0, column].set_xlim(limits)
        axes[2, column].set_xlim(limits)
        axes[1, column].set_xlim((0, 15))

    if mode == "single":
        for column in range(4):
            axes[0, column].set_ylim((0, 100))
            axes[1, column].set_ylim((0, 750))
            axes[2, column].set_ylim((0, 3))


def update_figure(
    fig,
    axes,
    time_axis,
    message,
    mode: str,
    batch_ids: Deque[int],
    batch_times: Deque[float],
) -> None:
    for row, (feature, xlabel, title) in enumerate(HISTOGRAMS):
        for chamber_id in range(4):
            axis = axes[row, chamber_id]
            axis.clear()
            axis.grid(linestyle="dotted")
            axis.set_title(f"CH{chamber_id}")
            axis.set_xlabel(xlabel)
            chamber = message[f"CH{chamber_id}"][feature]
            axis.bar(
                [int(value) for value in chamber["bin_edges"]],
                chamber["bin_counts"],
            )
        axes[row, 0].set_ylabel(title)

    batch_ids.append(int(message["epoch_id"]))
    batch_times.append(float(message["time"]))
    time_axis.clear()
    time_axis.grid(linestyle="dotted")
    time_axis.bar(list(batch_ids), list(batch_times))
    time_axis.set_xlabel("epoch id")
    time_axis.set_ylabel("processing time (s)")
    time_axis.set_title("Recent processing latency")

    _apply_axes_limits(axes, mode)
    fig.canvas.draw_idle()
    fig.canvas.flush_events()
    plt.pause(0.001)


def print_summary(message) -> None:
    chamber_hits = "  ".join(
        f"CH{i}: {message[f'CH{i}']['total_hits']}" for i in range(4)
    )
    print(
        f"epoch={message['epoch_id']}  hits={message['hits']}  "
        f"processing={message['time']:.3f}s  {chamber_hits}"
    )


def run(mode: str = "single") -> None:
    consumer = KafkaConsumer(
        settings.results_topic,
        bootstrap_servers=list(settings.kafka_bootstrap_servers),
        consumer_timeout_ms=100000,
        auto_offset_reset="latest",
    )
    plt.ion()
    fig, axes, time_axis = create_figure()
    batch_ids: Deque[int] = deque(maxlen=30)
    batch_times: Deque[float] = deque(maxlen=30)
    cumulative = None

    print(
        f"Dashboard consuming {settings.results_topic!r} in {mode!r} mode. "
        "Close the figure or press Ctrl+C to stop."
    )
    try:
        for kafka_message in consumer:
            if not plt.fignum_exists(fig.number):
                break
            message = json.loads(kafka_message.value)
            if mode == "cumulative":
                if cumulative is None:
                    cumulative = initialise_cumulative_message(message)
                else:
                    cumulative = add_to_cumulative_message(cumulative, message)
                shown = cumulative
            else:
                shown = message

            print_summary(shown)
            update_figure(
                fig,
                axes,
                time_axis,
                shown,
                mode,
                batch_ids,
                batch_times,
            )
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        consumer.close()
        plt.ioff()
        plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Live Kafka dashboard for miniDT results.")
    parser.add_argument(
        "--mode",
        choices=("single", "cumulative"),
        default="single",
        help="Show individual batches or cumulative histograms.",
    )
    args = parser.parse_args()
    run(mode=args.mode)


if __name__ == "__main__":
    main()

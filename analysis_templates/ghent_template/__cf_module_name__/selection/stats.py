# coding: utf-8

"""
Stat-related methods.
"""
from __future__ import annotations

import functools

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.stats import increment_stats
from columnflow.production import Producer, producer
from columnflow.production.cms.btag import btag_weights
from __cf_short_name_lc__.production.weights import event_weights_to_normalize

from columnflow.util import maybe_import
from columnflow.columnar_util import optional_column, has_ak_column
from columnflow.ml import MLModel

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={
        increment_stats,
        event_weights_to_normalize,
        optional_column("veto"),
    },
)
def __cf_short_name_lc___increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    **kwargs,
) -> ak.Array:
    # collect important information from the results
    unvetoed_mask = ~events.veto if has_ak_column(events, "veto") else Ellipsis
    event_mask = results.event
    n_jets = results.x.n_jets

    # weight map definition
    weight_map = {
        # "num" operations
        "num_events": Ellipsis,  # all events
        "num_events_selected": event_mask,  # selected events only
    }

    if self.dataset_inst.is_mc:
        weight_map["num_negative_weights"] = (events.mc_weight < 0) & \
                                             (True if unvetoed_mask is Ellipsis else unvetoed_mask)
        # "sum" operations
        weight_map["sum_mc_weight"] = (events.mc_weight, unvetoed_mask)  # weights of all events
        weight_map["sum_mc_weight_selected"] = (events.mc_weight, event_mask)  # weights of selected events

        weight_columns = list(
            set(self[event_weights_to_normalize].produced_columns)
        )
        weight_columns = sorted([col.string_nano_column for col in weight_columns])

        # mc weight times correction weight (with variations) without any selection
        for name in weight_columns:
            if "weight" not in name:
                # skip non-weight columns here
                continue

            weight_map[f"sum_mc_weight_{name}"] = (events.mc_weight * events[name], unvetoed_mask)

            # weights for selected events
            weight_map[f"sum_mc_weight_{name}_selected"] = (events.mc_weight * events[name], event_mask)

    group_map = {
        "process": {
            "values": events.process_id,
            "mask_fn": (lambda v: events.process_id == v),
        },
        "njet": {
            "values": results.x.n_jets,
            "mask_fn": (lambda v: n_jets == v),
        },
    }

    group_combinations = [("process", "njet")]

    self[increment_stats](
        events,
        results,
        stats,
        weight_map=weight_map,
        group_map=group_map,
        group_combinations=group_combinations,
        **kwargs,
    )

    return events


@__cf_short_name_lc___increment_stats.init
def __cf_short_name_lc___increment_stats_init(self: Selector) -> None:
    if not getattr(self, "dataset_inst", None):
        return

    if self.dataset_inst.is_mc:
        self.uses |= {"mc_weight"}

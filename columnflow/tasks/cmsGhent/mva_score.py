from __future__ import annotations

import luigi
import law
from collections import OrderedDict

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory, ConfigTask, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, VariablesMixin,
    ChunkedIOMixin, DatasetsProcessesMixin
)

from columnflow.tasks.external import GetDatasetLFNs
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.calibration import CalibrateEvents
from columnflow.tasks.selection import MergeSelectionStats
from columnflow.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents, ReduceEvents
from columnflow.util import dev_sandbox, dict_add_strict, ensure_proxy, safe_div, four_vec
from columnflow.columnar_util import set_ak_column

from columnflow.production import Producer, producer


class CreateMVAHistograms(
    CalibratorsMixin,
    VariablesMixin,
    ChunkedIOMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,

):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        GetDatasetLFNs=GetDatasetLFNs,
        MergeSelectionStats=MergeSelectionStats,
        CalibrateEvents=CalibrateEvents,
    )

    # strategy for handling missing source columns when adding aliases on event chunks
    missing_column_alias_strategy = "original"

    # names of columns that contain category ids
    # (might become a parameter at some point)
    category_id_columns = {"category_ids"}

    @property
    def merging_stats_exist(self):
        return True

    @law.util.classproperty
    def mandatory_columns(cls) -> set[str]:
        return set(cls.category_id_columns) | {"process_id"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store the normalization weight producer for MC
        self.norm_weight_producer = None
        if self.dataset_inst.is_mc:
            self.norm_weight_producer = Producer.get_cls("normalization_weights")(
                inst_dict=self.get_producer_kwargs(self),
            )

        self.lepton_gen_features_producer = Producer.get_cls("lepton_gen_features")(
            inst_dict=self.get_producer_kwargs(self),
        )

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["lfns"] = self.reqs.GetDatasetLFNs.req(self)

        reqs["calibrations"] = [
            self.reqs.CalibrateEvents.req(self, calibrator=calibrator_inst.cls_name)
            for calibrator_inst in self.calibrator_insts
            if calibrator_inst.produced_columns
        ]

        reqs["selection_stats"] = self.reqs.MergeSelectionStats.req(
            self, tree_index=0, branch=-1, _exclude=MergeSelectionStats.exclude_params_forest_merge,)

        if self.dataset_inst.is_mc:
            reqs["normalization"] = self.norm_weight_producer.run_requires()

        reqs["lepton_gen_features"] = self.lepton_gen_features_producer.run_requires()

        return reqs

    def requires(self):
        reqs = {
            "lfns": self.reqs.GetDatasetLFNs.req(self),
            "calibrations": [
                self.reqs.CalibrateEvents.req(self, calibrator=calibrator_inst.cls_name)
                for calibrator_inst in self.calibrator_insts
                if calibrator_inst.produced_columns
            ],
            "selection_stats": self.reqs.MergeSelectionStats.req(
                self, tree_index=0, branch=-1, _exclude=MergeSelectionStats.exclude_params_forest_merge,),
        }

        if self.dataset_inst.is_mc:
            reqs["normalization"] = self.norm_weight_producer.run_requires()
        reqs["lepton_gen_features"] = self.lepton_gen_features_producer.run_requires()

        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return {
            "hists": self.target(f"mvascore_histograms__{self.branch}.pickle"),
        }

    @law.decorator.log
    @law.decorator.localize(input=False)
    @ensure_proxy
    @law.decorator.safe_output
    def run(self):
        import hist
        import numpy as np
        import awkward as ak
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )
        # prepare inputs and outputs
        inputs = self.input()
        reqs = self.requires()
        lfn_task = reqs["lfns"]
        # declare output: dict of histograms
        variable_insts = list(map(self.config_inst.get_variable, self.variables))

        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # setup the normalization weights & jet btag producer
        if self.dataset_inst.is_mc:
            self.norm_weight_producer.run_setup(
                self.requires()["normalization"],
                self.input()["normalization"],
            )
        self.lepton_gen_features_producer.run_setup(
            self.requires()["lepton_gen_features"],
            self.input()["lepton_gen_features"],
        )

        histograms = {}

        h = (
            hist.Hist.new
            .IntCat(
                [0, 1],
                name="isPrompt",
                label="Lepton is Prompt (0 if NonPrompt)",
            )
            .IntCat(
                [11, 13],
                name="abs_pdgId",
                label="Lepton is Electron (11) or Muon (13)",
            )
        )
        for var_inst in variable_insts:
            histograms[var_inst.name] = h.Var(
                var_inst.bin_edges,
                name=var_inst.name,
                label=var_inst.get_full_x_title(),
            ).Weight()

        # define columns that need to be read
        read_columns = {"category_ids", "process_id", "run", "luminosityBlock",
            "event", "genWeight", } | self.lepton_gen_features_producer.used_columns

        # add Electron and Muon mva's to columns that need to be read
        read_columns |= four_vec({"Electron", "Muon"}, {"dxy", "dz", "sip3d", "miniPFRelIso_all"}) | {
            f"Electron.{var_inst.expression}" for var_inst in variable_insts} | {
            "Electron.lostHits", "Electron.deltaEtaSC"} | {
            f"Muon.{var_inst.expression}" for var_inst in variable_insts} | {
            "Muon.mediumId", }

        if self.dataset_inst.is_mc:
            read_columns |= self.norm_weight_producer.used_columns

        read_columns = set(map(Route, read_columns))

        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        with law.localize_file_targets(
            [
                input_file,
                *(inp["columns"] for inp in inputs["calibrations"]),
            ],
            mode="r",
        ) as (local_input_file, *inps):
            with self.publish_step("load and open ..."):
                nano_file = local_input_file.load(formatter="uproot")

            n_calib = len(inputs["calibrations"])
            for (events, *cols), pos in self.iter_chunked_io(
                [nano_file, *(inp.path for inp in inps)],
                source_type=["coffea_root"] + ["awkward_parquet"] * n_calib,
                read_columns=[read_columns] * (1 + n_calib),
            ):
                # optional check for overlapping inputs within additional columns
                if self.check_overlapping_inputs:
                    self.raise_if_overlapping(list(cols))

                # insert additional columns
                events = update_ak_array(events, *cols)

                # add gen features column (interesting column is "Lepton.isPrompt")
                events = self.lepton_gen_features_producer(events)

                # apply basic lepton veto
                # conditions differing for muons and leptons
                ele, mu = events.Electron, events.Muon
                ele_absetaSC = abs(ele.eta + ele.deltaEtaSC)
                masks = {
                    "Electron": (abs(ele.eta) < 2.5) & (ele.lostHits < 2) & ((ele_absetaSC > 1.5560) | (ele_absetaSC < 1.4442)),
                    "Muon": (abs(events.Muon.eta) < 2.4) & events.Muon.mediumId
                }

                # conditions shared for muons and leptons
                for lepton_name in masks:
                    lepton = events[lepton_name]
                    veto_mask = masks[lepton_name] & (
                        # lepton pt requirement for --version low_pt_mva
                        # (lepton.pt > 10) &
                        # (lepton.pt < 25) &
                        # lepton pt requirement for --version high_pt_mva
                        # (lepton.pt > 25) &
                        # inclusive pt requirement
                        (lepton.pt > 10) &
                        (lepton.miniPFRelIso_all < 0.4) &
                        (lepton.sip3d < 8) &
                        (lepton.dz < 0.1) &
                        (lepton.dxy < 0.05)
                    )
                    events = set_ak_column(events, f"{lepton_name}.veto", veto_mask)

                # add normalization weight
                if self.dataset_inst.is_mc:
                    # norm_weight_producer requires mc_weight and not gen_weight
                    events = set_ak_column(events, "mc_weight", events.genWeight)
                    events = self.norm_weight_producer(events)

                # flatten and concatenate all leptons
                lepton = ak.flatten(ak.concatenate([events.Muon, events.Electron], axis=-1))

                # remove the leptons that did not pass veto
                lepton = lepton[lepton.veto]

                # prepare the expression
                for var_inst in variable_insts:
                    fill_kwargs = {
                        "isPrompt": lepton.isPrompt,
                        "abs_pdgId": abs(lepton.pdgId),
                    }

                    # apply it
                    fill_kwargs[var_inst.name] = lepton[var_inst.expression]
                    histograms[var_inst.name].fill(**fill_kwargs)

        self.output()["hists"].dump(histograms, formatter="pickle")


CreateMVAHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateMVAHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)


class MergeMVAHistograms(
    CalibratorsMixin,
    VariablesMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    only_missing = luigi.BoolParameter(
        default=False,
        description="when True, identify missing variables first and only require histograms of "
        "missing ones; default: False",
    )
    remove_previous = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, remove particlar input histograms after merging; default: False",
    )

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        CreateMVAHistograms=CreateMVAHistograms,
    )

    def create_branch_map(self):
        # create a dummy branch map so that this task could be submitted as a job
        return {0: None}

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["hists"] = self.as_branch().requires()

        return reqs

    def requires(self):
        # optional dynamic behavior: determine not yet created variables and require only those

        return self.reqs.CreateMVAHistograms.req(
            self,
            branch=-1,
            _exclude={"branches"},
        )

    def output(self):
        return {"hists": law.SiblingFileCollection({
            variable_name: self.target(f"mvahist__{variable_name}.pickle")
            for variable_name in self.variables
        })}

    @law.decorator.log
    def run(self):
        # preare inputs and outputs
        inputs = self.input()["collection"]
        outputs = self.output()

        # load input histograms
        hists = [
            inp["hists"].load(formatter="pickle")
            for inp in self.iter_progress(inputs.targets.values(), len(inputs), reach=(0, 50))
        ]
        # create a separate file per output variable
        variable_names = list(hists[0].keys())
        for variable_name in self.iter_progress(variable_names, len(variable_names), reach=(50, 100)):
            self.publish_message(f"merging histograms for '{variable_name}'")

            variable_hists = [h[variable_name] for h in hists]
            merged = sum(variable_hists[1:], variable_hists[0].copy())
            outputs["hists"][variable_name].dump(merged, formatter="pickle")

        # optionally remove inputs
        if self.remove_previous:
            inputs.remove()


MergeMVAHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeMVAHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)


class MVAROCCurve(
    CalibratorsMixin,
    VariablesMixin,
    DatasetsProcessesMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeMVAHistograms=MergeMVAHistograms,
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["merged_hists"] = self.requires_from_branch()

        return reqs

    def requires(self):
        return {
            d: self.reqs.MergeMVAHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
            )
            for d in self.datasets
        }

    def create_branch_map(self):
        # create a dummy branch map so that this task could be submitted as a job
        return {0: None}

    def output(self):
        return {"plots": {lepton: self.target(f"{self.datasets_repr}/mva{lepton}__{'_'.join(self.variables)}.pdf") for lepton in ("Electron", "Muon")}}

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params

    @law.decorator.log
    def run(self):
        import hist
        import numpy as np
        import awkward as ak
        import matplotlib
        import matplotlib.pyplot as plt
        import mplhep
        variable_insts = list(map(self.config_inst.get_variable, self.variables))
        colors = ["#3f90da", "#ffa90e", "#bd1f01", "#94a4a2", "#832db6",
            "#a96b59", "#e76300", "#b9ac70", "#717581", "#92dadd"]

        # read in and combine all histograms
        hists = {}
        for var_inst in variable_insts:

            for i, (dataset, inp) in enumerate(self.input().items()):
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0]["hists"].targets[var_inst.name].load(formatter="pickle")

                h = h_in.copy()

                if var_inst not in hists.keys():
                    hists[var_inst] = h
                else:
                    hists[var_inst] += h

        for abs_pdgId, lepton in ((11, "Electron"), (13, "Muon")):
            # initate figure (seperate for Electron and Muon)
            plt.style.use(mplhep.style.CMS)
            fig, ax = plt.subplots()
            color_counter = 0

            for var_inst in variable_insts:
                hist_var = hists[var_inst]

                FPR = np.zeros(len(var_inst.bin_edges))
                TPR = np.zeros(len(var_inst.bin_edges))

                # go over all possible mva cuts
                for i, mva_wp in enumerate(var_inst.bin_edges):
                    FPR[i] = hist_var[{var_inst.name: slice(i, None, sum), "isPrompt": 0j, "abs_pdgId": hist.loc(abs_pdgId)}].value / \
                        hist_var[{var_inst.name: sum, "isPrompt": 0j,
                            "abs_pdgId": hist.loc(abs_pdgId)}].value
                    TPR[i] = hist_var[{var_inst.name: slice(i, None, sum), "isPrompt": 1j, "abs_pdgId": hist.loc(abs_pdgId)}].value / \
                        hist_var[{var_inst.name: sum, "isPrompt": 1j,
                            "abs_pdgId": hist.loc(abs_pdgId)}].value

                # plot 0.9 true positive rate on the ROC-curve
                idx = (np.abs(TPR - 0.9)).argmin()
                ax.scatter(
                    [FPR[idx]],
                    [TPR[idx]],
                    color=colors[color_counter],
                )

                ax.plot(
                    FPR,
                    TPR,
                    label=var_inst.x_title,
                    color=colors[color_counter],
                )
                color_counter += 1

            ax.set_xlabel("False Positive Rate")
            ax.set_xlim(10**(-3), 1)
            ax.set_ylabel("True Positive Rate")
            ax.set_ylim(0.7, 1)
            ax.set_xscale("log")
            legend_kwargs = {
                "title": f"{lepton} ID",
                "ncol": 1,
                "loc": "lower right",
                "fontsize": 16,
            }
            ax.grid()
            ax.legend(**legend_kwargs)
            mplhep.cms.label(", ROC-curve", data=False, year=int(self.config_inst.x.year))
            self.output()["plots"][lepton].dump(fig, formatter="mpl")

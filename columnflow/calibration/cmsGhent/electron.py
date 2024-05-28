# coding: utf-8

"""
electron energy scale And smearing.
"""

import functools

from columnflow.types import Any
from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import ak_random
from columnflow.production.util import attach_coffea_behavior
from columnflow.util import maybe_import, InsertableDict, DotDict
from columnflow.columnar_util import set_ak_column, layout_ak_array, optional_column as optional

np = maybe_import("numpy")
ak = maybe_import("awkward")
correctionlib = maybe_import("correctionlib")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


def get_evaluators(
    correction_set: correctionlib.highlevel.CorrectionSet,
    names: list[str],
) -> list[Any]:
    """
    Helper function to get a list of correction evaluators from a
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet` object given
    a list of *names*. The *names* can refer to either simple or compound
    corrections.

    :param correction_set: evaluator provided by :external+correctionlib:doc:`index`
    :param names: List of names of corrections to be applied
    :raises RuntimeError: If a requested correction in *names* is not available
    :return: List of compounded corrections, see
        :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    """
    # raise nice error if keys not found
    available_keys = set(correction_set.keys()).union(correction_set.compound.keys())
    missing_keys = set(names) - available_keys
    if missing_keys:
        raise RuntimeError("corrections not found:" + "".join(
            f"\n  - {name}" for name in names if name in missing_keys
        ) + "\navailable:" + "".join(
            f"\n  - {name}" for name in sorted(available_keys)
        ))

    # retrieve the evaluators
    return [
        correction_set.compound[name]
        if name in correction_set.compound
        else correction_set[name]
        for name in names
    ]


def ak_evaluate(evaluator: correctionlib.highlevel.Correction, *args) -> float:
    """
    Evaluate a :external+correctionlib:py:class:`correctionlib.highlevel.Correction`
    using one or more :external+ak:py:class:`awkward arrays <ak.Array>` as inputs.

    :param evaluator: Evaluator instance
    :raises ValueError: If no :external+ak:py:class:`awkward arrays <ak.Array>` are provided
    :return: The correction factor derived from the input arrays
    """
    # fail if no arguments
    if not args:
        raise ValueError("Expected at least one argument.")

    # collect arguments that are awkward arrays
    ak_args = [
        arg for arg in args if isinstance(arg, ak.Array)
    ]

    # broadcast akward arrays together and flatten
    if ak_args:
        bc_args = ak.broadcast_arrays(*ak_args)
        flat_args = (
            np.asarray(ak.flatten(bc_arg, axis=None))
            for bc_arg in bc_args
        )
        output_layout_array = bc_args[0]
    else:
        flat_args = iter(())
        output_layout_array = None

    # multiplex flattened and non-awkward inputs
    all_flat_args = [
        next(flat_args) if isinstance(arg, ak.Array) else arg
        for arg in args
    ]

    # apply evaluator to flattened/multiplexed inputs
    result = evaluator.evaluate(*all_flat_args)

    # apply broadcasted layout to result
    if output_layout_array is not None:
        result = layout_ak_array(result, output_layout_array)

    return result


@calibrator(
    uses={
        "run",
        "Electron.pt", "Electron.eta", "Electron.deltaEtaSC", "Electron.r9", "Electron.seedGain",
        attach_coffea_behavior,
    },
    produces={
        "Electron.pt",
    },
    # toggle for scaling and smearing photon energy TODO
    photon=False,
    uncertainty_sources=None,
    # function to determine the correction file for electrons
    get_electron_file=(lambda self, external_files: external_files.electron_ss),
    # function to determine the correction file for photons
    get_photon_file=(lambda self, external_files: external_files.photon_ss),

    # function to determine the electron ss config
    get_electron_ss_cfg=(lambda self: self.config_inst.x.electron_ss),
)
def electron_ss(
    self: Calibrator,
    events: ak.Array,
    **kwargs,
) -> ak.Array:
    """
    Performs the electron (photon) energy scale corrections on data & smearing + uncertainties on Monte Carlo

    Requires an external file in the config under ``electron_ss`` and photon_ss if photon is True:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "electron_ss": "/eos/cms/store/group/phys_egamma/akapoor/S+SJSON/2022Re-recoBCD/electronSS.json.gz",
            "photon_ss": "/eos/cms/store/group/phys_egamma/akapoor/S+SJSON/2022Re-recoBCD/photonSS.json.gz",
        })

    The electron_ss configuration should be an auxiliary entry in the config, specifying the correction
    details under "electron_ss":

    .. code-block:: python

        cfg.x.electron_ss = {
            "campaign": "2022Re-recoE+PromptFG",
            "version": 2,
            "uncertainty_sources": ["scale","smearing"]
        }

    This instance of :py:class:`~columnflow.calibration.Calibrator` is
    initialized with the following parameters by default:

    :param events: awkward array containing events to process

    :param photon_: If true also scale/smear the photon energy
    """  # noqa
    rand_gen = np.random.Generator(np.random.SFC64(events.event.to_list()))

    # simple function to handle valtype of both scale and smearing
    def valtype(source, unc_dir):

        if source == "scale":
            if unc_dir == "nominal":
                return "total_correction"
            else:
                return "total_uncertainty"
        elif source == "smearing":
            if unc_dir == "nominal":
                return "rho"
            else:
                return "err_rho"

    def correction_electrons(gain, run, eta, r9, pt, source, unc_dir="nominal"):
        # variable naming convention
        variable_map = {
            "gain": gain,
            "run": run,
            "eta": eta,
            "r9": r9,
            "et": pt,
            "valtype": valtype(source, unc_dir),
        }

        # apply correction
        corrector = self.evaluators[source]

        # determine correct inputs (change depending on corrector)
        inputs = [
            variable_map[inp.name]
            for inp in corrector.inputs
        ]

        # is (err_)rho for smearing evaluator
        correction = ak_evaluate(corrector, *inputs)

        # smearing formula is different
        if source == "smearing":

            if unc_dir == "nominal":
                rho = correction
                correction = ak_random(1, rho, rand_func=rand_gen.normal)
            else:
                err_rho = correction
                variable_map["valtype"] = "rho"
                inputs = [
                    variable_map[inp.name]
                    for inp in corrector.inputs
                ]
                rho = ak_evaluate(corrector, *inputs)
                if unc_dir == "up":
                    correction = ak_random(1, rho + err_rho, rand_func=rand_gen.normal)
                elif unc_dir == "down":
                    correction = ak_random(1, rho - err_rho, rand_func=rand_gen.normal)
        else:
            if unc_dir == "up":
                correction = 1 + correction
            elif unc_dir == "down":
                correction = 1 - correction
        return correction

    # nominal correction
    nominal_correction = correction_electrons(
        gain=events.Electron.seedGain,
        run=events.run,
        eta=events.Electron.eta + events.Electron.deltaEtaSC,
        r9=events.Electron.r9,
        pt=events.Electron.pt,
        source="scale" if self.dataset_inst.is_data else "smearing",
    )

    # apply nominal correction
    events = set_ak_column_f32(events, "Electron.pt", events.Electron.pt * nominal_correction)

    # apply all uncertainties in sources (only Monte Carlo)
    sources = self.uncertainty_sources
    if sources is None:
        sources = self.get_electron_ss_cfg().uncertainty_sources

    for unc_name in sources:
        for unc_dir in ("up", "down"):
            unc_correction = correction_electrons(
                gain=events.Electron.seedGain,
                run=events.run,
                eta=events.Electron.eta + events.Electron.deltaEtaSC,
                r9=events.Electron.r9,
                pt=events.Electron.pt,
                source=unc_name,
                unc_dir=unc_dir,
            )
            events = set_ak_column_f32(
                events, f"Electron.pt_electron_{unc_name}_{unc_dir}", events.Electron.pt * unc_correction)

    return events


@electron_ss.requires
def electron_ss_requires(self: Calibrator, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@electron_ss.init
def electron_ss_init(self: Calibrator) -> None:
    electron_ss_cfg = self.get_electron_ss_cfg()

    # if Monte Carlo add uncertainty_sources to self.produces
    sources = self.uncertainty_sources
    if sources is None:
        sources = electron_ss_cfg.uncertainty_sources

    if self.dataset_inst.is_mc:

        self.produces |= {
            f"Electron.pt_electron_{unc_name}_{unc_dir}"
            for unc_name in sources
            for unc_dir in ("up", "down")
        }


@electron_ss.setup
def electron_ss_setup(self: Calibrator, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    """
    Load the correct electron_ss files using the :py:func:`from_string` method of the
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet` function and apply the
    corrections as needed.


    :param reqs: Requirement dictionary for this :py:class:`~columnflow.calibration.Calibrator`
        instance.
    :param inputs: Additional inputs, currently not used.
    :param reader_targets: TODO: add documentation.
    """

    electron_ss_cfg = self.get_electron_ss_cfg()

    # if Monte Carlo add uncertainty_sources to self.produces
    sources = self.uncertainty_sources
    if sources is None:
        sources = electron_ss_cfg.uncertainty_sources

    bundle = reqs["external_files"]

    # import the correction sets from the external file
    import correctionlib
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_electron_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )

    # store all required evaluators
    evaluators = {}

    evaluators["scale"] = "Scale"
    if self.dataset_inst.is_mc:
        evaluators["smearing"] = "Smearing"

    self.evaluators = {
        name: get_evaluators(correction_set, [key])[0]
        for name, key in evaluators.items()
    }


# custom electron_ss calibrator that only runs nominal correction
electron_ss_nominal = electron_ss.derive("electron_ss_nominal", cls_dict={"uncertainty_sources": []})

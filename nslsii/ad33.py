"""Classes to help with supporting AreaDetector 33 (and the
wait_for_plugins functionality)


This is actually adding a mix of functionality from AD 2-2 to 3-3 and
all of these names may change in the future.

"""

from ophyd import Device, Component as Cpt
from ophyd.areadetector.base import ADBase, ADComponent as ADCpt, ad_group, EpicsSignalWithRBV as SignalWithRBV
from ophyd.areadetector.plugins import PluginBase
from ophyd.areadetector.trigger_mixins import TriggerBase, ADTriggerStatus
from ophyd.device import DynamicDeviceComponent as DDC, Staged
from ophyd.signal import Signal, EpicsSignalRO, EpicsSignal
from ophyd.quadem import QuadEM

import time as ttime


class V22Mixin(Device): ...


class V26Mixin(V22Mixin):
    adcore_version = Cpt(EpicsSignalRO, "ADCoreVersion_RBV", string=True, kind="config")
    driver_version = Cpt(EpicsSignalRO, "DriverVersion_RBV", string=True, kind="config")


class V33Mixin(V26Mixin): ...


class CamV33Mixin(V33Mixin):
    wait_for_plugins = Cpt(EpicsSignal, "WaitForPlugins", string=True, kind="config")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_sigs["wait_for_plugins"] = "Yes"

    def ensure_nonblocking(self):
        self.stage_sigs["wait_for_plugins"] = "Yes"
        for c in self.parent.component_names:
            cpt = getattr(self.parent, c)
            if cpt is self:
                continue
            if hasattr(cpt, "ensure_nonblocking"):
                cpt.ensure_nonblocking()


class FilePluginV22Mixin(V22Mixin):
    create_directories = Cpt(EpicsSignal, "CreateDirectory", kind="config")


class SingleTriggerV33(TriggerBase):
    _status_type = ADTriggerStatus

    def __init__(self, *args, image_name=None, **kwargs):
        super().__init__(*args, **kwargs)
        if image_name is None:
            image_name = "_".join([self.name, "image"])
        self._image_name = image_name

    def trigger(self):
        "Trigger one acquisition."
        if self._staged != Staged.yes:
            raise RuntimeError("This detector is not ready to trigger.Call the stage() method before triggering.")

        self._status = self._status_type(self)

        def _acq_done(*args, **kwargs):
            # TODO sort out if anything useful in here
            self._status._finished()

        self._acquisition_signal.put(1, use_complete=True, callback=_acq_done)
        self.generate_datum(self._image_name, ttime.time(), {})
        return self._status


class StatsPluginV33(PluginBase):
    """This supports changes to time series PV names in AD 3-3

    Due to https://github.com/areaDetector/ADCore/pull/333
    """

    _default_suffix = "Stats1:"
    _suffix_re = r"Stats\d:"
    _html_docs = ["NDPluginStats.html"]
    _plugin_type = "NDPluginStats"

    _default_configuration_attrs = PluginBase._default_configuration_attrs + (
        "centroid_threshold",
        "compute_centroid",
        "compute_histogram",
        "compute_profiles",
        "compute_statistics",
        "bgd_width",
        "hist_size",
        "hist_min",
        "hist_max",
        "ts_num_points",
        "profile_size",
        "profile_cursor",
    )

    bgd_width = ADCpt(SignalWithRBV, "BgdWidth")
    centroid_threshold = ADCpt(SignalWithRBV, "CentroidThreshold")

    centroid = DDC(
        ad_group(EpicsSignalRO, (("x", "CentroidX_RBV"), ("y", "CentroidY_RBV"))),
        doc="The centroid XY",
        default_read_attrs=("x", "y"),
    )

    compute_centroid = ADCpt(SignalWithRBV, "ComputeCentroid", string=True)
    compute_histogram = ADCpt(SignalWithRBV, "ComputeHistogram", string=True)
    compute_profiles = ADCpt(SignalWithRBV, "ComputeProfiles", string=True)
    compute_statistics = ADCpt(SignalWithRBV, "ComputeStatistics", string=True)

    cursor = DDC(
        ad_group(SignalWithRBV, (("x", "CursorX"), ("y", "CursorY"))),
        doc="The cursor XY",
        default_read_attrs=("x", "y"),
    )

    hist_entropy = ADCpt(EpicsSignalRO, "HistEntropy_RBV")
    hist_max = ADCpt(SignalWithRBV, "HistMax")
    hist_min = ADCpt(SignalWithRBV, "HistMin")
    hist_size = ADCpt(SignalWithRBV, "HistSize")
    histogram = ADCpt(EpicsSignalRO, "Histogram_RBV")

    max_size = DDC(
        ad_group(EpicsSignal, (("x", "MaxSizeX"), ("y", "MaxSizeY"))),
        doc="The maximum size in XY",
        default_read_attrs=("x", "y"),
    )

    max_value = ADCpt(EpicsSignalRO, "MaxValue_RBV")
    max_xy = DDC(
        ad_group(EpicsSignalRO, (("x", "MaxX_RBV"), ("y", "MaxY_RBV"))),
        doc="Maximum in XY",
        default_read_attrs=("x", "y"),
    )

    mean_value = ADCpt(EpicsSignalRO, "MeanValue_RBV")
    min_value = ADCpt(EpicsSignalRO, "MinValue_RBV")

    min_xy = DDC(
        ad_group(EpicsSignalRO, (("x", "MinX_RBV"), ("y", "MinY_RBV"))),
        doc="Minimum in XY",
        default_read_attrs=("x", "y"),
    )

    net = ADCpt(EpicsSignalRO, "Net_RBV")
    profile_average = DDC(
        ad_group(EpicsSignalRO, (("x", "ProfileAverageX_RBV"), ("y", "ProfileAverageY_RBV"))),
        doc="Profile average in XY",
        default_read_attrs=("x", "y"),
    )

    profile_centroid = DDC(
        ad_group(EpicsSignalRO, (("x", "ProfileCentroidX_RBV"), ("y", "ProfileCentroidY_RBV"))),
        doc="Profile centroid in XY",
        default_read_attrs=("x", "y"),
    )

    profile_cursor = DDC(
        ad_group(EpicsSignalRO, (("x", "ProfileCursorX_RBV"), ("y", "ProfileCursorY_RBV"))),
        doc="Profile cursor in XY",
        default_read_attrs=("x", "y"),
    )

    profile_size = DDC(
        ad_group(EpicsSignalRO, (("x", "ProfileSizeX_RBV"), ("y", "ProfileSizeY_RBV"))),
        doc="Profile size in XY",
        default_read_attrs=("x", "y"),
    )

    profile_threshold = DDC(
        ad_group(EpicsSignalRO, (("x", "ProfileThresholdX_RBV"), ("y", "ProfileThresholdY_RBV"))),
        doc="Profile threshold in XY",
        default_read_attrs=("x", "y"),
    )

    set_xhopr = ADCpt(EpicsSignal, "SetXHOPR")
    set_yhopr = ADCpt(EpicsSignal, "SetYHOPR")
    sigma_xy = ADCpt(EpicsSignalRO, "SigmaXY_RBV")
    sigma_x = ADCpt(EpicsSignalRO, "SigmaX_RBV")
    sigma_y = ADCpt(EpicsSignalRO, "SigmaY_RBV")
    sigma = ADCpt(EpicsSignalRO, "Sigma_RBV")
    ts_acquiring = ADCpt(EpicsSignal, "TS:TSAcquiring")

    ts_centroid = DDC(
        ad_group(EpicsSignal, (("x", "TS:TSCentroidX"), ("y", "TS:TSCentroidY"))),
        doc="Time series centroid in XY",
        default_read_attrs=("x", "y"),
    )

    # ts_control = ADCpt(EpicsSignal, 'TS:TSControl', string=True)
    ts_current_point = ADCpt(EpicsSignal, "TS:TSCurrentPoint")
    ts_max_value = ADCpt(EpicsSignal, "TS:TSMaxValue")

    ts_max = DDC(
        ad_group(EpicsSignal, (("x", "TS:TSMaxX"), ("y", "TS:TSMaxY"))),
        doc="Time series maximum in XY",
        default_read_attrs=("x", "y"),
    )

    ts_mean_value = ADCpt(EpicsSignal, "TS:TSMeanValue")
    ts_min_value = ADCpt(EpicsSignal, "TS:TSMinValue")

    ts_min = DDC(
        ad_group(EpicsSignal, (("x", "TS:TSMinX"), ("y", "TS:TSMinY"))),
        doc="Time series minimum in XY",
        default_read_attrs=("x", "y"),
    )

    ts_net = ADCpt(EpicsSignal, "TS:TSNet")
    ts_num_points = ADCpt(EpicsSignal, "TS:TSNumPoints")
    ts_read = ADCpt(EpicsSignal, "TS:TSRead")
    ts_sigma = ADCpt(EpicsSignal, "TS:TSSigma")
    ts_sigma_x = ADCpt(EpicsSignal, "TS:TSSigmaX")
    ts_sigma_xy = ADCpt(EpicsSignal, "TS:TSSigmaXY")
    ts_sigma_y = ADCpt(EpicsSignal, "TS:TSSigmaY")
    ts_total = ADCpt(EpicsSignal, "TS:TSTotal")
    total = ADCpt(EpicsSignalRO, "Total_RBV")


class QuadEMPort(ADBase):
    port_name = Cpt(Signal, value="")

    def __init__(self, port_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.port_name.put(port_name)


class QuadEMV33(QuadEM):
    conf = Cpt(QuadEMPort, port_name="EM180")
    em_range = Cpt(SignalWithRBV, "Range", string=True)

    current1 = ADCpt(StatsPluginV33, "Current1:")
    current2 = ADCpt(StatsPluginV33, "Current2:")
    current3 = ADCpt(StatsPluginV33, "Current3:")
    current4 = ADCpt(StatsPluginV33, "Current4:")
    sum_all = ADCpt(StatsPluginV33, "SumAll:")

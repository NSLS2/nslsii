from ophyd import Device, Component as Cpt
from ophyd import EpicsSignal, EpicsSignalRO
from ophyd.areadetector import EpicsSignalWithRBV as SignalWithRBV
from ophyd.status import SubscriptionStatus


class QEProTEC(Device):
    # Thermal electric cooler settings
    tec = Cpt(SignalWithRBV, "TEC")
    tec_temp = Cpt(SignalWithRBV, "TEC_TEMP")
    curr_tec_temp = Cpt(EpicsSignalRO, "CURR_TEC_TEMP_RBV")

    def __init__(self, *args, tolerance=1, **kwargs):
        self.tolerance = tolerance
        super().__init__(*args, **kwargs)

    def set(self, value):

        def check_setpoint(value, old_value, **kwargs):
            if abs(value - self.tec_temp.get()) < self.tolerance:
                print(f"Reached setpoint {self.tec_temp.get()}.")
                return True
            return False

        status = SubscriptionStatus(self.curr_tec_temp, run=False, callback=check_setpoint)
        self.tec_temp.put(value)
        self.tec.put(1)

        return status


class QEPro(Device):
    # Device information
    serial = Cpt(EpicsSignal, "SERIAL")
    model = Cpt(EpicsSignal, "MODEL")

    # Device Status
    status = Cpt(EpicsSignal, "STATUS")
    status_msg = Cpt(EpicsSignal, "STATUS_MSG")
    device_connected = Cpt(EpicsSignalRO, "CONNECTED_RBV")

    # Utility signal that periodically checks device temps.
    __check_status = Cpt(EpicsSignal, "CHECK_STATUS")

    # Bit array outlining which features are supported by the device
    features = Cpt(EpicsSignalRO, "FEATURES_RBV")

    # Togglable features (if supported)
    strobe = Cpt(SignalWithRBV, "STROBE")
    electric_dark_correction = Cpt(SignalWithRBV, "EDC")
    non_linearity_correction = Cpt(SignalWithRBV, "NLC")
    shutter = Cpt(SignalWithRBV, "SHUTTER")

    # Thermal electric cooler
    tec_device = Cpt(QEProTEC, "")

    # Light source feature signals
    light_source = Cpt(SignalWithRBV, "LIGHT_SOURCE")
    light_source_intensity = Cpt(SignalWithRBV, "LIGHT_SOURCE_INTENSITY")
    light_source_count = Cpt(EpicsSignalRO, "LIGHT_SOURCE_COUNT_RBV")

    # Signals for specifying the number of spectra to average and counter for spectra
    # collected in current scan
    num_spectra = Cpt(SignalWithRBV, "NUM_SPECTRA")
    spectra_collected = Cpt(EpicsSignalRO, "SPECTRA_COLLECTED_RBV")

    # Integration time settings (in ms)
    int_min_time = Cpt(EpicsSignalRO, "INT_MIN_TIME_RBV")
    int_max_time = Cpt(EpicsSignalRO, "INT_MAX_TIME_RBV")
    integration_time = Cpt(SignalWithRBV, "INTEGRATION_TIME", kind="normal")

    # Internal buffer feature settings
    buff_min_capacity = Cpt(EpicsSignalRO, "BUFF_MIN_CAPACITY_RBV")
    buff_max_capacity = Cpt(EpicsSignalRO, "BUFF_MAX_CAPACITY_RBV")
    buff_capacity = Cpt(SignalWithRBV, "BUFF_CAPACITY")
    buff_element_count = Cpt(EpicsSignalRO, "BUFF_ELEMENT_COUNT_RBV")

    # Formatted Spectra
    spectrum = Cpt(EpicsSignal, "SPECTRUM", kind="normal")
    dark = Cpt(EpicsSignal, "DARK", kind="normal")
    reference = Cpt(EpicsSignal, "REFERENCE", kind="normal")

    # Length of spectrum (in pixels)
    formatted_spectrum_len = Cpt(EpicsSignalRO, "FORMATTED_SPECTRUM_LEN_RBV")

    # X-axis format and array
    x_axis = Cpt(EpicsSignal, "X_AXIS")
    x_axis_format = Cpt(SignalWithRBV, "X_AXIS_FORMAT")

    # Dark/Ref available signals
    dark_available = Cpt(EpicsSignalRO, "DARK_AVAILABLE_RBV")
    ref_available = Cpt(EpicsSignalRO, "REF_AVAILABLE_RBV")

    # Collection settings and start signals.
    acquire = Cpt(SignalWithRBV, "COLLECT", put_complete=True)
    collect_mode = Cpt(SignalWithRBV, "COLLECT_MODE")
    spectrum_type = Cpt(SignalWithRBV, "SPECTRUM_TYPE")
    correction = Cpt(SignalWithRBV, "CORRECTION")
    trigger_mode = Cpt(SignalWithRBV, "TRIGGER_MODE")

    @property
    def has_nlc_feature(self):
        return self.features.get() & 32

    @property
    def has_lightsource_feature(self):
        return self.features.get() & 16

    @property
    def has_edc_feature(self):
        return self.features.get() & 8

    @property
    def has_buffer_feature(self):
        return self.features.get() & 4

    @property
    def has_tec_feature(self):
        return self.features.get() & 2

    @property
    def has_irrad_feature(self):
        return self.features.get() & 1

    def set_temp(self, temperature):
        self.tec_device.set(temperature).wait()

    def get_dark_frame(self):

        self.spectrum_type.put(1)
        self.acquire.put(1, wait=True)

    def get_reference_frame(self):

        if self.dark_available.get() == 0:
            return
        self.spectrum_type.put(2)
        self.acquire.put(1, wait=True)

    def setup_collection(
        self, integration_time, num_spectra_to_average, correction_type="reference", electric_dark_correction=True
    ):
        self.integration_time.put(integration_time)
        self.num_spectra = num_spectra_to_average
        if electric_dark_correction:
            self.electric_dark_correction = True

        self.get_dark_frame()
        self.get_reference_frame()

    def trigger(self):

        self.acquire.put(1, wait=True)

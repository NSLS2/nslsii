import time
import datetime
from ophyd import (Device, Component as Cpt,
                   EpicsSignal, EpicsSignalRO, DeviceStatus)

_time_fmtstr = '%Y-%m-%d %H:%M:%S'


class TwoButtonShutter(Device):
    # TODO: this needs to be fixed in EPICS as these names make no sense
    # the value coming out of the PV does not match what is shown in CSS
    RETRY_PERIOD = 0.5
    MAX_ATTEMPTS = 10
    open_cmd = Cpt(EpicsSignal, 'Cmd:Opn-Cmd', string=True)
    open_val = 'Open'

    close_cmd = Cpt(EpicsSignal, 'Cmd:Cls-Cmd', string=True)
    close_val = 'Not Open'

    status = Cpt(EpicsSignalRO, 'Pos-Sts', string=True)
    fail_to_close = Cpt(EpicsSignalRO, 'Sts:FailCls-Sts', string=True)
    fail_to_open = Cpt(EpicsSignalRO, 'Sts:FailOpn-Sts', string=True)
    enabled_status = Cpt(EpicsSignalRO, 'Enbl-Sts', string=True)

    # user facing commands
    open_str = 'Open'
    close_str = 'Close'

    def set(self, val):
        if self._set_st is not None:
            raise RuntimeError(f'trying to set {self.name}'
                               ' while a set is in progress')

        cmd_map = {self.open_str: self.open_cmd,
                   self.close_str: self.close_cmd}
        target_map = {self.open_str: self.open_val,
                      self.close_str: self.close_val}

        cmd_sig = cmd_map[val]
        target_val = target_map[val]

        st = DeviceStatus(self)
        if self.status.get() == target_val:
            st._finished()
            return st

        self._set_st = st
        print(self.name, val, id(st))
        enums = self.status.enum_strs

        def shutter_cb(value, timestamp, **kwargs):
            try:
                value = enums[int(value)]
            except (ValueError, TypeError):
                # we are here because value is a str not int
                # just move on
                ...
            if value == target_val:
                self._set_st = None
                self.status.clear_sub(shutter_cb)
                st._finished()

        cmd_enums = cmd_sig.enum_strs
        count = 0

        def cmd_retry_cb(value, timestamp, **kwargs):
            nonlocal count
            try:
                value = cmd_enums[int(value)]
            except (ValueError, TypeError):
                # we are here because value is a str not int
                # just move on
                ...
            count += 1
            if count > self.MAX_ATTEMPTS:
                cmd_sig.clear_sub(cmd_retry_cb)
                self._set_st = None
                self.status.clear_sub(shutter_cb)
                st._finished(success=False)
            if value == 'None':
                if not st.done:
                    time.sleep(self.RETRY_PERIOD)
                    cmd_sig.set(1)

                    ts = datetime.datetime.fromtimestamp(timestamp) \
                        .strftime(_time_fmtstr)
                    if count > 2:
                        msg = '** ({}) Had to reactuate shutter while {}ing'
                        print(msg.format(ts, val if val != 'Close'
                                         else val[:-1]))
                else:
                    cmd_sig.clear_sub(cmd_retry_cb)

        cmd_sig.subscribe(cmd_retry_cb, run=False)
        self.status.subscribe(shutter_cb)
        cmd_sig.set(1)

        return st

    def pause(self):
        import time
        prev_st = self._set_st
        if prev_st is not None:
            while not prev_st.done:
                time.sleep(.1)
        self._was_open = (self.open_val == self.status.get())
        st = self.set('Close')
        while not st.done:
            time.sleep(.5)

    def resume(self):
        import time
        prev_st = self._set_st
        if prev_st is not None:
            while not prev_st.done:
                time.sleep(.1)
        if self._was_open:
            st = self.set('Open')
            while not st.done:
                time.sleep(.5)

    def unstage(self):
        self._was_open = False
        return super().unstage()

    def __init__(self, *args, close_on_pause=True, close_on_stop=True, **kwargs):
        self._was_open = False
        super().__init__(*args, **kwargs)
        self._set_st = None
        self.read_attrs = ['status']

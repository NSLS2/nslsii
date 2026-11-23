from collections import defaultdict

import bluesky.plans as bp
import pytest
from bluesky.run_engine import call_in_bluesky_event_loop
from ophyd_async.core import get_mock_put, init_devices, set_mock_value

from nslsii.ophyd_async.devices import BeckhoffGPIO, BeckhoffTerminalModel
from nslsii.ophyd_async.devices.beckhoff_gpio import _TERMINAL_MODELS

IOC_CONFIG = {
    "type": "elio",
    "terminals": [
        {"POS": 1, "MODEL": "EL3068"},
        {"POS": 2, "MODEL": "EL4008"},
        {"POS": 3, "MODEL": "EL1808"},
        {"POS": 5, "MODEL": "EL2798"},
        {"POS": 6, "MODEL": "EL3314"},
        {"POS": 7, "MODEL": "EL9505"},
    ],
    "environment": {"SYS": "XF:31ID1-ES", "DEV": "GPIO:1"},
}


async def make_gpio() -> BeckhoffGPIO:
    async with init_devices(mock=True):
        gpio = BeckhoffGPIO.from_ioc_config(IOC_CONFIG)
    return gpio


@pytest.mark.asyncio
async def test_pv_names_follow_elio_standard():
    gpio = await make_gpio()
    assert set(gpio.terminals) == {1, 2, 3, 5, 6, 7}
    assert len(gpio.terminals[7].channels) == 2
    expected = {
        gpio.terminals[1].channels[8].value: "XF:31ID1-ES{GPIO:1_1}AI:Ch8-I",
        gpio.terminals[2].channels[1].value: "XF:31ID1-ES{GPIO:1_2}AO:Ch1-SP",
        gpio.terminals[3].channels[2].value: "XF:31ID1-ES{GPIO:1_3}DI:Ch2-Sts",
        gpio.terminals[5].channels[3].value: "XF:31ID1-ES{GPIO:1_5}Rly:Ch3-Cmd",
        gpio.terminals[6].channels[4].value: "XF:31ID1-ES{GPIO:1_6}T:Ch4-I",
        gpio.terminals[1].channels[1].description: "XF:31ID1-ES{GPIO:1_1}AI:Ch1-I.DESC",
        gpio.coupler.ebus_status: "XF:31ID1-ES{GPIO:1}:EbusStatus",
        gpio.coupler.watchdog_time: "XF:31ID1-ES{GPIO:1}:WatchdogExpTime",
    }
    for signal, pv in expected.items():
        assert signal.source == f"mock+ca://{pv}"


@pytest.mark.asyncio
async def test_read_and_set():
    gpio = await make_gpio()
    set_mock_value(gpio.terminals[1].channels[2].value, 4.5)
    set_mock_value(gpio.terminals[3].channels[1].value, True)
    reading = await gpio.read()
    assert reading["gpio-terminals-1-channels-2"]["value"] == 4.5
    assert reading["gpio-terminals-3-channels-1"]["value"] is True

    await gpio.terminals[2].channels[1].set(2.5)
    get_mock_put(gpio.terminals[2].channels[1].value).assert_called_once_with(2.5)
    await gpio.terminals[5].channels[1].set(True)
    assert await gpio.terminals[5].channels[1].value.get_value() is True

    config = await gpio.read_configuration()
    assert "gpio-coupler-serial_number" in config
    assert "gpio-coupler-watchdog_time" in config
    assert "gpio-coupler-ebus_status" not in config
    assert not any(key.startswith("gpio-coupler") for key in reading)


def test_mapping_terminals_and_invalid_model():
    gpio = BeckhoffGPIO("XF:TEST", "GPIO:2", {4: "EL1124"}, name="gpio")
    assert gpio.terminals[4].channels[4].value.source == "ca://XF:TEST{GPIO:2_4}DI:Ch4-Sts"
    watchdog_backend = gpio.coupler.watchdog_time._connector.backend
    assert watchdog_backend.read_pv == "XF:TEST{GPIO:2}:WatchdogExpTime"
    assert watchdog_backend.write_pv == "XF:TEST{GPIO:2}:WatchdogTime"
    with pytest.raises(ValueError, match="Unsupported Beckhoff terminal model"):
        BeckhoffGPIO("XF:TEST", "GPIO:2", {1: "EL9999"})


def test_channel_names_show_up_in_documents(RE):
    async def connect_and_rename():
        async with init_devices(mock=True):
            gpio = BeckhoffGPIO.from_ioc_config(IOC_CONFIG)
        gpio.terminals[1].channels[2].set_name("sample_temp")
        gpio.terminals[3].channels[1].set_name("shutter_open")
        set_mock_value(gpio.terminals[1].channels[2].value, 4.5)
        set_mock_value(gpio.terminals[3].channels[1].value, True)
        return gpio

    gpio = call_in_bluesky_event_loop(connect_and_rename())
    docs = defaultdict(list)
    RE(bp.count([gpio]), lambda name, doc: docs[name].append(doc))

    descriptor = docs["descriptor"][0]
    event = docs["event"][0]
    assert "sample_temp" in descriptor["data_keys"]
    assert "shutter_open" in descriptor["data_keys"]
    assert "gpio-terminals-1-channels-2" not in descriptor["data_keys"]
    assert event["data"]["sample_temp"] == 4.5
    assert event["data"]["shutter_open"] is True
    assert "sample_temp" in descriptor["hints"]["gpio"]["fields"]
    assert "sample_temp-description" in descriptor["configuration"]["gpio"]["data_keys"]


def test_every_terminal_model_has_a_channel_layout():
    assert set(_TERMINAL_MODELS) == set(BeckhoffTerminalModel)


@pytest.mark.parametrize("model", ["EL3068", BeckhoffTerminalModel.EL3068])
def test_terminal_model_given_as_string_or_enum(model):
    gpio = BeckhoffGPIO("XF:TEST", "GPIO:2", {1: model})
    assert gpio.terminals[1].model is BeckhoffTerminalModel.EL3068
    assert len(gpio.terminals[1].channels) == 8

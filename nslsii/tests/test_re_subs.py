import pytest
import json

from nslsii.re_subs import BlueskyDocStreamPrinter, BlueskyDocJSONWriter

TEST_DOCS = [
    (
        "start",
        {
            "uid": "05275162-aa62-4ef1-97c5-7ced141e3d07",
            "time": 1744206429.5673962,
            "versions": {"ophyd": "1.10.0", "bluesky": "1.13.2.dev44+g0ed50c97"},
            "scan_id": 3,
            "plan_type": "generator",
            "plan_name": "test",
        },
    ),
    (
        "descriptor",
        {
            "configuration": {
                "manta-cam1": {
                    "data": {
                        "manta-cam1-driver-acquire_period": 0.0036960000000000005,
                        "manta-cam1-driver-acquire_time": 0.2,
                    },
                    "timestamps": {
                        "manta-cam1-driver-acquire_time": 1742823799.303813,
                    },
                    "data_keys": {
                        "manta-cam1-driver-acquire_time": {
                            "dtype": "number",
                            "shape": [],
                            "dtype_numpy": "<f8",
                            "source": "ca://XF:31ID1-ES{GigE-Cam:1}cam1:AcquireTime_RBV",
                            "units": "",
                            "precision": 3,
                        },
                    },
                },
            },
            "data_keys": {
                "manta-cam1": {
                    "source": "ca://XF:31ID1-ES{GigE-Cam:1}HDF1:FullFileName_RBV",
                    "shape": [1, 544, 728],
                    "dtype": "array",
                    "dtype_numpy": "|u1",
                    "external": "STREAM:",
                    "object_name": "manta-cam1",
                },
            },
            "name": "primary",
            "object_keys": {"manta-cam1": ["manta-cam1"]},
            "run_start": "05275162-aa62-4ef1-97c5-7ced141e3d07",
            "time": 1744206431.9428892,
            "uid": "44e54ca5-a109-4cda-b75f-2dd58d103fc6",
            "hints": {
                "manta-cam1": {"fields": ["manta-cam1"]},
            },
        },
    ),
    (
        "stream_resource",
        {
            "uid": "603f7d89-db27-46d5-b328-ca1449477de2",
            "data_key": "manta-cam1",
            "mimetype": "application/x-hdf5",
            "uri": "file://localhost/tmp/tiled_storage/data/3a846589-a019-45e9-9521-5e642880e945.h5",
            "parameters": {"dataset": "/entry/data/data", "chunk_shape": [1, 544, 728]},
            "run_start": "05275162-aa62-4ef1-97c5-7ced141e3d07",
        },
    ),
    (
        "stream_datum",
        {
            "stream_resource": "603f7d89-db27-46d5-b328-ca1449477de2",
            "uid": "603f7d89-db27-46d5-b328-ca1449477de2/0",
            "seq_nums": {"start": 1, "stop": 6},
            "indices": {"start": 0, "stop": 5},
            "descriptor": "44e54ca5-a109-4cda-b75f-2dd58d103fc6",
        },
    ),
    (
        "stop",
        {
            "uid": "fedac0af-0491-4d8c-8414-bc7d8276ddde",
            "time": 1744206433.3085756,
            "run_start": "05275162-aa62-4ef1-97c5-7ced141e3d07",
            "exit_status": "success",
            "reason": "",
            "num_events": {"primary": 5},
        },
    ),
]


@pytest.mark.parametrize(
    "printing_enabled, expected_output",
    [
        (True, [(f"name = '{name}'", json.dumps(doc, indent=4)) for name, doc in TEST_DOCS]),
        (False, [("", "")] * len(TEST_DOCS)),
    ],
)
def test_bs_doc_stream_printer(capsys, printing_enabled: bool, expected_output: list[tuple[str, str]]):
    """Test that the dump_docs_to_stdout function works as expected."""

    doc_stream_printer = BlueskyDocStreamPrinter()

    if printing_enabled:
        doc_stream_printer.enable_printing()
    else:
        doc_stream_printer.disable_printing()

    # Check that the output is as expected
    for i, (name, doc) in enumerate(TEST_DOCS):
        doc_stream_printer(name, doc)
        out, err = capsys.readouterr()
        assert expected_output[i][0] in out
        assert expected_output[i][1] in out


@pytest.mark.parametrize(
    "writing_enabled, flush_each_doc_enabled, expected_file_exists, expected_file_contents",
    [
        (
            True,
            True,
            [True] * len(TEST_DOCS),
            [[{name: doc} for name, doc in TEST_DOCS[:i]] for i in range(1, len(TEST_DOCS) + 1)],
        ),
        (
            True,
            False,
            [False] * (len(TEST_DOCS) - 1) + [True],
            [None] * (len(TEST_DOCS) - 1) + [[{name: doc} for name, doc in TEST_DOCS]],
        ),
        (False, True, [False] * len(TEST_DOCS), [None] * len(TEST_DOCS)),
        (False, False, [False] * len(TEST_DOCS), [None] * len(TEST_DOCS)),
    ],
)
def test_json_bluesky_doc_writer(
    tmp_path,
    writing_enabled: bool,
    flush_each_doc_enabled: bool,
    expected_file_exists: list[bool],
    expected_file_contents: list[dict | None],
):
    """Test that the JSONBlueskyDocWriter works as expected."""

    writer = BlueskyDocJSONWriter(write_directory=tmp_path, flush_on_each_doc=flush_each_doc_enabled)
    expected_filename = TEST_DOCS[0][1]["uid"] + ".json"

    if writing_enabled:
        writer.enable_writing()
    else:
        writer.disable_writing()

    for i, (name, doc) in enumerate(TEST_DOCS):
        writer(name, doc)

        assert (tmp_path / expected_filename).exists() == expected_file_exists[i]
        if expected_file_contents[i] is not None:
            with open(tmp_path / expected_filename) as fp:
                data = json.load(fp)
                assert data == expected_file_contents[i]


def test_json_bluesky_doc_writer_no_start(tmp_path):
    """Test that the JSONBlueskyDocWriter works as expected when there is no start document."""

    writer = BlueskyDocJSONWriter(tmp_path)
    expected_filename = TEST_DOCS[0][1]["uid"] + ".json"

    writer(*TEST_DOCS[1])

    assert not (tmp_path / expected_filename).exists()

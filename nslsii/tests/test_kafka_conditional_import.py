import os
import subprocess
import sys

import pytest

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_conditional_import_negative_case():
    """
    Test that bluesky_kafka is not imported when publish_documents_with_kafka=False.

    This is a "subprocess test" meaning the entire test function executes in a
    separate python interpreter and the pass/fail result is returned by sys.exit().
    This is necessary to guarantee bluesky_kakfa has not been imported as a result
    of other tests.
    """
    the_test = """
import sys
from unittest.mock import Mock

import IPython.core.interactiveshell

import nslsii


ip = IPython.core.interactiveshell.InteractiveShell()
nslsii.configure_base(
    user_ns=ip.user_ns,
    redis_url = "localhost",
    redis_prefix = "",
    # a mock databroker will be enough for this test
    broker_name=Mock(),
    bec=False,
    epics_context=False,
    magics=False,
    mpl=False,
    configure_logging=False,
    pbar=False,
    ipython_logging=False,
    # this is the important condition for the test
    publish_documents_with_kafka=False,
)

if "bluesky_kafka" in sys.modules:
    sys.exit(1)
else:
    sys.exit(0)    
"""
    proc = subprocess.run(
        [sys.executable, "-c", the_test],
    )

    if proc.returncode:
        pytest.fail(
            "The subprocess returned with non-zero exit status " f"{proc.returncode}."
        )


test_bluesky_kafka_config = """\
---
  abort_run_on_kafka_exception: true
  bootstrap_servers:
    - kafka1:9092
    - kafka2:9092
    - kafka3:9092
  runengine_producer_config:
    acks: 0
    message.timeout.ms: 3000
    compression.codec: snappy
"""

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_conditional_import_positive_case(tmp_path):
    """
    Test that bluesky_kafka is imported when publish_documents_with_kafka=True.

    The connection to a Kafka broker will fail but that does not affect the test result.

    This is a "subprocess test" meaning the entire test function executes in a
    separate python interpreter and the pass/fail result is returned by sys.exit().
    This is necessary to guarantee bluesky_kakfa has not been imported as a result
    of other tests.
    """

    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"
    with open(test_config_file_path, "wt") as f:
        f.write(test_bluesky_kafka_config)

    the_test = f"""
import sys
from unittest.mock import Mock

import IPython.core.interactiveshell

import nslsii


ip = IPython.core.interactiveshell.InteractiveShell()
nslsii.configure_base(
    user_ns=ip.user_ns,
    redis_url = "localhost",
    redis_prefix = "",
    # a mock databroker will be enough for this test
    broker_name=Mock(),
    bec=False,
    epics_context=False,
    magics=False,
    mpl=False,
    configure_logging=False,
    pbar=False,
    ipython_logging=False,
    # this is the important condition for the test
    publish_documents_with_kafka=True,
)

if "bluesky_kafka" in sys.modules:
    sys.exit(0)
else:
    sys.exit(1)    
"""
    proc = subprocess.run(
        [sys.executable, "-c", the_test],
        env={
            **os.environ,
            "BLUESKY_KAFKA_CONFIG_PATH": str(test_config_file_path),
        },
    )

    if proc.returncode:
        pytest.fail(
            "The subprocess returned with non-zero exit status " f"{proc.returncode}."
        )

import pytest

from bluesky.plan_stubs import open_run, close_run, null, mv, trigger_and_read
from bluesky.tests.utils import DocCollector

from nslsii.batches import setup_batch


@pytest.mark.parametrize("N", [1, 2])
@pytest.mark.parametrize("M", [1, 2])
@pytest.mark.parametrize("comment_function", [None, lambda doc: doc["bob"]])
def test_batch(RE, N, M, comment_function):
    comment_function = None
    batch_md = {"bob": "ardvark", "number": 5}

    def inner_plan(M):
        for j in range(M):
            yield from open_run()
            yield from close_run()

    def batch(batch_md, *, N=5, M=1, comment_function=None):
        add_to_batch, close_batch = yield from setup_batch(
            batch_md, comment_function=comment_function
        )
        for j in range(N):
            yield from add_to_batch(inner_plan(M=M))
        yield from close_batch()

    dc = DocCollector()
    RE(batch(batch_md, N=N, M=M, comment_function=comment_function), dc.insert)

    assert len(dc.start) == N * M + 1
    assert len(dc.stop) == N * M + 1

    assert len(set(d["batch_uid"] for d in dc.start)) == 1

    for start in dc.start:
        if "purpose" in start:
            assert start["purpose"] == "batch header"
            for k in batch_md:
                assert start[k] == batch_md[k]
        else:
            assert start["batch_md"] == batch_md
            assert "batch_index" in start

    (batch_header,) = (
        doc for doc in dc.start if doc.get("purpose", None) == "batch header"
    )

    (desc,) = dc.descriptor[batch_header["uid"]]
    events = dc.event[desc["uid"]]

    assert len(events) == N * M

    test_func = comment_function or (lambda doc: f'step {doc["batch_index"]}')
    start_by_uid = {doc["uid"]: doc for doc in dc.start}
    for evnt in events:
        assert evnt["data"]["step_comment"] == test_func(
            start_by_uid[evnt["data"]["step_uid"]]
        )

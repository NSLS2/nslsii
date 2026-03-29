"""
Tools to manage "batches" of runs.

The goal is to provide tools to easily group more than one run into a larger
unit that can be atomically retried for analysis.

"""

import uuid
from functools import partial
from itertools import count

from bluesky import Msg
from bluesky.plan_stubs import open_run, mv, trigger_and_read
from bluesky.preprocessors import set_run_key_wrapper, subs_wrapper, msg_mutator
from ophyd import Device, Signal, Component as Cpt

# do not leak imports or helpers
__all__ = ["setup_batch"]


class RunMd(Device):
    """A helper synthetic device to read per-run batch data from."""

    uid = Cpt(Signal, value="", kind="hinted")
    comment = Cpt(Signal, value="", kind="normal")
    index = Cpt(Signal, value=0, kind="hinted")


def setup_batch(batch_md, *, comment_function=None):
    """
    Set up a "batch" run.

    This will create an additional run, on top of any wrapped runs that
    includes *batch_md* flatted into the start document, a key `'purpose'` with
    the value `"batch header"` and a key `'batch_uid'` with a generated uid..

    The primary event stream of this run will include the keys: `'step_uid'`,
    `'step_comment'` and `'step_index'` extracted from the "batched" runs.

    Each wrapped run will have the key `'batch_md'` with the *batch_md* as the
    value, `'batch_uid'` with the generated uid as the value and
    `'batch_index'` with the running count of runs in this batch (starting from
    0).  If the start documents already contain any of these keys the user
    values will be respected (but do this at your own risk).

    Parameters
    ----------
    batch_md : dict[str, Any]
        Needs to be insertable to a start document.

    comment_function : Optional[Callable[Start, str]]
        A function to extarct a string comment from a start document.  If
        this raises it will kill the scan.

        If not specific defaults to `f"step {index}"`

    Yields
    ------
    msg : Msg
        To open a run for the "header" run.

    Returns
    -------
    add_to_batch : GeneratorFunction[plan] -> Any
        This wraps the inner plan in the batch.

        What ever the inner plan returns (if anything) will be returned by the
        wrapper.

    close_batch : Callable -> None
        Yield from this plan to close the batch (emit a stop document to

        Only run this once!

    Examples
    --------
    Typical usage::

       def batch(batch_md, *, N=5, comment_function=None):
           add_to_batch, close_batch = yield from setup_batch(
               batch_md, comment_function=comment_function
           )
           for j in range(N):
               yield from add_to_batch(inner_plan())
           yield from close_batch()

    """
    # do not mutate the input!
    batch_md = dict(batch_md)
    batch_md.pop("batch_uid", None)
    md = RunMd(name="step")
    run_index = count()
    batch_uid = str(uuid.uuid4())

    srk_wrapper = partial(set_run_key_wrapper, run=f"batch_leader-{batch_uid}")

    yield from srk_wrapper(
        open_run(md={**batch_md, "purpose": "batch header", "batch_uid": batch_uid})
    )

    def enrich_metadata(msg):
        if msg.command != "open_run":
            return msg
        # TODO maybe force these?
        msg.kwargs.setdefault("batch_md", batch_md)
        msg.kwargs.setdefault("batch_index", next(run_index))
        msg.kwargs.setdefault("batch_uid", batch_uid)
        return msg

    def add_to_batch(inner):
        """
        Wrap a plan to be included in the batch.

        This function is bound to the batch that created it via closures.

        Parameters
        ----------
        inner : plan
            The plan to wrap.  This may create any number of runs.
        """
        starts = []
        ret = yield from subs_wrapper(
            msg_mutator(inner, enrich_metadata),
            {"start": [lambda name, doc: starts.append(doc)]},
        )
        for start in starts:
            j = start["batch_index"]
            comment = (
                comment_function(start) if comment_function is not None else f"step {j}"
            )
            yield from mv(
                *(md.uid, start["uid"]),
                *(md.index, j),
                *(md.comment, comment),
            )

            yield from srk_wrapper(trigger_and_read([md]))
        # return what ever the wrapped plan returned
        return ret

    def close_batch(exit_status=None, reason=None):
        """
        Close the "header" run.

        This function is bound to the batch that created it via closures.

        .. warning ::

            Only run this once!

        Parameters
        ----------
        exit_status : {None, 'success', 'abort', 'fail'}
            The exit status to report in the Stop document
        reason : str, optional
            Long-form description of why the run ended

        Yields
        ------
        msg : Msg
            Msg('close_run')

        """
        return (
            yield Msg(
                "close_run",
                exit_status=exit_status,
                reason=reason,
                run=f"batch_leader-{batch_uid}",
            )
        )

    return add_to_batch, close_batch

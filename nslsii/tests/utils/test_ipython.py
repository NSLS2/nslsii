from unittest.mock import MagicMock

import pytest
from bluesky.run_engine import RunEngine
from IPython.terminal.prompts import Prompts
from pygments.token import Token
from pytest_mock import MockerFixture

from nslsii.utils import ProposalIDPrompt


@pytest.fixture
def shell(mocker: MockerFixture) -> MagicMock:
    shell = mocker.MagicMock()
    shell.execution_count = 7
    return shell


@pytest.fixture
def RE() -> RunEngine:
    return RunEngine({})


def test_is_ipython_prompts_subclass(RE: RunEngine, shell: MagicMock) -> None:
    prompt = ProposalIDPrompt(RE, shell)
    assert isinstance(prompt, Prompts)
    assert prompt.shell is shell


def test_prompt_shows_data_session(RE: RunEngine, shell: MagicMock) -> None:
    RE.md["data_session"] = "pass-123456"
    tokens = ProposalIDPrompt(RE, shell).in_prompt_tokens()
    assert tokens == [
        (Token.Prompt, "pass-123456 ["),
        (Token.PromptNum, "7"),
        (Token.Prompt, "]: "),
    ]


def test_prompt_falls_back_when_no_data_session(
    RE: RunEngine, shell: MagicMock
) -> None:
    RE.md.pop("data_session", None)
    tokens = ProposalIDPrompt(RE, shell).in_prompt_tokens()
    assert tokens[0] == (Token.Prompt, "N/A [")


def test_prompt_reflects_metadata_changes(RE: RunEngine, shell: MagicMock) -> None:
    prompt = ProposalIDPrompt(RE, shell)
    RE.md["data_session"] = "pass-111111"
    assert prompt.in_prompt_tokens()[0] == (Token.Prompt, "pass-111111 [")
    RE.md["data_session"] = "pass-222222"
    assert prompt.in_prompt_tokens()[0] == (Token.Prompt, "pass-222222 [")


def test_prompt_reflects_execution_count(RE: RunEngine, shell: MagicMock) -> None:
    prompt = ProposalIDPrompt(RE, shell)
    shell.execution_count = 42
    assert prompt.in_prompt_tokens()[1] == (Token.PromptNum, "42")


def test_rendered_prompt_text(RE: RunEngine, shell: MagicMock) -> None:
    RE.md["data_session"] = "pass-123456"
    text = "".join(t for _, t in ProposalIDPrompt(RE, shell).in_prompt_tokens())
    assert text == "pass-123456 [7]: "

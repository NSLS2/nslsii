
from bluesky.run_engine import RunEngine
from IPython.core.getipython import get_ipython
from IPython.terminal.interactiveshell import TerminalInteractiveShell
from IPython.terminal.prompts import Prompts
from pygments.token import Token


class ProposalIDPrompt(Prompts):
    """Custom IPython prompt that shows the current proposal ID."""

    def __init__(self, RE: RunEngine, shell: TerminalInteractiveShell):
        super().__init__(shell)
        self._RE = RE

    def in_prompt_tokens(self, cli=None):
        return [
            (
                Token.Prompt,
                f"{self._RE.md.get('data_session', 'N/A')} [",
            ),
            (Token.PromptNum, str(self.shell.execution_count)),
            (Token.Prompt, "]: "),
        ]

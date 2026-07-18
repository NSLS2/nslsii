import json
import os
from pathlib import Path


class BlueskyDocJSONWriter:
    def __init__(self, write_directory: Path | None = None, flush_on_each_doc: bool = True):
        self._write_json_file: bool = False
        self._flush_on_each_doc = flush_on_each_doc
        self._output_file_name: str | None = None
        self._document_cache = []
        self._write_directory: Path = Path("/tmp")
        if write_directory is not None:
            self.set_write_directory(write_directory)

    def set_write_directory(self, write_directory: Path):
        """
        Set the directory to write JSON files to.
        """

        if not os.path.exists(write_directory):
            raise FileNotFoundError(f"Directory does not exist: {write_directory}")
        elif not os.access(write_directory, os.W_OK):
            raise PermissionError(f"Cannot write to directory: {write_directory}")

        self._write_directory = write_directory

    def enable_writing(self):
        """
        Enable writing of JSON files.
        """

        self._write_json_file = True

    def disable_writing(self):
        """
        Disable writing of JSON files.
        """

        self._write_json_file = False
        if self._output_file_name is not None:
            self._document_cache = []
            self._output_file_name = None

    def __call__(self, name: str, doc: dict):
        if self._write_json_file:
            if name == "start":
                self._output_file_name = os.path.join(self._write_directory, f"{doc['uid']}.json")

            if self._output_file_name is not None:
                self._document_cache.append({name: doc})

                if self._flush_on_each_doc or name == "stop":
                    with open(self._output_file_name, "w") as fp:
                        json.dump(
                            self._document_cache,
                            fp,
                            indent=4,
                        )

                if name == "stop":
                    self._document_cache = []
                    self._output_file_name = None


class BlueskyDocStreamPrinter:
    """
    Print documents to stdout.
    """

    def __init__(self):
        self._print_docs_to_stdout = True

    def enable_printing(self):
        """
        Enable printing of documents to stdout.
        """

        self._print_docs_to_stdout = True

    def disable_printing(self):
        """
        Disable printing of documents to stdout.
        """

        self._print_docs_to_stdout = False

    def __call__(self, name: str, doc: dict):
        if self._print_docs_to_stdout:
            print("========= Emitting Doc =============")
            print(f"{name = }")
            print(f"{json.dumps(doc, indent=4)}")
            print("============ Done ==================")

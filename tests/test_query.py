import os
import string
import tempfile
from contextlib import contextmanager
from pathlib import Path

import numpy as np
from datasets import Dataset

from datasets_sql import query


@contextmanager
def set_current_working_directory_to_temp_dir(*args, **kwargs):
    original_working_dir = str(Path().resolve())
    with tempfile.TemporaryDirectory(*args, **kwargs) as tmp_dir:
        try:
            os.chdir(tmp_dir)
            yield
        finally:
            os.chdir(original_working_dir)


def test_simple_query():
    d = Dataset.from_dict({"a": list(range(10))})
    d = query("SELECT * FROM d LIMIT 2")
    assert isinstance(d, Dataset)
    assert d.column_names == ["a"]
    assert d.num_rows == 2


def test_join_query():
    d1 = Dataset.from_dict({"id": list(range(10)), "text1": list(string.ascii_letters[:10])})
    d2 = Dataset.from_dict(
        {"id": np.random.permutation(list(range(10))), "text2": np.random.permutation(list(string.ascii_letters[:10]))}
    )
    d = query("SELECT d1.id, text1, text2 FROM d1 JOIN d2 ON d1.id = d2.id")
    assert isinstance(d, Dataset)
    assert d.column_names == ["id", "text1", "text2"]
    assert len(d) == 10


def test_obj_attr_replacement_scan():
    class Data:
        def __init__(self):
            self.d = Dataset.from_dict({"a": list(range(10))})

    d_obj = Data()
    d = query("SELECT * FROM d_obj.d LIMIT 2")
    assert isinstance(d, Dataset)
    assert d.column_names == ["a"]
    assert d.num_rows == 2


def test_query_caching():
    with set_current_working_directory_to_temp_dir():
        d = Dataset.from_dict({"a": list(range(10))})
        dataset_path = "my_dataset"
        d.save_to_disk(dataset_path)
        with Dataset.load_from_disk(dataset_path) as d:
            with query("SELECT * FROM d LIMIT 2") as d1:
                d1_cache_files = d1.cache_files
                d1_fingerprint = d1._fingerprint
            with query("SELECT * FROM d LIMIT 2") as d2:
                d2_cache_files = d2.cache_files
                d2_fingerprint = d2._fingerprint
            assert len(d1_cache_files) > 0 and d1_cache_files == d2_cache_files
            assert d1_fingerprint == d2_fingerprint

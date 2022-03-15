import string

import numpy as np
from datasets import Dataset

from datasets_sql import query


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

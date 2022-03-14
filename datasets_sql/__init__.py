import pyarrow
from packaging import version


if version.parse(pyarrow.__version__).major < 7:
    raise ImportWarning(
        "To use `datasets_sql`, the module `pyarrow>=7.0.0` is required, and the current version of `pyarrow` doesn't match this condition."
    )

del pyarrow
del version

from .query import query

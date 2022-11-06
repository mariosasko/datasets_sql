import pyarrow
from packaging import version


__version___ = "0.2.0"


if version.parse(pyarrow.__version__).major < 5:
    raise ImportWarning(
        "To use `datasets_sql`, the module `pyarrow>=5.0.0` is required, and the current version of `pyarrow` doesn't match this condition."
    )

del pyarrow
del version

from .query import query

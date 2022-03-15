import inspect
import os
import shutil
import tempfile
from pathlib import PurePath
from typing import Optional

import duckdb
import pyarrow as pa
from datasets import Dataset, DatasetInfo, Features
from datasets.arrow_writer import ArrowWriter
from datasets.fingerprint import Hasher, is_caching_enabled
from datasets.utils.logging import get_logger
from sql_metadata import Parser, QueryType


logger = get_logger(__name__)


QUERY_FUNC_VERSION = "1.0"


def _query_func_identifier():
    return f"{query.__module__}.{query.__name__}@{QUERY_FUNC_VERSION}"


def query(
    sql_query: str,
    keep_in_memory: bool = False,
    load_from_cache_file: bool = None,
    cache_file_name: Optional[str] = None,
    writer_batch_size: Optional[int] = 1000,
    features: Optional[Features] = None,
    disable_nullable: bool = False,
    new_fingerprint: Optional[str] = None,
) -> Dataset:
    """
    Run a SQL query against one or more :class:`Dataset` objects and return the resultant table as a :class:`Dataset` object. Datasets are referenced by their variable names
    in the query string. The query must be a SELECT statement.

    Args:
        keep_in_memory (:obj:`bool`, default `False`): Keep the resultant dataset in memory instead of writing it to a cache file.
        load_from_cache_file (:obj:`bool`, default `True` if caching is enabled): If a cache file storing the result of the query
            can be identified, use it instead of recomputing.
        cache_file_name (:obj:`str`, optional, default `None`): Provide the name of a path for the cache file. It is used to store the
            results of the query instead of the automatically generated cache file name.
        writer_batch_size (:obj:`int`, default `1000`): Number of rows per write operation for the cache file writer.
            This value is a good trade-off between memory usage during the processing, and processing speed.
            Higher value makes the processing do fewer lookups, lower value consume less temporary memory..
        features (`Optional[datasets.Features]`, default `None`): Use a specific Features to store the cache file
            instead of the automatically generated one.
        disable_nullable (:obj:`bool`, default `False`): Disallow null values in the table.
        new_fingerprint (:obj:`str`, optional, default `None`): the new fingerprint of the resultant dataset.
            If `None`, the new fingerprint is computed using a hash of the datasets referenced in the query and the function arguments.

    Returns:
        :class:`Dataset`
    """
    if keep_in_memory and cache_file_name is not None:
        raise ValueError("Please use either `keep_in_memory` or `cache_file_name` but not both.")

    query_parser = Parser(sql_query)

    if query_parser.query_type is not QueryType.SELECT:
        raise ValueError("The query must be a SELECT statement.")

    # Traverse the frames in the reverse order and check their locals for the referenced datasets
    datasets = []
    frame_stack = inspect.stack()[1:]
    for table in query_parser.tables:
        for frame_info in frame_stack:
            frame_locals = frame_info.frame.f_locals
            if table in frame_locals:
                dataset = frame_locals[table]
                break
        else:
            raise ValueError(f"The dataset `{table}` not found in the namespace.")

        if not isinstance(dataset, Dataset):
            raise ValueError(f"The dataset `{table}` is not a Dataset object.")

        if dataset._indices is not None:
            raise ValueError(
                f"The dataset `{table}` has an indices mapping. Please flatten the indices with `.flatten_indices()`."
            )

        datasets.append(dataset)

    load_from_cache_file = load_from_cache_file if load_from_cache_file is not None else is_caching_enabled()

    if new_fingerprint is None:
        hasher = Hasher()
        sql_query_without_names = sql_query
        for name, dataset in zip(query_parser.tables, datasets):
            hasher.update(dataset._fingerprint)
            # Ignore the table names
            sql_query_without_names = sql_query_without_names.replace(name, "")
        hasher.update(sql_query_without_names)
        hasher.update(writer_batch_size)
        hasher.update(features)
        hasher.update(disable_nullable)
        hasher.update(_query_func_identifier())
        new_fingerprint = hasher.hexdigest()

    def init_buffer_and_writer():
        # Prepare output buffer and batched writer in memory or on file if we update the table
        writer_features = features
        if writer_features is None:
            writer_features = dataset.features
            update_features = True
        else:
            update_features = False
        if keep_in_memory or cache_file_name is None:
            buf_writer = pa.BufferOutputStream()
            tmp_file = None
            writer = ArrowWriter(
                features=writer_features,
                stream=buf_writer,
                writer_batch_size=writer_batch_size,
                update_features=update_features,
                fingerprint=new_fingerprint,
                disable_nullable=disable_nullable,
            )
        else:
            buf_writer = None
            logger.info(f"Caching processed dataset at {cache_file_name}")
            tmp_file = tempfile.NamedTemporaryFile("wb", dir=os.path.dirname(cache_file_name), delete=False)
            writer = ArrowWriter(
                features=writer_features,
                path=tmp_file.name,
                writer_batch_size=writer_batch_size,
                update_features=update_features,
                fingerprint=new_fingerprint,
                disable_nullable=disable_nullable,
            )
        return buf_writer, writer, tmp_file

    # TODO: Maybe a separate directory for sql query cache for easier cleanup?

    # Check cache
    if any(dataset.cache_files for dataset in datasets):
        if cache_file_name is None:
            # we create a unique hash from the function,
            # current dataset file and the mapping args
            cache_file_name = dataset._get_cache_file_path(new_fingerprint)
        if os.path.exists(cache_file_name) and load_from_cache_file:
            logger.warning(f"Loading cached processed dataset at {cache_file_name}")
            info = dataset.info.copy()
            info.features = features
            info.task_templates = None
            return Dataset.from_file(cache_file_name, info=info, split=dataset.split)

    # Connect to the database and execute the query
    db_file = str(PurePath(cache_file_name).with_suffix(".db")) if cache_file_name is not None else ":memory:"
    conn = duckdb.connect(database=db_file)
    for table, dataset in zip(query_parser.tables, datasets):
        conn.register(table, dataset.data.table)
    try:
        query_result = conn.execute(sql_query)
    except Exception:
        if db_file is not None:
            conn.close()
            if os.path.exists(db_file):
                os.remove(db_file)
        raise

    buf_writer, writer, tmp_file = init_buffer_and_writer()

    # Cache the result in an arrow file
    with writer:
        try:
            chunk_idx = 0
            while True:
                table = query_result.fetch_arrow_chunk(chunk_idx + 1, True)
                if not table:
                    break
                writer.write_table(table)
                chunk_idx += 1
        except (Exception, KeyboardInterrupt):
            if writer is not None:
                writer.finalize()
            if tmp_file is not None:
                tmp_file.close()
                if os.path.exists(tmp_file.name):
                    os.remove(tmp_file.name)
            if db_file is not None:
                conn.close()
                if os.path.exists(db_file):
                    os.remove(db_file)
            raise

    if tmp_file is not None:
        tmp_file.close()
        shutil.move(tmp_file.name, cache_file_name)
        umask = os.umask(0o666)
        os.umask(umask)
        os.chmod(cache_file_name, 0o666 & ~umask)

    if db_file is not None:
        conn.close()
        if os.path.exists(db_file):
            os.remove(db_file)

    info = DatasetInfo(features=writer._features)
    if buf_writer is None:
        result = Dataset.from_file(cache_file_name, info=info)
        result._fingerprint = new_fingerprint
    else:
        result = Dataset.from_buffer(buf_writer.getvalue(), info=info)
        result._fingerprint = new_fingerprint
    return result

"""Engine selection: :class:`LoaderFactory` and the :class:`DataLoader` facade.

``LoaderFactory`` maps engine names to loader classes and is the extension
point for future engines (Snowflake, Redshift, BigQuery, ...): implement
:class:`~dataSource.engines.base.BaseLoader` and register the class.

``DataLoader`` is the one-stop entry point — configure the engine once, then
use the identical API regardless of backend.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Optional, Sequence, Tuple, Type, Union

import pandas as pd

from dataSource.engines.athena_engine import AthenaLoader
from dataSource.engines.base import BaseLoader, DateLike, FrameOrChunks
from dataSource.engines.duckdb_engine import DuckDBLoader
from dataSource.engines.wrangler_engine import WranglerLoader
from dataSource.exceptions import ConfigurationError
from dataSource.utils import get_logger, require_non_empty_str

logger = get_logger("factory")

DEFAULT_ENGINE = "duckdb"


class LoaderFactory:
    """Create loader instances by engine name.

    Purpose:
        Central registry of available engines, decoupling application code
        from concrete loader classes.

    Supported operations:
        ``create``, ``register``, ``supported_engines``.

    Example:
        >>> loader = LoaderFactory.create(engine="athena", database="analytics")
        >>> LoaderFactory.supported_engines()
        ('athena', 'duckdb', 'wrangler')
    """

    _registry: Dict[str, Type[BaseLoader]] = {}

    @classmethod
    def register(
        cls,
        engine: str,
        loader_cls: Type[BaseLoader],
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a loader class under an engine name.

        Args:
            engine: Engine name (matched case-insensitively by ``create``).
            loader_cls: A :class:`BaseLoader` subclass.
            overwrite: Allow replacing an existing registration.

        Raises:
            ConfigurationError: If the name is taken (and ``overwrite`` is
                False) or ``loader_cls`` is not a ``BaseLoader`` subclass.

        Example:
            >>> LoaderFactory.register("snowflake", SnowflakeLoader)
        """
        key = require_non_empty_str(engine, "engine").lower()
        if not (isinstance(loader_cls, type) and issubclass(loader_cls, BaseLoader)):
            raise ConfigurationError(
                f"Engine {key!r} must be registered with a BaseLoader subclass, "
                f"got: {loader_cls!r}"
            )
        if key in cls._registry and not overwrite:
            raise ConfigurationError(
                f"Engine {key!r} is already registered; pass overwrite=True to replace it."
            )
        cls._registry[key] = loader_cls

    @classmethod
    def create(cls, engine: str = DEFAULT_ENGINE, **config: Any) -> BaseLoader:
        """Create a loader for the requested engine.

        Args:
            engine: One of :meth:`supported_engines` (case-insensitive).
                Defaults to ``"duckdb"``.
            **config: Engine-specific constructor arguments (e.g.
                ``database=``, ``workgroup=`` for athena; ``source=``,
                ``s3_prefix=`` for duckdb/wrangler).

        Returns:
            A configured :class:`BaseLoader` instance.

        Raises:
            ConfigurationError: If the engine name is unknown or the engine
                rejects its configuration.

        Example:
            >>> loader = LoaderFactory.create(
            ...     engine="athena", database="analytics", workgroup="primary"
            ... )
        """
        key = require_non_empty_str(engine, "engine").lower()
        loader_cls = cls._registry.get(key)
        if loader_cls is None:
            supported = ", ".join(sorted(cls._registry))
            raise ConfigurationError(
                f"Unsupported engine {engine!r}. Supported engines: {supported}."
            )
        logger.info("Creating %r loader", key)
        return loader_cls(**config)

    @classmethod
    def supported_engines(cls) -> Tuple[str, ...]:
        """Return the registered engine names, sorted.

        Returns:
            Tuple of engine names, e.g. ``("athena", "duckdb", "wrangler")``.

        Example:
            >>> "duckdb" in LoaderFactory.supported_engines()
            True
        """
        return tuple(sorted(cls._registry))


for _loader_cls in (DuckDBLoader, AthenaLoader, WranglerLoader):
    LoaderFactory.register(_loader_cls.engine, _loader_cls, overwrite=True)


class DataLoader(BaseLoader):
    """Engine-agnostic loader facade — configure the engine once, use one API.

    Purpose:
        The main entry point of the library. Delegates every call to the
        backend created by :class:`LoaderFactory`, so switching engines is a
        one-line change. DuckDB is the default engine, keeping historical
        behaviour.

    Supported operations:
        ``read_parquet``, ``read_sql``, ``read_table``, ``scan``,
        ``load_dataframe``.

    Example:
        >>> loader = DataLoader(
        ...     engine="athena",
        ...     database="analytics",
        ...     workgroup="primary",
        ...     output_location="s3://company-athena-results/",
        ... )
        >>> df = loader.read_sql("SELECT * FROM sales LIMIT 100")
        >>> DataLoader().engine_name          # default backend
        'duckdb'
    """

    engine = "auto"

    def __init__(self, engine: str = DEFAULT_ENGINE, **config: Any) -> None:
        """Create the facade and its backend.

        Args:
            engine: ``"duckdb"`` (default), ``"athena"``, ``"wrangler"``, or
                any engine registered on :class:`LoaderFactory`.
            **config: Engine-specific constructor arguments.

        Raises:
            ConfigurationError: If the engine is unknown or misconfigured.
        """
        self._backend = LoaderFactory.create(engine, **config)

    @property
    def backend(self) -> BaseLoader:
        """The concrete engine instance behind this facade."""
        return self._backend

    @property
    def engine_name(self) -> str:
        """The active engine's canonical name (e.g. ``"duckdb"``)."""
        return self._backend.engine

    def read_parquet(
        self,
        path: Optional[Union[str, Sequence[str]]] = None,
        *,
        columns: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> FrameOrChunks:
        """Read parquet data from S3 (delegates to the active engine).

        Args:
            path: ``s3://`` prefix, file path, or list of paths.
            columns: Optional column projection.
            **kwargs: Engine-specific options.

        Returns:
            A ``pandas.DataFrame`` or an iterator of DataFrames (chunked).

        Raises:
            LoaderError: On engine failure or unsupported operation.
            ValueError: On invalid arguments.

        Example:
            >>> df = DataLoader(engine="wrangler").read_parquet(
            ...     path="s3://bucket/data/", columns=["id", "price"]
            ... )
        """
        return self._backend.read_parquet(path, columns=columns, **kwargs)

    def read_sql(self, sql: str, **kwargs: Any) -> FrameOrChunks:
        """Execute SQL (delegates to the active engine).

        Args:
            sql: SQL text.
            **kwargs: Engine-specific options (e.g. ``collection=`` for
                duckdb, ``chunksize=`` for athena).

        Returns:
            A ``pandas.DataFrame`` or an iterator of DataFrames (chunked).

        Raises:
            LoaderError: On engine failure or unsupported operation.
            ValueError: On invalid arguments.

        Example:
            >>> df = DataLoader(engine="athena", database="analytics").read_sql(
            ...     "SELECT * FROM sales LIMIT 100"
            ... )
        """
        return self._backend.read_sql(sql, **kwargs)

    def read_table(
        self,
        table: str,
        *,
        columns: Optional[Sequence[str]] = None,
        date_column: Optional[str] = None,
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Load a table/collection (delegates to the active engine).

        Args:
            table: Table or collection name.
            columns: Optional column projection.
            date_column: Column for the inclusive date filter (all-or-none
                with ``start_date``/``end_date``).
            start_date: Inclusive range start.
            end_date: Inclusive range end.
            **kwargs: Engine-specific options.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            LoaderError: On engine failure.
            ValueError: On partial date-filter arguments.

        Example:
            >>> df = DataLoader().read_table("leads", columns=["_id"])
        """
        return self._backend.read_table(
            table,
            columns=columns,
            date_column=date_column,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )

    def scan(self, target: str, **kwargs: Any) -> Iterator[pd.DataFrame]:
        """Lazily stream a result in chunks (delegates to the active engine).

        Args:
            target: SQL (duckdb/athena) or path/collection (wrangler).
            **kwargs: Engine-specific options.

        Returns:
            An iterator of ``pandas.DataFrame`` chunks.

        Raises:
            LoaderError: On engine failure.

        Example:
            >>> for chunk in DataLoader().scan(
            ...     "SELECT * FROM parquet_scan('{parquet_path}')", collection="leads"
            ... ):
            ...     process(chunk)
        """
        return self._backend.scan(target, **kwargs)

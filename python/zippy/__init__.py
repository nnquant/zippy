"""Native server and local compute APIs with one explicit Flight client.

Use ``Client`` for Catalog, query, write and subscription operations. Connection ownership
is explicit; this package does not maintain a default client or a second data protocol.

:example: with Client("zippy://default") as client: tables = client.list_tables()
"""

from zippy_client import Client as Client
from zippy_client import Row as Row
from zippy_client import col as col
from zippy_client import lit as lit

from ._internal import NativeProcessor as NativeProcessor
from ._internal import run_server_daemon as run_server_daemon
from ._internal import resolve_server_control_endpoint as resolve_server_control_endpoint
from ._internal import log_info as log_info
from ._internal import setup_log as setup_log
from ._internal import version as version
from ._internal import __version__ as __version__
from ._internal import __build_revision__ as __build_revision__
from ._internal import AbsSpec as AbsSpec
from ._internal import AggCountSpec as AggCountSpec
from ._internal import AggFirstSpec as AggFirstSpec
from ._internal import AggLastSpec as AggLastSpec
from ._internal import AggMaxSpec as AggMaxSpec
from ._internal import AggMinSpec as AggMinSpec
from ._internal import AggSumSpec as AggSumSpec
from ._internal import AggVwapSpec as AggVwapSpec
from ._internal import CastSpec as CastSpec
from ._internal import ClipSpec as ClipSpec
from ._internal import CSDemeanSpec as CSDemeanSpec
from ._internal import CSRankSpec as CSRankSpec
from ._internal import CSZscoreSpec as CSZscoreSpec
from ._internal import ExpressionFactor as ExpressionFactor
from ._internal import LogSpec as LogSpec
from ._internal import TsDelaySpec as TsDelaySpec
from ._internal import TsDiffSpec as TsDiffSpec
from ._internal import TsEmaSpec as TsEmaSpec
from ._internal import TsMeanSpec as TsMeanSpec
from ._internal import TsReturnSpec as TsReturnSpec
from ._internal import TsStdSpec as TsStdSpec
from .compute import ComputePipeline as ComputePipeline
from .factors import Duration as Duration
from .factors import TS_EMA as TS_EMA
from .factors import TS_MEAN as TS_MEAN
from .factors import TS_STD as TS_STD
from .factors import TS_DELAY as TS_DELAY
from .factors import TS_DIFF as TS_DIFF
from .factors import TS_RETURN as TS_RETURN
from .factors import ABS as ABS
from .factors import LOG as LOG
from .factors import CLIP as CLIP
from .factors import CAST as CAST
from .factors import Expr as Expr
from .factors import AGG_FIRST as AGG_FIRST
from .factors import AGG_LAST as AGG_LAST
from .factors import AGG_MAX as AGG_MAX
from .factors import AGG_MIN as AGG_MIN
from .factors import AGG_SUM as AGG_SUM
from .factors import AGG_COUNT as AGG_COUNT
from .factors import AGG_VWAP as AGG_VWAP
from .factors import CS_RANK as CS_RANK
from .factors import CS_ZSCORE as CS_ZSCORE
from .factors import CS_DEMEAN as CS_DEMEAN
from .factors import bar as bar
from .replay_compare import compare_replay as compare_replay
from .replay_source import ParquetReplayEngine as ParquetReplayEngine
from .replay_source import ParquetReplaySource as ParquetReplaySource
from .state import KeyValueState as KeyValueState
from .state import StateSubscription as StateSubscription

__all__ = [
    "Client",
    "Row",
    "col",
    "lit",
    "NativeProcessor",
    "run_server_daemon",
    "resolve_server_control_endpoint",
    "log_info",
    "setup_log",
    "version",
    "__version__",
    "__build_revision__",
    "AbsSpec",
    "AggCountSpec",
    "AggFirstSpec",
    "AggLastSpec",
    "AggMaxSpec",
    "AggMinSpec",
    "AggSumSpec",
    "AggVwapSpec",
    "CastSpec",
    "ClipSpec",
    "CSDemeanSpec",
    "CSRankSpec",
    "CSZscoreSpec",
    "ExpressionFactor",
    "LogSpec",
    "TsDelaySpec",
    "TsDiffSpec",
    "TsEmaSpec",
    "TsMeanSpec",
    "TsReturnSpec",
    "TsStdSpec",
    "Duration",
    "TS_EMA",
    "TS_MEAN",
    "TS_STD",
    "TS_DELAY",
    "TS_DIFF",
    "TS_RETURN",
    "ABS",
    "LOG",
    "CLIP",
    "CAST",
    "Expr",
    "AGG_FIRST",
    "AGG_LAST",
    "AGG_MAX",
    "AGG_MIN",
    "AGG_SUM",
    "AGG_COUNT",
    "AGG_VWAP",
    "CS_RANK",
    "CS_ZSCORE",
    "CS_DEMEAN",
    "bar",
    "ComputePipeline",
    "compare_replay",
    "ParquetReplayEngine",
    "ParquetReplaySource",
    "KeyValueState",
    "StateSubscription",
]

# Namespace package: https://docs.python.org/2/library/pkgutil.html
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

from .aggregate import AggregateKey, aggregate_count, aggregate_rate
from .comment import Comment
from .compute_id import ComputeIdFrom
from .department import ComputeDepartmentKey, SetDepartment
from .rank import ComputeRank
from .remove_key import RemoveKey

from .load_plugin import load_plugins

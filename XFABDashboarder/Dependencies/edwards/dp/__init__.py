"""The module includes classes for computing derived parameters."""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>
#          Dennis Hou <duanyang.hou@edwardsvacuum.com>

from ._base import Base
from ._pdm_trend import PdMTrend
from ._trend import Trend
from ._spike import Spike
from ._spike2 import Spike2
from ._independent_spike import IndependentSpike
from ._switching_stft import SwitchingSTFT
from ._switching_count import SwitchingCount
from ._similarity_search import SimilaritySearch
from ._no_baseline_spike import NoBaselineSpike
from ._vi_trend import ViTrend

__all__ = [
    'Base',
    'PdMTrend',
    'Trend',
    'Spike',
    'Spike2',
    'IndependentSpike',
    'SwitchingSTFT',
    'SwitchingCount',
    'SimilaritySearch',
    'NoBaselineSpike',
    'ViTrend',
]

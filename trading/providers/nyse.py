

from functools import cached_property
from typing import Sequence, override
from trading.core.securities import Exchange, Security


class NYSESecurity(Security):
    pass

class NYSE(Exchange):
    
    @cached_property
    @override
    def securities(self) -> Sequence[NYSESecurity]:
        return []
    
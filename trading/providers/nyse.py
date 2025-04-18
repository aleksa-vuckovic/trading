from typing import Sequence, override
from base.caching import cached_scalar
from base.utils import cached
from trading.core.securities import Exchange, Security


class NYSESecurity(Security):
    pass

class NYSE(Exchange):
    
    @override
    @cached
    def securities(self) -> Sequence[NYSESecurity]:
        return []
    
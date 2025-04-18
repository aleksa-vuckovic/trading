from typing import Sequence, override
from base.caching import cached_scalar
from trading.core.securities import Exchange, Security


class NYSESecurity(Security):
    pass

class NYSE(Exchange):
    
    @override
    @cached_scalar()
    def securities(self) -> Sequence[NYSESecurity]:
        return []
    
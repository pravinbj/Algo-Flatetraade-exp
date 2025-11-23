"""Adapter layer to allow swapping API backends for market data and instrument lookup.

Provides a simple, minimal interface used by scripts in this repo:
- initialize() -> returns backend client object
- searchscrip(exchange, searchtext) -> returns searchscrip response dict
- get_quotes(exchange, token) -> returns quote dict
- get_time_price_series(exchange, token, starttime, endtime, interval) -> list/dict

Currently implements a Noren adapter that wraps the existing market_data.initialize_api()
to keep changes minimal. If you later add the external pythonAPI package into the workspace
we can implement another adapter class that wraps that package and switch via config.
"""
from typing import Any, Dict, List, Optional

import market_data


class APIAdapter:
    """Abstract adapter interface (duck-typed)."""
    def initialize(self) -> Any:
        raise NotImplementedError()

    def searchscrip(self, exchange: str, searchtext: str) -> Dict:
        raise NotImplementedError()

    def get_quotes(self, exchange: str, token: str) -> Dict:
        raise NotImplementedError()

    def get_time_price_series(self, exchange: str, token: str, starttime: str, endtime: str, interval: str) -> List[Dict]:
        raise NotImplementedError()


class NorenAdapter(APIAdapter):
    """Adapter for the existing Noren/Flattrade wrapper in this repo."""
    def __init__(self):
        self.client = None

    def initialize(self):
        # initialize_api handles authentication and returns the API client
        self.client = market_data.initialize_api()
        return self.client

    def searchscrip(self, exchange: str, searchtext: str) -> Dict:
        if not self.client:
            self.initialize()
        return self.client.searchscrip(exchange=exchange, searchtext=searchtext)

    def get_quotes(self, exchange: str, token: str) -> Dict:
        if not self.client:
            self.initialize()
        return self.client.get_quotes(exchange=exchange, token=token)

    def get_time_price_series(self, exchange: str, token: str, starttime: str, endtime: str, interval: str):
        if not self.client:
            self.initialize()
        return self.client.get_time_price_series(exchange=exchange, token=token, starttime=starttime, endtime=endtime, interval=interval)


# Factory
def get_adapter(name: str = 'noren') -> APIAdapter:
    name = name.lower()
    if name in ('noren', 'flattrade', 'norenrest'):
        return NorenAdapter()
    # Future adapters can be returned here (e.g., 'pythonapi')
    raise ValueError(f"Unknown API adapter: {name}")

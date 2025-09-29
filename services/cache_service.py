"""Query caching with TTL"""
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple, Optional
import hashlib
import json

class QueryCache:
    def __init__(self, ttl_seconds: int = 300, max_size: int = 1000):
        self.cache: Dict[str, Tuple[datetime, Any]] = {}
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def _generate_key(self, query: str, **kwargs) -> str:
        data = f"{query}:{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def get(self, query: str, **kwargs) -> Tuple[Optional[Any], bool]:
        key = self._generate_key(query, **kwargs)
        
        if key in self.cache:
            cached_time, result = self.cache[key]
            if datetime.now() - cached_time < timedelta(seconds=self.ttl_seconds):
                self.hits += 1
                return result, True
            else:
                del self.cache[key]
        
        self.misses += 1
        return None, False
    
    def set(self, query: str, value: Any, **kwargs):
        if len(self.cache) >= self.max_size:
            oldest = min(self.cache.items(), key=lambda x: x[1][0])
            del self.cache[oldest[0]]
        
        key = self._generate_key(query, **kwargs)
        self.cache[key] = (datetime.now(), value)
    
    def clear(self):
        self.cache.clear()
        self.hits = 0
        self.misses = 0
    
    def get_stats(self) -> Dict[str, Any]:
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "cache_size": len(self.cache),
            "max_size": self.max_size
        }
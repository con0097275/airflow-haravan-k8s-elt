 # registry + ETLFactory
 
from typing import Dict, Type
from .base import ApiETL

# Global registry of pipeline name -> ETL class
REGISTRY: Dict[str, Type[ApiETL]] = {}

def register_pipeline(name: str):
    """Decorator: @register_pipeline('haravan') on class definition."""
    def _wrap(cls: Type[ApiETL]) -> Type[ApiETL]:
        REGISTRY[name] = cls
        return cls
    return _wrap

class ETLFactory:
    @staticmethod
    def create(name: str, **kwargs) -> ApiETL:
        try:
            cls = REGISTRY[name]
        except KeyError:
            raise ValueError(f"Unknown pipeline '{name}'. Registered: {list(REGISTRY)}")
        return cls(**kwargs)



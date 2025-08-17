# Ensure subpackages load & register themselves with the factory
from .base import ApiETL
from .factory import ETLFactory, register_pipeline
from .dw import DatawarehouseConnection

# Import each pipeline subpackage so its @register_pipeline runs:
from . import haravan  

__all__ = ["ApiETL", "ETLFactory", "register_pipeline", "DatawarehouseConnection"]

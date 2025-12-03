"""
Paquete loaddata
================

Clase principal:
- load_data: Clase para carga incremental a base de datos PostgreSQL.
"""

from .carga import LoadData

__all__ = ["LoadData"]

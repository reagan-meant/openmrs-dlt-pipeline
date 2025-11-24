"""
Transform flatten module - flattened analytical tables
"""
from .observations import create_flattened_observations, incremental_flattened_observations
from .appointments import create_flattened_appointments, incremental_flattened_appointments
from .patient_programs import create_flattened_patient_program

__all__ = [
    'create_flattened_observations',
    'incremental_flattened_observations',
    'create_flattened_appointments',
    'incremental_flattened_appointments',
    'create_flattened_patient_program',
]

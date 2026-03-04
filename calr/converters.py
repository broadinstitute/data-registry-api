"""
Standard CalR format definition and conversion utilities.
"""
from dataclasses import dataclass
from typing import List

@dataclass
class CalRFormat:
    """
    Standard CalR format column specification.
    
    All converters should produce DataFrames with these columns.
    """
    
    # Required columns
    SUBJECT_ID = 'subject.id'
    SUBJECT_MASS = 'subject.mass'
    CAGE = 'cage'
    DATE_TIME = 'Date.Time'
    
    # Metabolic variables
    VO2 = 'vo2'
    VCO2 = 'vco2'
    EE = 'ee'
    EE_ACC = 'ee.acc'
    RER = 'rer'
    
    # Feeding/drinking
    FEED = 'feed'
    FEED_ACC = 'feed.acc'
    DRINK = 'drink'
    DRINK_ACC = 'drink.acc'
    
    # Locomotion
    XYTOT = 'xytot'
    XYAMB = 'xyamb'
    
    # Wheel
    WHEEL = 'wheel'
    WHEEL_ACC = 'wheel.acc'
    
    # Other sensors
    PEDMETER = 'pedmeter'
    ALLMETER = 'allmeter'
    BODY_TEMP = 'body.temp'
    
    # Time bins
    MINUTE = 'minute'
    HOUR = 'hour'
    DAY = 'day'
    
    # Experimental time offsets
    EXP_MINUTE = 'exp.minute'
    EXP_HOUR = 'exp.hour'
    EXP_DAY = 'exp.day'
    
    @classmethod
    def all_columns(cls) -> List[str]:
        """Return list of all standard CalR column names."""
        return [
            cls.SUBJECT_ID, cls.SUBJECT_MASS, cls.CAGE, cls.DATE_TIME,
            cls.VO2, cls.VCO2, cls.EE, cls.EE_ACC, cls.RER,
            cls.FEED, cls.FEED_ACC, cls.DRINK, cls.DRINK_ACC,
            cls.XYTOT, cls.XYAMB,
            cls.WHEEL, cls.WHEEL_ACC,
            cls.PEDMETER, cls.ALLMETER, cls.BODY_TEMP,
            cls.MINUTE, cls.HOUR, cls.DAY,
            cls.EXP_MINUTE, cls.EXP_HOUR, cls.EXP_DAY
        ]

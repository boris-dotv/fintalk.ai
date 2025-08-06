from typing import Text
from dm import weather
from dm import music 
from dm import maps 

PARSER_MAPPING = {
    "weather": weather,
    "maps": maps,
    "music": music
}

class DMFactory:
    """
    build dm instance by name.
    """

    @staticmethod
    def get(name: Text):

        if name in PARSER_MAPPING:
            return PARSER_MAPPING[name].process
        else:
            return None


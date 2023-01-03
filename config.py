from configparser import ConfigParser
from typing import Dict

class Config():
    def __init__(self, filename : str):
        self.filename : str = filename
        # create a parser
        self.parser = self._setupParser()
    
    def _setupParser(self):
        parser = ConfigParser()
        # read config file
        parser.read(self.filename)
        return parser
    
    def parse_section(self, section : str) -> Dict[str, str]:
        out = {}
        if self.parser.has_section(section):
            params = self.parser.items(section)
            for param in params:
                out[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, self.filename))
        return out

    def config_db(self, section : str ='postgresql') -> Dict[str, str]:
        return self._parse_section(section)

    def config_reader(self, section : str = 'reader') -> Dict[str, str]:
        return self._parse_section(section = section)
        

    
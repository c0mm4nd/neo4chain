import logging
import os
from hexbytes import HexBytes

logger = logging.getLogger(__name__)

def hex_to_int(n):
    if type(n) is int:
        return int
    elif type(n) is str:
        if len(n) > 2 and n[0:2] == '0x':
            return int(n, 0)
        else:
            return int(n, 16)
    elif type(n) is HexBytes:
        _n = n.hex()
        if len(_n) > 2 and _n[0:2] == '0x':
            return int(_n, 0)
        else:
            return int(_n, 16)
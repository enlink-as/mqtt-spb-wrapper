import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))


def test_simple_import():
    from mqtt_spb_wrapper.spb_base import SpbEntity
    assert SpbEntity is not None

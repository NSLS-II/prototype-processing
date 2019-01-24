import time
import copy
import numpy
import logging

# DEFINE DATABASE

from databroker import Broker
from ophyd.sim import NumpySeqHandler

raw = Broker.named('temp')
raw.reg.register_handler("NPY_SEQ", NumpySeqHandler)
# processed.reg.register_handler("NPY_SEQ", NumpySeqHandler)
raw.prepare_hook = lambda name, doc: copy.deepcopy(doc)

# ACQUISITION WITH LIVE PROCESSING

from bluesky import RunEngine
from ophyd.sim import img, motor
from bluesky.plans import scan
from bluesky.callbacks.zmq import Publisher

RE = RunEngine({})
RE.subscribe(raw.insert)
publisher = Publisher('localhost:5577', prefix=b'raw')
RE.subscribe(publisher)

uid, = RE(scan([img], motor, -1, 1, 3))

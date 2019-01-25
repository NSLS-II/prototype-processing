import time
import copy
from collections import namedtuple
import numpy
import logging

logger = logging.getLogger('iss_processor')

from databroker import Broker
from ophyd.sim import NumpySeqHandler

processed = Broker.named('temp')  # makes a second, unique temporary Broker

# PROCESSING

import sys
import xas.interpolate
from event_model import Filler, compose_run, DocumentRouter, pack_event_page, verify_filled
from bluesky.callbacks.zmq import RemoteDispatcher, Publisher

publisher = Publisher('localhost:5577', prefix=b'interpolated')


# HANDLERS COPIED FROM PROFILE -- MOVE THESE TO CENTRAL PLACE!!

class PizzaBoxEncHandlerTxt:
    encoder_row = namedtuple('encoder_row',
                             ['ts_s', 'ts_ns', 'encoder', 'index', 'state'])
    "Read PizzaBox text files using info from filestore."
    def __init__(self, fpath, chunk_size):
        self.chunk_size = chunk_size
        with open(fpath, 'r') as f:
            self.lines = list(f)

    def __call__(self, chunk_num):
        cs = self.chunk_size
        return [self.encoder_row(*(int(v) for v in ln.split()))
                for ln in self.lines[chunk_num*cs:(chunk_num+1)*cs]]


class PizzaBoxDIHandlerTxt:
    di_row = namedtuple('di_row', ['ts_s', 'ts_ns', 'encoder', 'index', 'di'])
    "Read PizzaBox text files using info from filestore."
    def __init__(self, fpath, chunk_size):
        self.chunk_size = chunk_size
        with open(fpath, 'r') as f:
            self.lines = list(f)

    def __call__(self, chunk_num):
        cs = self.chunk_size
        return [self.di_row(*(int(v) for v in ln.split()))
                for ln in self.lines[chunk_num*cs:(chunk_num+1)*cs]]


class PizzaBoxAnHandlerTxt:
    encoder_row = namedtuple('encoder_row', ['ts_s', 'ts_ns', 'index', 'adc'])
    "Read PizzaBox text files using info from filestore."

    bases = (10, 10, 10, 16)
    def __init__(self, fpath, chunk_size):
        self.chunk_size = chunk_size
        with open(fpath, 'r') as f:
            self.lines = list(f)

    def __call__(self, chunk_num):

        cs = self.chunk_size
        return [self.encoder_row(*(int(v, base=b) for v, b in zip(ln.split(), self.bases)))
                for ln in self.lines[chunk_num*cs:(chunk_num+1)*cs]]

handler_registry = {'PIZZABOX_AN_FILE_TXT': PizzaBoxAnHandlerTxt,
                    'PIZZABOX_ENC_FILE_TXT': PizzaBoxEncHandlerTxt,
                    'PIZZABOX_DI_FILE_TXT': PizzaBoxDIHandlerTxt}

def is_applicable(start_doc):
    ...
    return True
    # return True or False


def my_analysis_function(arr, factor):
    return factor * numpy.sum(arr)


class Interpolator(Filler):
    version = xas.__version__
    
    def __call__(self, name, doc):
        _, processed_doc = super().__call__(name, doc)

    def start(self, doc):
        doc = super().start(doc)
        metadata = {'raw_uid': doc['uid'],
                    'processor_version': self.version,
                    'processor_parameters': {}}
        self.compose_run_bundle = compose_run(metadata=metadata)
        return self.compose_run_bundle.start_doc

    def descriptor(self, doc):
        doc = super().descriptor(doc)
        name = 'primary'
        data_keys = {'sum': {'shape': [], 'dtype': 'number', 'source': repr(self)}}
        self.compose_descriptor_bundle = self.compose_run_bundle.compose_descriptor(
                name=name, data_keys=data_keys,
                object_names=None, configuration={}, hints=None)
        return self.compose_descriptor_bundle.descriptor_doc
        
    def event_page(self, doc):
        doc = super().event_page(doc)
        verify_filled(doc)
        key, = doc['data']
        print('event_page doc', doc)
        result = [xas.interpolate.interpolate(dataframe) for dataframe in doc['data'][key]]
        event_page = self.compose_descriptor_bundle.compose_event_page(
            data={'sum': result},
            timestamps={'sum': [time.time()] * len(result)},
            seq_num=doc['seq_num'],
            validate=False)  # FIXME
        return event_page

    def stop(self, doc):
        doc = super().stop(doc)
        return self.compose_run_bundle.compose_stop()


dispatcher = RemoteDispatcher('localhost:5578', prefix=b'raw')

if __name__ == '__main__':
    dispatcher.subscribe(Interpolator(handler_registry))
    dispatcher.start()

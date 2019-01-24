import time
import copy
import numpy
import logging

logger = logging.getLogger('iss_processor')
logging.basicConfig(level='DEBUG')


from databroker import Broker
from ophyd.sim import NumpySeqHandler

processed = Broker.named('temp')  # makes a second, unique temporary Broker

# PROCESSING

import sys
from event_model import Filler, compose_run, DocumentRouter, pack_event_page
from bluesky.callbacks.zmq import RemoteDispatcher, Publisher

publisher = Publisher('localhost:5577', prefix=b'interpolated')

def is_applicable(start_doc):
    ...
    return True
    # return True or False


def my_analysis_function(arr, factor):
    return factor * numpy.sum(arr)


class Processor(DocumentRouter):
    version = 1
    
    def __init__(self, factor):
        self.factor = factor

    def start(self, doc):
        metadata = {'raw_uid': doc['uid'],
                    'processor_version': self.version,
                    'processor_parameters': {'factor': self.factor}}
        self.compose_run_bundle = compose_run(metadata=metadata)
        return self.compose_run_bundle.start_doc

    def descriptor(self, doc):
        name = 'primary'
        data_keys = {'sum': {'shape': [], 'dtype': 'number', 'source': repr(self)}}
        self.compose_descriptor_bundle = self.compose_run_bundle.compose_descriptor(
                name=name, data_keys=data_keys,
                object_names=None, configuration={}, hints=None)
        return self.compose_descriptor_bundle.descriptor_doc
        
    def event_page(self, doc):
        result = [my_analysis_function(img, self.factor) for img in doc['data']['img']]
        event_doc = self.compose_descriptor_bundle.compose_event_page(
                data={'sum': result},
                timestamps={'sum': [time.time()] * len(result)},
                seq_num=doc['seq_num'],
                validate=False)  # FIXME
        return event_doc

    def stop(self, doc):
        return self.compose_run_bundle.compose_stop()


class LiveProcessor(DocumentRouter):
    def __init__(self, factor):
        self.factor = factor

    def __call__(self, name, doc):
        if name == 'start':
            return self.start(doc)
        if not self.applicable:
            return
        _, filled_doc = self.filler(name, doc)
        _, processed_doc = self.processor(name, filled_doc)
        print(processed_doc)
        processed_doc.pop('id', None)
        processed.insert(name, processed_doc)
        publisher(name, processed_doc)

    def start(self, doc):
        self.filler = Filler({"NPY_SEQ": NumpySeqHandler})
        self.applicable = is_applicable(doc)
        self.processor = Processor(factor=self.factor)
        _, processed_doc = self.processor('start', doc)
        processed.insert('start', processed_doc)
        publisher('start', processed_doc)



dispatcher = RemoteDispatcher('localhost:5578', prefix=b'raw')

if __name__ == '__main__':
    dispatcher.subscribe(LiveProcessor(factor=float(sys.argv[1])))
    dispatcher.start()

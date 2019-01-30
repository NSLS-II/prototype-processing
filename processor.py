import time
import copy
from collections import namedtuple
import numpy
import logging
import pandas as pd

logger = logging.getLogger('iss_processor')

from databroker import Broker
from ophyd.sim import NumpySeqHandler

processed = Broker.named('temp')  # makes a second, unique temporary Broker

# PROCESSING

import sys
import xas.interpolate
import xas.xray
from event_model import Filler, compose_run, DocumentRouter, pack_event_page, verify_filled
from bluesky.callbacks.zmq import RemoteDispatcher, Publisher

publisher = Publisher('localhost:5577', prefix=b'interpolated')


# HANDLERS COPIED FROM PROFILE -- MOVE THESE TO CENTRAL PLACE!!

# New handlers to support reading files into a Pandas dataframe
class PizzaBoxAnHandlerTxtPD:
    "Read PizzaBox text files using info from filestore."
    def __init__(self, fpath):
        self.df = pd.read_table(fpath, names=['ts_s', 'ts_ns', 'index', 'adc'], sep=' ')

    def __call__(self):
        return self.df

class PizzaBoxDIHandlerTxtPD:
    "Read PizzaBox text files using info from filestore."
    def __init__(self, fpath):
        self.df = pd.read_table(fpath, names=['ts_s', 'ts_ns', 'encoder', 'index', 'di'], sep=' ')

    def __call__(self):
        return self.df

class PizzaBoxEncHandlerTxtPD:
    "Read PizzaBox text files using info from filestore."
    def __init__(self, fpath):
        self.df = pd.read_table(fpath, names=['ts_s', 'ts_ns', 'encoder', 'index', 'state'], sep=' ')

    def __call__(self):
        return self.df


handler_registry = {'PIZZABOX_AN_FILE_TXT_PD': PizzaBoxAnHandlerTxtPD,
                    'PIZZABOX_ENC_FILE_TXT_PD': PizzaBoxEncHandlerTxtPD,
                    'PIZZABOX_DI_FILE_TXT_PD': PizzaBoxDIHandlerTxtPD}

def is_applicable(start_doc):
    ...
    return True
    # return True or False


def my_analysis_function(arr, factor):
    return factor * numpy.sum(arr)


class Interpolator(Filler):
    version = xas.__version__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_doc = None
        self.descriptor_doc = None
        self._preprocessed_data = {}

    def __call__(self, name, doc):
        _, processed_doc = super().__call__(name, doc)

        if name == 'stop':
            interpolated_data = xas.interpolate.interpolate(self._preprocessed_data)
            self._preprocessed_data = {}
            event_page = self.compose_descriptor_bundle.compose_event_page(
                 data={'interpolated_data': interpolated_data},
                 timestamps={'interpolated_data': [time.time()] * len(interpolated_data)},
                 seq_num=doc['seq_num'],
                 validate=False)  # FIXME
            return event_page

        elif name == 'event_page':
            ...

        else:
            processed.insert(name, processed_doc)  # send result to "processed databroker"
            publisher(name, processed_doc)  # send result back to GUI via 0MQ, where it will kick off the next analysis steps

    def start(self, doc):
        doc = super().start(doc)
        self.start_doc = doc
        metadata = {'raw_uid': doc['uid'],
                    'processor_version': self.version,
                    'processor_parameters': {}}
        self.compose_run_bundle = compose_run(metadata=metadata)
        return self.compose_run_bundle.start_doc

    def descriptor(self, doc):
        doc = super().descriptor(doc)
        self.descriptor_doc = doc
        name = 'primary'
        data_keys = {'interpolated_data': {'shape': [], 'dtype': 'number', 'source': repr(self)}}
        self.compose_descriptor_bundle = self.compose_run_bundle.compose_descriptor(
                name=name, data_keys=data_keys,
                object_names=None, configuration={}, hints=None)
        return self.compose_descriptor_bundle.descriptor_doc
        
    def event_page(self, doc):
        doc = super().event_page(doc)
        verify_filled(doc)
        stream_name, = doc['data']
        dev_name = self.descriptor_doc['data_keys'][stream_name]['devname']

        raw_data, = doc['data'][stream_name]

        def load_adc_trace(df_raw):
            df = pd.DataFrame()
            df['timestamp'] = df_raw['ts_s'] + 1e-9 * df_raw['ts_ns']
            df['adc'] = df_raw['adc'].apply(lambda x: (int(x, 16) >> 8) - 0x40000 if (int(x, 16) >> 8) > 0x1FFFF else int(x, 16) >> 8) * 7.62939453125e-05
            return df

        def load_enc_trace(df_raw):
            df = pd.DataFrame()
            df['timestamp'] = df_raw['ts_s'] + 1e-9 * df_raw['ts_ns']
            df['encoder'] = df_raw['encoder'].apply(lambda x: int(x) if int(x) <= 0 else -(int(x) ^ 0xffffff - 1))
            return df

        def load_trig_trace(df):
            df['timestamp'] = df['ts_s'] + 1e-9 * df['ts_ns']
            df = df.iloc[::2]
            return df.iloc[:, [5, 3]]

        data = pd.DataFrame()
        stream_source = self.descriptor_doc['data_keys'][stream_name]['source']

        if stream_source == 'pizzabox-di-file':
            data = load_trig_trace(raw_data)

        if stream_source == 'pizzabox-adc-file':
            data = load_adc_trace(raw_data)
            stream_offset = f'{stream_name} offset'
            if stream_offset in self.start_doc:
                data.iloc[:, 1] -= self.start_doc[stream_offset]
            stream_gain = f'{stream_name} gain'
            if stream_gain in self.start_doc:
                data.iloc[:, 1] /= 10**self.start_doc[stream_gain]

        if stream_source == 'pizzabox-enc-file':
            data = load_enc_trace(raw_data)
            if dev_name == 'hhm_theta':
                data.iloc[:, 1] = xas.xray.encoder2energy(data['encoder'], 360000, -float(self.start_doc['angle_offset']))
                dev_name = 'energy'

        self._preprocessed_data[dev_name] = data


dispatcher = RemoteDispatcher('localhost:5578', prefix=b'raw')

if __name__ == '__main__':
    dispatcher.subscribe(Interpolator(handler_registry))
    dispatcher.start()

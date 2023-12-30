import json
import time
import logging
from pprint import pformat
from pathlib import Path

import requests
from esridump.dumper import EsriDumper, DumperState
from .utils import mark_as_done


logger = logging.getLogger(__name__)

def scrape_endpoint(data_folder, url,
                    params, svc,
                    layer_params_map,
                    whitelist, blacklist,
                    post_processing_func=mark_as_done,
                    post_processing_func_args={},
                    ignore_layer_types=['Raster Layer']):

    """A function to scrape layers in a service.

    Parameters
    ----------
      data_folder: str
        The folder to save data in.


      url: str
        The url of the service endpoint to scrape.

        Example:
          https://arc.indiawris.gov.in/server/rest/services/Common/Administrative_NWIC/MapServer


      params: dict
        Settings used while scraping all the layers of this service.

        Look at EsriDumper constructor arguments for possible values.


      svc: str
        Name of the service to scrape, used for creating the data sub folder structure.


      layer_params_map: dict
        A dictionary of layer scraping settings overrides per layer,
        these override the default settings used to scrape a layer using esridump.

        Look at EsriDumper constructor arguments for possible values.


      whitelist: list
        A dictionary entry whose entries specify which layers of the service should be scraped.

        Layer names are suffixed with the _<layer_id> to avoid name clashes. 

        layer_id can be obtained by visiting the parent service folder web page on the base_url
        or can be obtained by running the get_all_info() function from explore.py
        and looking at the data in the all_layer_list.jsonl file.

        If the dictionary is empty, all layers in the service endpoint are pulled.


      blacklist: list
        layers which need to be excluded from scraping.

        Layer names are suffixed with the _<layer_id> to avoid name clashes.


      post_processing_func: function
        After all the data is downloaded/attempted, this function is called.

        It returns false if it was not successful download or an ignore.
        ( return code is currently unused.. I don't rememeber why I had it in the first place :( )

        It takes the data folder, the layer file, the layer status file as mandatory args

        default: util.mark_as_done
          This function just sets the state in state file to 'done',

          if the current state in state file is 'downloaded'.


      post_processing_func_args: dict
        Extra args to be used while invoking post_processing_func.
        default: {}


      ignore_layer_types: list
        Types of layers to ignore for downloading.

        Layer type is obtained from a call to the metadata api of the layer.

        default: [ 'Raster Layer' ]
    """

    logger.info(f'handling mapserver {url}')
    svc_folder = data_folder / Path(svc)
    layers_list_file = svc_folder / 'layers_list.json'
    if layers_list_file.exists():
        with open(layers_list_file, 'r') as f:
            all_layers = json.load(f)
    else:
        list_layer_params = {}
        list_layer_params.update(params)
        list_dumper = EsriDumper(url, **list_layer_params)
        list_query_args = list_dumper._build_query_args({
            'f': 'json',
        })
        list_headers = list_dumper._build_headers()
        list_url = list_dumper._build_url()
        response = list_dumper._request('GET', list_url, params=list_query_args, headers=list_headers)
        metadata = list_dumper._handle_esri_errors(response, "Could not retrieve layer metadata")
        logger.debug(f'metadata={pformat(metadata)}')

        all_layers = metadata.get('layers', [])
        svc_folder.mkdir(parents=True, exist_ok=True)
        with open(layers_list_file, 'w') as f:
            json.dump(all_layers, f)

    layer_map = {}
    for layer in all_layers:
        layer_map[layer['id']] = layer

    def get_full_layer_name(layer):
        parts = []
        curr = layer
        while True:
            parts.append(curr['name'])
            parent_id = curr['parentLayerId']
            if parent_id == -1:
                break
            curr = layer_map[parent_id]
        parts.reverse()
        parts[-1] = parts[-1] + '_' + str(layer['id'])
        return "/".join(parts)

    for layer in all_layers:
        logger.debug(f'layer={pformat(layer)}')
        layer_id = layer['id']
        layer_name = get_full_layer_name(layer)
        if layer_name in blacklist:
            continue
        if whitelist is not None and layer_name not in whitelist:
            continue

        layer_file = svc_folder / f'{layer_name}.geojsonl'
        logger.info(f'handling {layer_name}:{layer_id}')

        Path(layer_file).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f'handling {layer_name}:{layer_id} at {layer_file}')
        layer_file_status = Path(str(layer_file) + '.status') 
        if not layer_file_status.exists():
            layer_file_status.write_text('wip')
        else:
            completed = post_processing_func(data_folder, layer_file, layer_file_status, **post_processing_func_args)
            if completed:
                continue

        layer_url = f'{url}/{layer_id}'
        logger.info(f'server layer url: {layer_url}')

        layer_params = {}
        layer_params.update(params)
        layer_specific_params = layer_params_map.get(layer_name, {})
        layer_params.update(layer_specific_params)

        dumper = EsriDumper(f'{url}/{layer_id}', **layer_params)
        metadata = dumper.get_metadata()
        sub_layers = metadata.get('subLayers', None)
        if sub_layers is not None and len(sub_layers) > 0:
            layer_file_status.write_text('not_layer')
            continue

        layer_type = layer.get('type', None)
        if layer_type in ignore_layer_types:
            layer_file_status.write_text('ignore')
            continue

        prev_state = None
        layer_file_state = Path(str(layer_file) + '.state') 
        if layer_file_state.exists():
            prev_state = DumperState.decode(layer_file_state.read_text().strip())
            logger.info(f'Previous state: {prev_state}')
        dumper = EsriDumper(layer_url,
                            state=prev_state,
                            update_state=True,
                            **layer_params)

        feature_iter = iter(dumper)
        with open(layer_file, 'a', encoding='utf8') as f:
            try:
                for feature in feature_iter:
                    line = json.dumps(feature, ensure_ascii=False) + '\n'
                    f.write(line)
            except:
                logger.info('saving state file')
                layer_file_state.parent.mkdir(exist_ok=True, parents=True)
                layer_file_state.write_text(dumper._state.encode())
                logger.info('Done saving state file')
                raise

        layer_file_status.write_text('downloaded')
        if post_processing_func is not None:
            post_processing_func(data_folder, layer_file, layer_file_status, **post_processing_func_args)



def scrape_map_servers(data_folder='data/',
                       base_url=None,
                       base_params={},
                       to_scrape={},
                       blacklist={},
                       **kwargs):

    """A function to scrape a selection of services/layers 
    specified by to_scrape from a arcgis server specified by base_url
    and save it to the directory specified by data_folder

    Parameters
    ----------
      data_folder: str
        The folder to save data in

        default: 'data/'


      base_url: str
        The url of the esri rest services base url to scrape.

        Example:
          https://arc.indiawris.gov.in/server/rest/services


      base_params: dict
        Params used while scraping all the services/layers specified in 'to_scrape'.

        Look at EsriDumper onstructor arguments for possible values


      to_scrape: dict
        The map of services/layers to be scraped.

        All services not specified in the keys in to_scrape are ignored.
        The service type( like MapServer ) is part of the name.

        Each entry for a service key maps 'to_scrape' arg dict can optionally have entries with a 'whitelist' and a 'params' key.
          If a 'whitelist' entry is missing in the dict provided for a service, all layers in the service are downloaded.

          A 'params' entry is itself a dictionary that for all the layers in this service 
          overrides the settings supplied for scraping layers with the 'base_params' argument.

          The 'whitelist' entry in the value for a service specifies which layers of the service should be scraped.
          Layer names are suffixed with the _<layer_id> to avoid name clashes. 

          layer_id can be obtained by visiting the parent service folder web page on the base_url.
          or can be obtained by running the get_all_info() function from explore.py and looking at the data in the all_layer_list.jsonl.

          A missing 'whitelist' entry means all the layers from the service will be scraped.

          A layer_params_map can be specified to override the params passed to the invocation of scrape_endpoint for a given layer.
          Look at EsriDumper onstructor arguments for possible values.

        Example: 
        {
          'Admin/Administrative_NWIC/MapServer': {
              'params': {
                   'max_page_size': 10
              }
          },
          'Common/Administrative_WRP/MapServer': {
              'whitelist': [
                  'State Capitals_0'
              ],
              'layer_params_map': {
                  'State Capitals_0': {
                      'max_page_size': 1
                  }
              }
          }
        }
        In the above example, everything in the 'Admin/Administrative_NWIC/MapServer' will get scraped and
        only the 'State Capitals' layer (with layer id 0) will be scraped.

        And while scraping the 'State Capitals' layer the 'max_page_size' param will be overriden to 1.

        And for layers under 'Admin/Administrative_NWIC/MapServer' the 'max_page_size' param of 10 is used while scraping them.


      blacklist: dict
        Layers which need to be excluded from scraping.

        It is a dict keyed with the service name with list of layers in the service as values.
        The service type( like MapServer ) is part of the name.

        Layer names are suffixed with the _<layer_id> to avoid name clashes. 
        layer_id can be obtained by visiting the parent service folder web page on the base_url.
        or can be obtained by running the get_all_info() function from explore.py and looking at the data in the all_layer_list.jsonl.

        if a value for a service in the dictionary is None, the whole service is blacklisted

        Example:
        {
            'Common/Administrative_NWIC/MapServer': [
                'Village_6'
            ],
            'Common/Administrative_NWIC_other/MapServer': None
        }

        In the above example 'Village_6' layer of 'Common/Administrative_NWIC/MapServer' will not be scraped,
        and all of layers of 'Common/Administrative_NWIC_other/MapServer' will not be scraped


      **kwargs: 
        Extra args to be passed on to scrape_endpoint function.

    """

    for svc, info in to_scrape.items():
        svc_url = f'{base_url}/{svc}'
        svc_whitelist = info.get('whitelist', None)
        svc_blacklist = blacklist.get(svc, [])
        if svc_blacklist is None:
            continue
        server_params = info.get('params', {})
        params = {}
        params.update(base_params)
        params.update(server_params)
        layer_params_map = info.get('layer_params_map', {})
        scrape_endpoint(data_folder, svc_url, params, svc,
                        layer_params_map,
                        svc_whitelist, svc_blacklist,
                        **kwargs)
    return True


def scrape_map_servers_wrap(delay=5.0,
                            max_delay=900.0,
                            **kwargs):
    """A wrapper around scrape_map_servers to retry on failure with increasing delay.

    Parameters
    ----------
      delay: float
        The initial delay in seconds and also the amount with which the delay increases.

        default: 5.0


      max_delay: float
        The most delay allowed between two attempts.

        default: 300.0


      **kwargs:
        args to be passed on to scrape_map_servers function.
    """

    attempt = 0

    while True:
        to_delay = delay * attempt
        if to_delay > max_delay:
            to_delay = max_delay
        logger.info(f'sleeping for {to_delay} secs before next attempt')
        time.sleep(to_delay)
        attempt += 1
        try:
            done = scrape_map_servers(**kwargs)
            if done:
                logger.info('All Done')
                return
        except Exception:
            logger.exception(f'{attempt=} to scrape failed')

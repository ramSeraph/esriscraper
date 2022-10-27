import json
import time
import logging
from pprint import pformat
from pathlib import Path

import requests
from esridump.dumper import EsriDumper, DumperState


logger = logging.getLogger(__name__)


def scrape_endpoint(data_folder, url,
                    params, svc,
                    layer_params_map,
                    whitelist, blacklist,
                    post_processing_func=None,
                    post_processing_func_args={},
                    ignore_layer_types=['Raster Layer']):
    logger.info(f'handling mapserver {url}')
    server_folder = data_folder / Path(svc)
    layers_list_file = server_folder / 'layers_list.json'
    if layers_list_file.exists():
        with open(layers_list_file, 'r') as f:
            all_layers = json.load(f)
    else:
        resp = requests.get(url, params={'f': 'json'})
        metadata = json.loads(resp.text)
        logger.debug(f'metadata={pformat(metadata)}')

        all_layers = metadata.get('layers', [])
        server_folder.mkdir(parents=True, exist_ok=True)
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

        layer_file = server_folder / f'{layer_name}.geojsonl'
        logger.info(f'handling {layer_name}:{layer_id}')

        Path(layer_file).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f'handling {layer_name}:{layer_id} at {layer_file}')
        layer_file_status = Path(str(layer_file) + '.status') 
        if not layer_file_status.exists():
            layer_file_status.write_text('wip')
        else:
            completed = post_processing_func(layer_file, layer_file_status, **post_processing_func_args)
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

        saved = []
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
            post_processing_func(layer_file, layer_file_status, **post_processing_func_args)
                


def scrape_map_servers(**kwargs):
    data_folder = kwargs.pop('data_folder')
    base_url = kwargs.pop('base_url')
    map_svcs = kwargs.pop('to_scrape')
    blacklist = kwargs.pop('blacklist', {})
    base_params = kwargs.pop('base_params', {})
    for svc, info in map_svcs.items():
        svc_url = f'{base_url}/{svc}'
        svc_whitelist = info.get('whitelist', None)
        svc_blacklist = blacklist.get(svc, [])
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


def scrape_map_servers_wrap(**kwargs):
    attempt = 0

    max_delay = kwargs.pop('max_delay')
    delay = kwargs.pop('delay')
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

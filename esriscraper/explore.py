import json
import copy
import logging
from pprint import pformat

from esridump.dumper import EsriDumper
from esridump.errors import EsriDownloadError

logger = logging.getLogger(__name__)

def get_info(sub_url, dumper):
    url = dumper._build_url(sub_url)
    logger.info(f'getting info for {url}')
    query_args = dumper._build_query_args({ 'f': 'json' })
    headers = dumper._build_headers()
    response = dumper._request('GET', url, params=query_args, headers=headers)
    info = dumper._handle_esri_errors(response, "Could not retrieve info")
    return info


def get_all_info(main_url, base_params, analysis_folder,
                 blacklist={},
                 folder_blacklist=[],
                 interested_server_types=['MapServer', 'FeatureServer']):
    """
    Dump information of all the available layers at service endpoint into 
    a line delimited json file.

    Parameters:
    ----------
      main_url: str
        The url of the esri rest services base url to scrape.

        Example:
          https://arc.indiawris.gov.in/server/rest/services


      base_params: dict
        Params used while scraping the metadata of services/layers

        Look at EsriDumper constructor arguments for possible values

      blacklist: dict
        Layers which need to be excluded from exploring.
        This is mostly done to avoid service/layers which break the tool

        It is passed as dict with keys mapped with service names and values as the list of layers
        The service type( like MapServer ) is part of the name.

        Layer names are suffixed with the _<layer_id> to avoid name clashes. 
        layer_id can be obtained by visiting the parent service folder web page on the base_url.

        If the value provided for a service is None, then all the layers in the service are ignored

        Example:
        {
            'Common/Administrative_NWIC/MapServer': [
                'Village_6'
            ]
        }


        Example:
        {
            'Common/Administrative_NWIC/MapServer': [
                'Village_6'
            ],
            'Common/Administrative_NWIC_other/MapServer': None
        }

        In the above example 'Village_6' layer of 'Common/Administrative_NWIC/MapServer' will not be scraped,
        and all of layers of 'Common/Administrative_NWIC_other/MapServer' will not be explored


      folder_blacklist: list
        Folders( logical grouping of services ) which need to be ignored for exploring.
        This is usually done to ignore folders that break the tool

        Example:
        [
            "bharatmaps",
            "panchayat",
            "svamitva",
            "Utilities"
        ]
    """
    meta_dumper = EsriDumper(main_url, **base_params)
    full_folder_map = { '' : False }
    full_services_map = {}

    full_services_map_file = analysis_folder / 'full_services_list.txt'
    if full_services_map_file.exists():
        logger.info(f'found existing services file at {full_services_map_file}')
        with open(full_services_map_file, 'r') as f:
            for line in f:
                line = line.strip('\n')
                full_services_map[line] = True
 
    while True:
        folder_map = copy.deepcopy(full_folder_map)
        for folder, val in folder_map.items():
            if val:
                continue
            logger.info(f'querying {folder}')
            info = get_info(f'/{folder}', meta_dumper)
            logger.debug(f'info={pformat(info)}')
            full_folder_map[folder] = True
            if info.get('error', None) != None:
                continue
            new_folders = info['folders']
            new_services = info['services']
            new_folders = [ folder + '/' + f for f in new_folders ]
            for f in new_folders:
                if f.startswith('/'):
                    f = f[1:]
                if f in folder_blacklist:
                    continue
                full_folder_map[f] = False
            for s in new_services:
                s_name = s['name']
                s_type = s['type']
                if s['type'] in interested_server_types:
                    s_full_name = f'{s_name}/{s_type}'
                    if s_full_name in blacklist and blacklist[s_full_name] is None:
                        continue
                    if s_full_name not in full_services_map:
                        full_services_map[s_full_name] = False
        if all([v != False for v in full_folder_map.values()]):
            break


    all_layer_list = []
    all_layer_map = {}
    all_layer_list_file = analysis_folder / 'all_layer_list.jsonl'
    if all_layer_list_file.exists():
        with open(all_layer_list_file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                e = json.loads(line)
                all_layer_list.append(e)
                all_layer_map = {(e['name'], e['id']):e for e in all_layer_list}
           
    full_services_map_file.parent.mkdir(exist_ok=True, parents=True)
    with open(full_services_map_file, 'w') as sfp:
        with open(all_layer_list_file, 'a') as f:
            for service, val in full_services_map.items():
                if val:
                    sfp.write(f'{service}\n')
                    sfp.flush()
                    continue
                svc_blacklist = blacklist.get(service, [])
                if svc_blacklist is None:
                    continue

                url = f'{main_url}/{service}'
                info = get_info(f'/{service}', meta_dumper)
                all_layers = info.get('layers', [])
                full_services_map[service] = True

                layer_map = {}
                for layer in all_layers:
                    layer_map[layer['id']] = layer

                def get_full_layer_name(layer):
                    parts = []
                    curr = layer
                    while True:
                        parts.append(curr['name'])
                        parent_id = curr.get('parentLayerId', -1)
                        if parent_id == -1:
                            break
                        curr = layer_map[parent_id]
                    parts.reverse()
                    return "/".join(parts)


                for layer in all_layers:
                    layer_id = layer['id']
                    sub_layers = layer.get('subLayerIds')
                    if sub_layers is not None and len(sub_layers) > 0:
                        continue
                    layer_name = get_full_layer_name(layer)
                    if f'{layer_name}_{layer_id}' in svc_blacklist:
                        continue
                    full_name = f'{service}/{layer_name}'
                    if (full_name, layer_id) in all_layer_map:
                        continue

                    url = f'{main_url}/{service}'
                    dumper = EsriDumper(f'{url}/{layer_id}', **base_params)
                    logger.info(f'getting full feature count for {full_name}, {layer_id}')
                    metadata = dumper.get_metadata()

                    fields = metadata.get('fields', None)
                    if fields is None:
                        fields = []
                    fnames = [ f['name'] for f in fields ]
                    try:
                        fcount = dumper.get_feature_count()
                    except EsriDownloadError as ex:
                        err_msgs = [
                            'Could not retrieve row count: Invalid or missing input parameters. ',
                            'Could not retrieve row count: Requested operation is not supported by this service. The requested capability is not supported.'
                        ]
                        if str(ex) in err_msgs:
                            logger.info(f'Unable to get feature count - {str(ex)}')
                            fcount = -1
                        else:
                            raise
                    entry = { 'name': full_name, 'id': layer_id, 'service': service, 'fcount': fcount, 'fnames': fnames }
                    all_layer_list.append(entry)
                    all_layer_map[(entry['name'], entry['id'])] = entry
                    f.write(json.dumps(entry) + '\n')
                    f.flush()
                sfp.write(f'{service}\n')
                sfp.flush()
    layer_count = len(all_layer_list)
    logger.info(f'{layer_count=}')
    return all_layer_list, all_layer_map

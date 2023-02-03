import json
import re
import copy
import logging

logger = logging.getLogger(__name__)

def already_done(data_folder):
    done_layers = []
    data_folder_name = str(data_folder)
    files = data_folder.glob('**/*.geojsonl.status')
    for file in files:
        status = file.read_text()
        if status in [ 'not_layer', 'raster_layer', 'wip' ]:
            continue
        if status != 'done':
            logger.error(file, status)
            raise Exception('unexpected')
        fname = str(file)
        fname = fname.replace(data_folder_name + '/', '')
        m = re.match(r'(.*)_([0-9]+)\.geojsonl\.status', fname)
        if m is None:
            raise Exception('unexpected {fname=}')
        
        layer_name = m.group(1)
        layer_id = int(m.group(2))
        done_layers.append((layer_name.lower(), layer_id))
    return done_layers


def expand_layers(layers):
    layers_expanded = {}
    for layer in layers:
        layers_expanded[layer] = []
        parts = layer[0].split('/')
        for i in range(len(parts)):
            layers_expanded[layer].append('/'.join(parts[i:]))
    return layers_expanded


def get_missing_layer_list(full_list, done_layers, match_ignore):
    black_list_map = match_ignore
    full_missing = []
    for e in full_list:
        if (e['name'].lower(), e['id']) in done_layers:
            continue
        service_name = e['service']
        no_go_list = black_list_map.get(service_name, [])
        if no_go_list is None:
            continue
        no_go_list = [ service_name + '/' + l for l in no_go_list ]
        if e['name'] in no_go_list:
            continue
        full_missing.append(e)
    return full_missing


def get_possible_matches(done_layers, done_layers_expanded, full_list_map, full_missing):
    possible_matches = {}
    for layer in done_layers:
        dfcount = full_list_map[layer]['fcount']
        logger.info(f'matching done layer: {layer}({dfcount})')
        #print(f'looking up possible matches for {layer}')
        suffixes = done_layers_expanded[layer]
        for suffix in suffixes:
            missing  = copy.deepcopy(full_missing)
            for i,e in enumerate(missing):
                lname = e['name'].lower()
                lid = e['id']
                lfcount = full_list_map[(lname, lid)]['fcount']
                #if lname.endswith(suffix) and dfcount == lfcount:
                if lname.endswith(suffix):
                    if dfcount != lfcount:
                        logger.info(f'done layer: {layer}({dfcount}), missed layer: {lname}({lfcount})')
                        continue
                    if lname not in possible_matches:
                        possible_matches[lname] = []
                    possible_matches[lname].append((layer, suffix))

    possible_best_matches = {}
    for lname, matches in possible_matches.items():
        matches.sort(key=lambda t: len(t[1]), reverse=True)
        best_match = matches[0][0]
        possible_best_matches[lname] = best_match
    return possible_best_matches


def prune_missing(full_missing, matched_set, known_matches):
    missing = []
    for e in full_missing:
        if e['name'].lower() in matched_set:
            continue
        if e['name'] in known_matches:
            continue
        missing.append(e)
    return missing


def read_all_layer_info(analysis_folder):
    all_layer_list = []
    all_layer_map = {}
    all_layer_list_file = analysis_folder / 'all_layer_list.jsonl'

    with open(all_layer_list_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            e = json.loads(line)
            all_layer_list.append(e)
            all_layer_map = {(e['name'].lower(), e['id']):e for e in all_layer_list}
    return all_layer_list, all_layer_map
 

def run_checks(data_folder, analysis_folder, match_ignore={}, known_matches={}):

    """
    Function to check if there are duplicates in the layers already downloaded
    and the layers still undownloaded as known from the master list obtained
    from running explore on the service endpoint( located at analysis_folder/all_layer_list.jsonl ).

    Two output file get written:
      analysis_folder/matches.json
        file containing the matches found in the undownloaded layers from the already downloaded data.
        data is keyed with the downloaded layer names.

      analysis_folder/need_to_check.json
        file containing the undownloaded layers which didn't match any of the downloaded layers

    This is in no way foolproof. 
    It checks if the names after removing the endpoint point types( MapServer, FeatureServer etc ) 
    have suffix matches and when there are suffix matches if the feature counts match too.. 
    In case of a siffix and feature count match it considers it a duplicate

    Parameters
    ----------
        data_folder: pathlib.Path
          folder where scrape functionality has downlaoded the data to

        analysis_folder: pathlib.Path
          folder where explore functionality has downlaoded the layer list to

        match_ignore: dict
          Layers to ignore whicle searching for matches.

          The keys in the dict are service names,
          the service type( like MapServer ) is part of the name.

          The values are either lists of layers to ignore for checking.
          or None, which means all the layers in the service are ignored for checking.

          Layer names are suffixed with the _<layer_id> to avoid name clashes. 
          layer_id can be obtained by visiting the parent service folder web page on the base_url
          or can be obtained by running the get_all_info() function from explore.py and looking at the data in the all_layer_list.jsonl.


          Example:
          {
              "G2G_SOI/G2G_PlanningTool/MapServer": None,
              "SOI/G2C_Portal_BaseMap/MapServer": [
                  "50K_Data/50k_Point_1/New Group Layer_163"
              ]
          ]

          In the above example all layers in "G2G_SOI/G2G_PlanningTool/MapServer" are ignored for checking for matches and
          the layer "50K_Data/50k_Point_1/New Group Layer_163" of "SOI/G2C_Portal_BaseMap/MapServer"
          is ignored while checking for matches.

        known_matches: dict
          A map of which layer is already known to be covered by another downloaded layer or a set of downloaded layers

          Example:
          {   
              "SOI/SOI_Public_Portal/MapServer/Public Portal/50K_Data/50K_Poly1/tracks_248": "G2G_Basemap_Portal/G2G_Basemap_Portal/MapServer/50K_Data/50K_Poly1/tracks_1_309",
              "SOI/SOI_Public_Portal/MapServer/Public Portal/DSSDI_Data/RoadCenterLine_156": [
                  "G2G_Basemap_Portal/G2G_Basemap_Portal/MapServer/DSSDI_Data/RoadCenterLine_148",
                  "G2G_Basemap_Portal/G2G_Basemap_Portal/MapServer/DSSDI_Data/RoadCenterLine_1_149"
              ]
          }
    """

    logger.info('getting all layers')
    full_list, full_list_map = read_all_layer_info(analysis_folder)
    
    logger.info('getting done layer list')
    done_layers = already_done(data_folder)
    
    logger.info('expanding done layer list')
    done_layers_expanded = expand_layers(done_layers)
    
    done_layers = set(done_layers)
    
    logger.info('getting full missing list')
    full_missing = get_missing_layer_list(full_list, done_layers, match_ignore)

    logger.info('getting matches')
    possible_best_matches = get_possible_matches(done_layers, done_layers_expanded, full_list_map, full_missing)

    matched_set = set(list(possible_best_matches.keys()))

    missing = prune_missing(full_missing, matched_set, known_matches)

    analysis_folder.mkdir(parents=True, exist_ok=True)

    matches_file = analysis_folder / 'matches.json'
    with open(matches_file, 'w') as f:
        json.dump(possible_best_matches, f, indent=2)
    logger.info(f'{len(possible_best_matches)=}')

    unmatched_file = analysis_folder / 'need_to_check.json'
    with open(unmatched_file, 'w') as f:
        json.dump(missing, f, indent=2)
    logger.info(f'{len(missing)=}')


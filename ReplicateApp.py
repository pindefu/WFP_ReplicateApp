from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from arcgis.features import FeatureLayer
from arcgis.mapping import WebMap

import traceback
from datetime import datetime
import logging
import time
import json
import os
import re
import sys
import arcgis

logger = None
batch_size = 2500
num_failed_records = 0
num_succeeded_records = 0

def get_config(in_file):
    with open(in_file) as config:
        param_dict = json.load(config)

    return param_dict

def get_logger(l_dir, t_filename, s_time):
    global logger

    logger = logging.getLogger(__name__)
    logger.setLevel(1)

    # Set Logger Time
    logger_date = datetime.fromtimestamp(s_time).strftime("%Y_%m_%d %H_%M_%S")
    logger_time = datetime.fromtimestamp(s_time).strftime("%H_%M_%S")

    # Debug Handler for Console Checks - logger.info(msg)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    # Ensure Logs Directory Exists
    # l_dir = os.path.join(t_dir, "logs", logger_date)
    # if not os.path.exists(l_dir):
    #     os.makedirs(l_dir)

    # Log Handler for Reports - logger.info(msg)
    l_file_name = "Log_{}_{}_{}.txt".format(t_filename, logger_date, logger_time)
    l_dir_file_path = os.path.join(l_dir, l_file_name)
    log_handler = logging.FileHandler(l_dir_file_path, "w")
    log_handler.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    logger.info("Script Started: {} - {}".format(logger_date, logger_time))

    return logger, l_dir, l_file_name

def alter_tracking(item, tracking_state):
    if item == None:
        return

    logger.info("\n\n{} Editor tracking on {}\n".format(tracking_state, item.title))
    flc = FeatureLayerCollection.fromitem(item)
    cap = flc.properties["editorTrackingInfo"]
    # logger.info("\n ... existng editor tracking property {}\n".format(cap))

    if tracking_state == "Disable":
        cap["enableEditorTracking"] = False

    else:
        cap["enableEditorTracking"] = True

    alter_response = ""
    try:
        alter_response = flc.manager.update_definition({"editorTrackingInfo": cap})
    except Exception:
        logger.info("Exception {}".format(traceback.format_exc()))
    finally:
        logger.info("Change tracking result: {}\n\n".format(alter_response))


def replace_ignore_case(text, old, new):
    return re.sub(re.escape(old), new, text, flags=re.IGNORECASE)


def run_update(the_func):
    def wrapper(*args, **kwargs):
        global batch_size
        global num_failed_records
        global num_succeeded_records

        # Run Function & Collect Update List
        edit_list = the_func(*args)
        num_total_records = len(edit_list)

        if edit_list:
            operation = kwargs.get("operation", None)
            # Batch Update List Into Smaller Sets
            # batch_size = kwargs.get("batch", None)
            use_global_ids = kwargs.get("use_global_ids", False)
            if not batch_size:
                batch_size = 1000
            update_sets = [
                edit_list[x : x + batch_size]
                for x in range(0, len(edit_list), batch_size)
            ]
            # logger.info("\nProcessing {} Batch(es)\n".format(len(update_sets)))

            if operation in ["update", "add"] and kwargs.get("track") is not None:
                try:
                    alter_tracking(kwargs.get("track"), "Disable")
                except RuntimeError:
                    logger.info("Alter Tracking - RunTime Error. Passing Until Testing Proves Otherwise . . .\n\n")
                    pass

            # Push Edit Batches
            try:
                for update_set in update_sets:
                    try:
                        keyStr = ""
                        if operation == "update":
                            edit_result = kwargs.get("update").edit_features(updates=update_set, use_global_ids=use_global_ids)
                            keyStr = "updateResults"
                        else:  # add
                            edit_result = kwargs.get("add").edit_features(adds=update_set)
                            keyStr = "addResults"


                        totalRecords = len(edit_result[keyStr])

                        print("updateResults {}: {}".format(totalRecords, edit_result[keyStr]))

                        succeeded_records = len(list(filter(lambda d: d["success"] == True,edit_result[keyStr],)))
                        logger.info("\nBatch Edit Results: {} of {} succeeded".format(succeeded_records, totalRecords))
                        num_succeeded_records = num_succeeded_records + succeeded_records
                        if totalRecords > succeeded_records:
                            failed_records = list(filter(lambda d: d["success"] == False,edit_result[keyStr],))
                            num_failed_records = num_failed_records + len(failed_records)
                            logger.info("\Failed records: {}".format(failed_records))


                    except Exception:
                        logger.info(traceback.format_exc())
            except Exception:
                logger.info(traceback.format_exc())
            finally:
                logger.info(" \n\n Summary: Total records {}, succeeded records {}, failed records {}".format(num_total_records, num_succeeded_records, num_failed_records))

                if operation in ["add", "update"]:
                    try:
                        alter_tracking(kwargs.get("track"), "Enable")
                    except RuntimeError:
                        logger.info("Alter Tracking - RunTime Error. Passing Until Testing Proves Otherwise . . .")
                        pass

        else:
            logger.info("Returned List Was Empty. No Edits Performed.")

    return wrapper


def getFolder(folder_name):
    me = gis.users.me
    my_folders = (me.folders)
    folder_list = [i['title'] for i in my_folders]

    if folder_name not in folder_list:
        gis.content.create_folder(folder_name)
        logger.info('\nFolder "{}" created'.format(folder_name))
    else:
        logger.info('\nFolder "{}" already exists'.format(folder_name))

    return folder_name

def queryMapExtent(ext_lyr_url, sWhere):
    # Query for the default extent for the web map
    extent_lyr = FeatureLayer(ext_lyr_url)
    response = extent_lyr.query(where=sWhere, return_extent_only=True, out_sr=102100)
    logger.info("\n\nExtent query response: {}".format(response))
    country_extent = response["extent"]
    return country_extent

def cloneFeatureLayers(fLyr_items, country, new_folder):

    itemId_lookup = {}
    for itmId in fLyr_items:
        lyr_item = gis.content.get(itmId)
        lyr_url = lyr_item.url
        # Get the service name from the layer url
        service_name = lyr_url.split("/")[-2]

        layer_ids = [layer.properties.id for layer in lyr_item.layers]
        table_ids = [table.properties.id for table in lyr_item.tables]

        new_service_name = "{}_{}".format(service_name, country)
        logger.info("\nCreating layer: {}".format(new_service_name))
        # copy the feature layers and name them with the country name
        item_copy = lyr_item.copy_feature_layer_collection(
            service_name = new_service_name,
            layers = layer_ids,
            tables = table_ids,
            folder = new_folder
        )
        logger.info("\nLayer created. Id: {}".format(item_copy.id))
        itemId_lookup[itmId] = item_copy.id

    return itemId_lookup

def getUrlLookup(itemId_lookup):
    url_lookup = {}
    for template_id in itemId_lookup:
        template_item = gis.content.get(template_id)
        template_flUrl = template_item.url

        new_id = itemId_lookup[template_id]
        new_item = gis.content.get(new_id)
        new_flUrl = new_item.url

        url_lookup[template_flUrl] = new_flUrl

    logger.info("\n\nUrl lookup: {}".format(url_lookup))
    return url_lookup


def processTask(task, naming_patterns, template_country_name, init_extent_config, fLyr_items, imgLyr_items, webmap_item, wab_template_item):
    country = task["country"]
    new_folder_name = naming_patterns["folder_pattern"].replace('{country}', country)
    new_folder = getFolder(new_folder_name)

    # create a feature layer object using a url
    ext_lyr_url = init_extent_config["layer_url"]
    sWhere = init_extent_config["where"].replace('{country}', country)
    country_extent = queryMapExtent(ext_lyr_url, sWhere)

    itemId_lookup = cloneFeatureLayers(fLyr_items, country, new_folder)
    if len(imgLyr_items) > 0 and "img_lyr_itemd_id" in task:
        itemId_lookup[imgLyr_items[0]] = task['img_lyr_itemd_id']

    logger.info("\n\nLayer id lookup: {}".format(itemId_lookup))

    new_webmap = cloneWebMap(webmap_item, new_folder, itemId_lookup, country_extent, country, naming_patterns)
    new_webmap_id = new_webmap.item.id
    itemId_lookup[webmap_item.id] = new_webmap_id

    logger.info("\n\nLayer and map id lookup: {}".format(itemId_lookup))

    new_wab = cloneWABApp(wab_template_item, new_webmap, itemId_lookup, country_extent, template_country_name, new_folder, country, naming_patterns)
    return new_wab

def migrate_resources(old_item, new_item, overwrite=True):
    """
    Migrate resources from one item to another.
    """
    from os.path import basename, dirname, join, exists
    results = []
    for resource in old_item.resources.list():
        try:
#             print(f"Migrating {resource['resource']}")
            folder = dirname(resource['resource'])
            file_name = basename(resource['resource'])
            file = old_item.resources.get(resource['resource'])
            if type(file) == str and exists(file):
                res = new_item.resources.add(
                    file = file,
                    file_name = file_name,
                    folder_name = folder
                )

            else:
                res = new_item.resources.add(
                    text = file,
                    file_name = file_name,
                    folder_name = folder
                )
            results.append(res)
        except Exception as e:
            print(e)
            print('trying to update')
            if overwrite:
                print('overwriting')
                if type(file) != str or not exists(file):
                    text = file
                    file = 'temp.txt'
                    with open(file, 'w') as f:
                        f.write(text)
                res = new_item.resources.update(
                    file = file,
                    file_name = file_name,
                    folder_name = folder
                )
                results.append(res)

    return results

# Replace the country name in the json using regular expression
# The pattern is " sourceCountryName ". The space before and after the country name may vary. There may be 0 or more spaces
# When replacing the country name, keep the space before and after the country name
# When matching the country name, ignore the cases
def replaceCountryName(str_json, sourceCountryName, targetCountryName):
    pattern = r'(?i)(\s*)' + sourceCountryName + r'(\s*)'
    str_json = re.sub(pattern, r'\1' + targetCountryName + r'\2', str_json)
    return str_json


def cloneWABApp(wab_template_item, new_webmap, itemId_lookup, country_extent, template_country_name, new_folder, country, naming_patterns):
    logger.info("\nTo clone the app")
    cloned_items = gis.content.clone_items(items=[wab_template_item], folder=new_folder, copy_data=False, search_existing_items=False, item_mapping = itemId_lookup)
    new_wab_item = cloned_items[0]
    logger.info("\nCloned the app. New ID: {}".format(new_wab_item.id))

    logger.info("\nTo update the app title, country text, init extent, and appItemId")
    wab_json = new_wab_item.get_data(try_json=True)
    appTitle = naming_patterns["app_title_patern"].replace('{country}', country)
    wab_json["title"] = appTitle
    wab_json["appItemId"] = new_wab_item.id
    wab_json['map']['mapOptions']['extent'] = country_extent

    str_json = json.dumps(wab_json)
    str_json = replaceCountryName(str_json, template_country_name, country)
    new_wab_item.update(item_properties = {"title": appTitle}, data = str_json)
    logger.info("\nApp updated successfully")

    #migrate_resources(wab_template_item, new_wab_item)

    return new_wab_item

def cloneWebMap(webmap_item, new_folder, itemId_lookup, country_extent, country, naming_patterns):
    logger.info("\nTo clone the web map")
    cloned_items = gis.content.clone_items(items=[webmap_item], folder=new_folder, copy_data=False, search_existing_items=False, item_mapping = itemId_lookup)
    logger.info("\nCloned the web map. ID: {}".format(cloned_items[0].id))
    logger.info("\nTo update the web map title")
    newMap = WebMap(cloned_items[0])
    newTitle = naming_patterns["webmap_title_patern"].replace('{country}', country)
    # set the new map's extent to the country extent
    newMap.item.extent = country_extent
    newMap.item.spatialReference = country_extent['spatialReference']
    newMap.update(item_properties = {"title": newTitle})
    logger.info("\nWeb map updated successfully")
    return newMap

def get_layer_item_ids(wm):
    wmo = WebMap(wm)
    fl_item_id_list = []
    il_item_id_list = []
    for layer in wmo.layers:
        # if layer url has ImageServer in it, regardless upper or lower case, it is an image layer
        if "imageserver" in layer['url'].lower():
            if not layer['itemId'] in il_item_id_list:
                il_item_id_list.append(layer['itemId'])

        try:
            fsvc = FeatureLayerCollection(layer['url'][:-1], gis)
            if not fsvc.properties['serviceItemId'] in fl_item_id_list:
                fl_item_id_list.append(fsvc.properties['serviceItemId'])
        except Exception as e:
            pass
    return fl_item_id_list, il_item_id_list

def getAppMapLayerItems(wab_template_itemId):
    logger.info("\n*********** Inspect the template app {} *************".format(wab_template_itemId))
    # Get the web map json
    wab_template_item = gis.content.get(wab_template_itemId)
    wab_template_json = wab_template_item.get_data(try_json=True)

    # get the web map
    webmap_id = wab_template_json['map']['itemId']

    logger.info("\n\tWeb map id: {}".format(webmap_id))
    webmap_item = gis.content.get(webmap_id)
    fl_items_in_wm, imgl_items_in_wm = get_layer_item_ids(webmap_item)
    logger.info("\n\tFeature layer items in web map: {}".format(fl_items_in_wm))
    logger.info("\n\tImage layer items in web map: {}".format(imgl_items_in_wm))

    return wab_template_item, webmap_item, fl_items_in_wm, imgl_items_in_wm

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, './config/config_ReplicateApp.json'))

    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)
    logger.info("Python version {}".format(sys.version))
    logger.info("ArcGIS Python API version {}".format(arcgis.__version__))

    try:
        the_portal = parameters['the_portal']
        portal_url = the_portal['url']
        the_username = the_portal['user']
        the_password = the_portal['pass']
        gis = GIS(portal_url, the_username, the_password)

        wab_template_itemId = parameters['wab_template_itemId']
        wab_template_item, webmap_item, fLyr_items, imgLyr_items = getAppMapLayerItems(wab_template_itemId)
        template_country_name = parameters['template_country']
        naming_patterns = parameters['naming_patterns']
        init_extent_config = parameters['init_extent_config']

        tasks = parameters["tasks"]
        for task in tasks:
            logger.info("\n\n *********** Processing: {} *************".format(task["country"]))
            task_start_time = time.time()
            processTask(task, naming_patterns, template_country_name, init_extent_config, fLyr_items, imgLyr_items, webmap_item, wab_template_item)
            logger.info("\n ------ task run time: {0} Minutes".format(round(((time.time() - task_start_time) / 60), 2)))

    except Exception:
        logger.info("\n\n{}".format(traceback.format_exc()))

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes\n\n'.format(round(((time.time() - start_time) / 60), 2)))

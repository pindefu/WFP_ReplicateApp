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

def readAssignments(survey123Layer):

    logger.info("\n ------ Query for current assignments ------ ")

    fs = survey123Layer.query("1=1", out_fields="SiteID, assignedTo", order_by_fields="SiteID", return_distinct_values=True, return_all_records=True, return_geometry=False)
    logger.info("\n Number of assignments returned  {}\n".format(len(fs.features)))

    siteId_Evaluator_Lookup = {}
    for f in fs.features:
        siteId_Evaluator_Lookup[f.attributes["SiteID"]] = f.attributes["assignedTo"]

    logger.info("\n\n siteId_Evaluator_Lookup {}".format(siteId_Evaluator_Lookup))
    return siteId_Evaluator_Lookup

def getFolder(folder_name):
    me = gis.users.me
    my_folders = (me.folders)
    folder_list = [i['title'] for i in my_folders]

    if folder_name not in folder_list:
        gis.content.create_folder(folder_name)
        logger.info("\n\nFolder {} created".format(folder_name))
    else:
        logger.info("\n\nFolder {} already exists".format(folder_name))

    return folder_name

def queryMapExtent(ext_lyr_url, sWhere):
    # Query for the default extent for the web map
    extent_lyr = FeatureLayer(ext_lyr_url)
    response = extent_lyr.query(where=sWhere, return_extent_only=True)
    logger.info("\n\nExtent query response: {}".format(response))
    country_extent = response["extent"]
    return country_extent

def copyFeatureLayers(flyr_itemId_lookup, country, new_folder):
    # loop through flyr_itemId_lookup
    # copy the feature layers and name them with the country name
    itemId_lookup = {}
    for lyr_url in flyr_itemId_lookup:
        # Get the service name from the layer url
        service_name = lyr_url.split("/")[-2]
        template_id = flyr_itemId_lookup[lyr_url]
        lyr_item = gis.content.get(template_id)

        layer_ids = [layer.properties.id for layer in lyr_item.layers]
        table_ids = [table.properties.id for table in lyr_item.tables]

        new_service_name = "{}_{}".format(service_name, country)
        logger.info("\n\nTo create the layer : {}".format(new_service_name))
        item_copy = lyr_item.copy_feature_layer_collection(
            service_name = new_service_name,
            layers = layer_ids,
            tables = table_ids,
            folder = new_folder
        )
        itemId_lookup[template_id] = item_copy

    logger.info("\n\nItem id lookup: {}".format(itemId_lookup))

    url_lookup = {}
    for fl_url in flyr_itemId_lookup:
        temp_item_id = flyr_itemId_lookup[fl_url]
        new_fl_url = itemId_lookup[temp_item_id].url
        url_lookup[fl_url] = new_fl_url

    logger.info("\n\nUrl lookup: {}".format(url_lookup))

    return itemId_lookup, url_lookup


def processTask(task, naming_patterns, init_extent_config, flyr_itemId_lookup, webmap_item, wab_template_item):
    country = task["country"]
    new_folder_name = naming_patterns["folder_pattern"].replace('{country}', country)
    new_folder = getFolder(new_folder_name)

    # create a feature layer object using a url
    ext_lyr_url = init_extent_config["layer_url"]
    sWhere = init_extent_config["where"].replace('{country}', country)
    country_extent = queryMapExtent(ext_lyr_url, sWhere)

    itemId_lookup, url_lookup = copyFeatureLayers(flyr_itemId_lookup, country, new_folder)

    new_webmap = createWebMap(webmap_item, itemId_lookup, url_lookup, country_extent, country, naming_patterns)

    new_wab = createWABApp(wab_template_item, new_webmap, itemId_lookup, url_lookup, country, naming_patterns)

def replaceUrlItemIds(aJson, itemId_lookup, url_lookup):
    str_json = json.dumps(aJson)
    # replace the layer urls, ignore the upper/lower case
    for old_url in url_lookup:
        new_url = url_lookup[old_url]
        logger.info("\n\nReplacing url {} with {}".format(old_url, new_url))
        str_json = replace_ignore_case(str_json, old_url, new_url)

    # replace the layer item ids
    for old_id in itemId_lookup:
        new_id = itemId_lookup[old_id].id
        logger.info("\n\nReplacing item id {} with {}".format(old_id, new_id))
        str_json = replace_ignore_case(str_json, old_id, new_id)

    return str_json

def createItemFromJson(oldItem, str_json, newTitle, country):

    new_item_properties = oldItem.properties
    # remove the id, owner, created, and url properties
    new_item_properties.pop("id", None)
    new_item_properties.pop("owner", None)
    new_item_properties.pop("created", None)
    new_item_properties.pop("url", None)
    new_item_properties["title"] = newTitle
    new_item_properties["tags"] = "{},{}".format(new_item_properties["tags"], country)
    new_item_properties["text"] = str_json

    logger.info("\n\nCreating the new item: {}".format(new_item_properties["title"]))
    return gis.content.add(item_properties = new_item_properties)



def createWABApp(wab_template_item, new_webmap, itemId_lookup, url_lookup, country, naming_patterns):
    wab_json = wab_template_item.get_data(try_json=True)
    str_json = replaceUrlItemIds(wab_json, itemId_lookup, url_lookup)

    appTitle = naming_patterns["wab_pattern"].replace('{country}', country)

    # In the web app json:
    #   replace the web map item id
    #   replace the layer urls
    # name the new web app with the country name
    wab_json = wab_template_item.get_data(try_json=True)
    wab_json["title"] = appTitle
    wab_json['map']['itemId'] = new_webmap.id
    wab_json['map']['mapOptions']['extent'] = new_webmap.extent

    return createItemFromJson(webmap_item, str_json, appTitle, country)


def createWebMap(webmap_item, itemId_lookup, url_lookup, country_extent, country, naming_patterns):
    webmap_json = webmap_item.get_data(try_json=True)
    str_json = replaceUrlItemIds(webmap_json, itemId_lookup, url_lookup)
    newTitle = naming_patterns["webmap_pattern"].replace('{country}', country)

    return createItemFromJson(webmap_item, str_json, newTitle, country)

def getLayerUrlItemLookup(wab_template_itemId):
    # Get the web map json
    wab_template_item = gis.content.get(wab_template_itemId)
    wab_template_json = wab_template_item.get_data(try_json=True)

    # get the web map
    webmap_id = wab_template_json['map']['itemId']
    logger.info("\n\nWeb map id: {}".format(webmap_id))
    webmap_item = gis.content.get(webmap_id)

    webmap_json = webmap_item.get_data(try_json=True)

    # get the operational layers in the web map
    operational_layers = webmap_json['operationalLayers']
    # logger.info("\n\nOperational layers: {}".format(operational_layers))
    template_img_lyr_name = parameters['webmap_OptLayers']["template_img_lyr_name"]
    template_feature_lyr_names = parameters['webmap_OptLayers']["template_feature_lyr_names"]
    # ToDo: republish the template image layer later
    # Find the template feature layers
    flyr_itemId_lookup = {}
    for lyr in operational_layers:
        lyr_url = lyr['url'].lower()
        # if the url ends with a number, remove the number and the slash
        if lyr_url[-1].isdigit():
            lyr_url = lyr_url[:-2]
        # if the layer url string includes the template feature layer name and the layer url is not already in the list of template feature layer urls
        for template_feature_lyr_name in template_feature_lyr_names:
            logger.info("\n\nTemplate feature layer name: {}".format("/{}/FeatureLayer".format(template_feature_lyr_name).lower() ))
            logger.info("{}".format(lyr_url))
            if "/{}/FeatureServer".format(template_feature_lyr_name).lower() in lyr_url:
                if lyr_url not in flyr_itemId_lookup:
                    flyr_itemId_lookup[lyr_url] = lyr['itemId'] if ('itemId' in lyr and lyr['itemId'] is not None) else None
                else:
                    if flyr_itemId_lookup[lyr_url] is None and 'itemId' in lyr:
                        flyr_itemId_lookup[lyr_url] = lyr['itemId']
    logger.info("\n\nTemplate feature layer item id lookup: {}".format(flyr_itemId_lookup))

    return flyr_itemId_lookup, webmap_item, wab_template_item


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

    try:
        the_portal = parameters['the_portal']
        portal_url = the_portal['url']
        the_username = the_portal['user']
        the_password = the_portal['pass']
        gis = GIS(portal_url, the_username, the_password)

        wab_template_itemId = parameters['wab_template_itemId']
        flyr_itemId_lookup, webmap_item, wab_template_item = getLayerUrlItemLookup(wab_template_itemId)

        naming_patterns = parameters['naming_patterns']
        init_extent_config = parameters['init_extent_config']

        tasks = parameters["tasks"]
        for task in tasks:
            logger.info("\n\n\n *********** {} *************".format(task["country"]))
            task_start_time = time.time()
            processTask(task, naming_patterns, init_extent_config, flyr_itemId_lookup, webmap_item, wab_template_item)
            logger.info("\n\n\n ... task run time: {0} Minutes".format(round(((time.time() - task_start_time) / 60), 2)))

    except Exception:
        logger.info("\n\n{}".format(traceback.format_exc()))

    finally:
        # Log Run Time
        logger.info('Program Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))

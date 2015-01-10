#!/usr/bin/env python
# coding: utf-8

# A Repeatable Extract-Tranform-Load Pipeline for NYPL Menus Data

# authors: Trevor Muñoz and Katie Rawson
# created: 17 October 2014
# updated: 28 October 2014, 19 November 2014, 3 December 2014,
#     14 December 2014, 8-9 January 2015

from collections import Counter
import datetime
import elasticsearch
from elasticsearch import helpers
import json
import logging
import logging.handlers
import os
import pandas as pd
import pytz
import re
import tarfile
import time


#TODO: Give option to set up or turn off at cmd line
LOG_FILENAME = os.path.join(os.environ['MENUS_LOG_HOME'],
                            'nypl_menus_data_transform.log')

PIPELINE_LOGGER = logging.getLogger('MenusDataTransformLogger')
PIPELINE_LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME,
    maxBytes=500000,
    backupCount=10)
handler.setFormatter(formatter)
PIPELINE_LOGGER.addHandler(handler)
UTC = pytz.utc


def normalize_names(obj):
    '''
    Take a name as a string, converts the string
    to lowercase, strips whitespace from beginning
    and end, normalizes multiple internal whitespace
    characters to a single space. E.g.:
    
    normalize_names('Chicken gumbo ') = 'chicken gumbo'
    
    '''
    tokens = obj.strip().lower().split()
    result = ' '.join(filter(None, tokens))
    return result


def fingerprint(obj):
    """
    A modified version of the fingerprint clustering algorithm implemented by Open Refine.
    See https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth
    This does not normalize to ASCII characters since diacritics may be significant in this dataset
    """
    alphanumeric_tokens = filter(None, re.split('\W', obj))
    seen = set()
    seen_add = seen.add
    deduped = sorted([i for i in alphanumeric_tokens if i not in seen and not seen_add(i)])
    fingerprint = ' '.join(deduped)
    
    return fingerprint


# TOFIX: This code is very slow
def reformat_dates(obj):
    naive_date_in = datetime.datetime.strptime(obj, '%Y-%m-%d %H:%M:%S %Z')
    date_in = naive_date_in.replace(tzinfo=UTC)
    date_out = date_in.strftime('%Y%m%dT%H%M%S%z')
    return date_out


def reshape_data(obj):
    '''
    Takes JSON, loads it, and spits out a dictionary of slightly different
    shape needed for bulk import by Elasticsearch.
    
    Input is JSON rather than simply another dictionary so I can punt on
    properly serializing things like 'NaN' by leaving that to pandas own
    to_json() function.
    '''

    data = json.loads(obj)
    action = {
              "_index": "menus",
              "_type": "item",
              "_id": int(data['item_id']),
              "_source": data
              }
    return action


def server(hostname, host_port):
    """
    Makes sure that an elasticsearch server is running.
    Sets up the appropriate index and mapping.
    """
    
    PIPELINE_LOGGER.info('Verifying Elasticsearch server is ready to receive data …')
    es = elasticsearch.Elasticsearch([{'host': hostname, 'port': host_port}])

    index_name = 'menus'
    type_name = 'item'

    # Check if the index already exists and, if so, delete it to keep things idempotent
    if es.indices.exists(index_name):
        PIPELINE_LOGGER.info("deleting '{0}' index...".format(index_name))
        res = es.indices.delete(index = index_name)
        PIPELINE_LOGGER.info(" response: '{0}'".format(res))

    PIPELINE_LOGGER.info("creating '{0}' index...".format(index_name))
    res = es.indices.create(index = index_name)
    PIPELINE_LOGGER.info(" response: '{0}'".format(res))

    item_mapping = {"properties": {
        'item_updated_at': {
            "type": "date",
            "format": "basic_date_time_no_millis"
        },
        'menu_page_id': {
            "type": "long",
        } ,
     'menu_sponsor': {
        "type": "string",
        "index": "not_analyzed"
        },
     'menu_page_count': {
        "type": "double"
        },
     'item_ypos': {
        "type": "double"
        },
     'dish_normalized_name': {
        "type": "string"
        },
     'menu_id': {
            "type": "long",
        },
     'dish_times_appeared': {
        "type": "double"
        },
     'menu_location': {
        "type": "string",
        "index": "not_analyzed"
        },
     'dish_menus_appeared': {
        "type": "double"
        },
     'menu_uri': {
        "type": "string",
        "index": "not_analyzed"
        },
     'menu_page_uri': {
        "type": "string",
        "index": "not_analyzed"
        },
     'page_image_uuid': {
        "type": "string"
        },
     'menu_page_number': {
        "type": "double"
        },
     'item_created_at': {
        "type": "date",
        "format": "basic_date_time_no_millis"
        },
     'dish_id': {
        "type": "long",
        },
     'dish_name': {
        "type": "string",
        "index": "not_analyzed"
        },
     'item_uri': {
        "type": "string",
        "index": "not_analyzed"
        },
     'item_id': {
        "type": "long",
        },
     'image_id': {
        "type": "long",
        },
     'item_xpos': {
        "type": "double"
        },
     'dish_name_fingerprint': {
        "type": "string",
        "index": "not_analyzed"
        },
     'dish_uri': {
        "type": "string",
        "index": "not_analyzed"
        }
    }
    }

    PIPELINE_LOGGER.info("Creating mapping for '{0}' index...".format(index_name))
    res = es.indices.put_mapping(index='menus', doc_type='item', body=item_mapping)
    PIPELINE_LOGGER.info(" response: '{0}'".format(res))

    return es


def load_and_tranform(fpath):
    """
    Takes a file path where data file(s) to be processed is/are located
    and returns a generator that yield dictionaries representing
    documents to be indexed
    """

    srcfile = [x for x in os.listdir(fpath) if os.path.splitext(x)[1] == '.tgz'][0]
    tar = tarfile.open(os.path.join(fpath, srcfile))

    PIPELINE_LOGGER.info('Extracting source files …')
    PIPELINE_LOGGER.info('Listing contents of the tar package …')
    for tf in tar.getmembers():
        PIPELINE_LOGGER.info('Name: {0} \t Last Modified: {1}'.format(tf.name, time.ctime(tf.mtime)))

    PIPELINE_LOGGER.info('Untarring and unzipping …')
    tar.extractall(path=fpath)

    for f in os.listdir(fpath):
        if f.endswith('csv'):
            if os.path.isfile(os.path.join(fpath, f)) == True:
                PIPELINE_LOGGER.info('{0} … \u2713'.format(f))

    tar.close()
    
    PIPELINE_LOGGER.info('Loading data from source files into memory …')

    latest_dish_data_df = pd.DataFrame.from_csv(os.path.join(fpath, 'Dish.csv'), index_col='id')
    latest_item_data_df = pd.DataFrame.from_csv(os.path.join(fpath, 'MenuItem.csv'), index_col='dish_id')
    latest_page_data_df = pd.DataFrame.from_csv(os.path.join(fpath, 'MenuPage.csv'), index_col='id')
    latest_menu_data_df = pd.DataFrame.from_csv(os.path.join(fpath, 'Menu.csv'), index_col='id')

    PIPELINE_LOGGER.info('Data loaded. Starting transformations …')
    
    # =================================
    # 
    #  Dish.csv
    #
    # =================================

    PIPELINE_LOGGER.info('Working on Dish.csv …')
    null_appearances = latest_dish_data_df[latest_dish_data_df.times_appeared == 0]
    PIPELINE_LOGGER.info('Data set contains {0} dishes that appear 0 times …'.format(len(null_appearances)))
    
    non_null_dish_data_df = latest_dish_data_df[latest_dish_data_df.times_appeared != 0]
    discarded_columns = [n for n in non_null_dish_data_df.columns if n not in ['name', 'menus_appeared', 'times_appeared']]
    PIPELINE_LOGGER.info('Discarding columns from Dish.csv …')
    for discard in discarded_columns:
        PIPELINE_LOGGER.info('{0} … removed'.format(discard))
        
    trimmed_dish_data_df = non_null_dish_data_df[['name', 'menus_appeared', 'times_appeared']]
    PIPELINE_LOGGER.info('Dish.csv contains {0} potentially-unique dish names before any normalization'.
                     format(trimmed_dish_data_df.name.nunique()))
    
    trimmed_dish_data_df['normalized_name'] = trimmed_dish_data_df.name.map(normalize_names)
    PIPELINE_LOGGER.info(
    'Dish.csv contains {0} potentially-unique dish names after normalizing whitespace and punctuation'
    .format(trimmed_dish_data_df.normalized_name.nunique())
    )
    
    trimmed_dish_data_df['fingerprint'] = trimmed_dish_data_df.normalized_name.map(fingerprint)
    PIPELINE_LOGGER.info(
    'Dish.csv contains {0} unique fingerprint values'
    .format(trimmed_dish_data_df.fingerprint.nunique())
    )
    #trimmed_dish_data_df.head()
    
    # =================================
    # 
    # MenuItem.csv
    #
    # =================================
    
    PIPELINE_LOGGER.info('Working on MenuItem.csv …')
    PIPELINE_LOGGER.info("Reformatting item dates …\
                        This may take a few minutes …")
    latest_item_data_df['item_created_at'] = latest_item_data_df.created_at.map(reformat_dates)
    latest_item_data_df['item_updated_at'] = latest_item_data_df.updated_at.map(reformat_dates)
    PIPELINE_LOGGER.info('Date reformatting complete …')
    
    discarded_columns2 = [n for n in latest_item_data_df.columns if n not in 
                      ['id', 'menu_page_id', 'xpos', 'ypos', 'item_created_at', 'item_updated_at']]
    PIPELINE_LOGGER.info('Discarding columns from MenuItem.csv …')
    for discard2 in discarded_columns2:
        PIPELINE_LOGGER.info('{0} … removed'.format(discard2))
        
    trimmed_item_data_df = latest_item_data_df[['id', 'menu_page_id', 'xpos', 'ypos',
                                           'item_created_at', 'item_updated_at']]
    #trimmed_item_data_df.head()
    
    # =================================
    # 
    # MenuPage.csv
    #
    # =================================
    
    PIPELINE_LOGGER.info('Working on MenuPage.csv …')
    latest_page_data_df[['full_height', 'full_width']].astype(int, raise_on_error=False)
    
    # =================================
    # 
    # Menu.csv
    #
    # =================================
    
    PIPELINE_LOGGER.info('Working on Menu.csv …')
    discarded_columns3 = [n for n in latest_menu_data_df.columns if n not in 
                      ['sponsor', 'location', 'date', 'page_count', 'dish_count']]
    PIPELINE_LOGGER.info('Discarding columns from Menu.csv …')
    for discard3 in discarded_columns3:
        PIPELINE_LOGGER.info('{0} … removed'.format(discard3))
    
    trimmed_menu_data_df = latest_menu_data_df[['sponsor', 'location', 'date',
                                            'page_count', 'dish_count']]
    #trimmed_menu_data_df.head()
    
    # =================================
    # 
    # Merged DataFrames
    #
    # =================================
    
    PIPELINE_LOGGER.info('Merging dataframes …')
    merged_item_pages_df = pd.merge(trimmed_item_data_df, latest_page_data_df, 
                                left_on='menu_page_id', right_index=True, )
    
    merged_item_pages_df.columns = ['item_id', 'menu_page_id', 'xpos', 'ypos', 
                                'item_created_at', 'item_updated_at', 'menu_id', 'page_number', 
                                'image_id', 'full_height', 'full_width', 'uuid']
    #merged_item_pages_df.head()
    
    merged_item_pages_menus_df = pd.merge(trimmed_menu_data_df, merged_item_pages_df, 
                                      left_index=True, right_on='menu_id')
    
    full_merge_df = pd.merge(merged_item_pages_menus_df, trimmed_dish_data_df, 
                      left_index=True, right_index=True)
    #full_merge_df.head()
    
    for_json_output_df = full_merge_df.reset_index()
    
    for_json_output_df.columns
    renamed_columns = ['dish_id', 'menu_sponsor', 'menu_location', 'menu_date', 'menu_page_count', 
                   'menu_dish_count', 'item_id', 'menu_page_id', 'item_xpos', 'item_ypos', 
                   'item_created_at', 'item_updated_at', 'menu_id', 'menu_page_number', 'image_id', 
                   'page_image_full_height', 'page_image_full_width', 'page_image_uuid', 'dish_name', 
                   'dish_menus_appeared', 'dish_times_appeared', 'dish_normalized_name', 'dish_name_fingerprint']
    for_json_output_df.columns = renamed_columns
    
    for_json_output_df[['menu_page_number', 'dish_id', 'item_id', 'menu_page_id', 'menu_id']].astype(int, raise_on_error=False)
    
    for_json_output_df['dish_uri']= for_json_output_df.dish_id.map(lambda x: 'http://menus.nypl.org/dishes/{0}'.format(int(x)))
    for_json_output_df['item_uri']= for_json_output_df.item_id.map(lambda x: 'http://menus.nypl.org/menu_items/{0}/edit'
                                               .format(int(x)))
    for_json_output_df['menu_page_uri'] = for_json_output_df.menu_page_id.map(lambda x: 'http://menus.nypl.org/menu_pages/{0}'
                                                          .format(int(x)))
    for_json_output_df['menu_uri'] = for_json_output_df.menu_id.map(lambda x:'http://menus.nypl.org/menus/{0}'
                                                .format(int(x)))
    
    for_json_output_df.fillna('null')
    
    PIPELINE_LOGGER.info('Merged dataframe ready')
    #for_json_output_df.head()
    
    # df.iterrows is a generator that yields a positional index and a Series,
    # call the to_json method on the series
    return (reshape_data(row.to_json()) for i, row in for_json_output_df.iterrows())


def load(fp, host='localhost', port=9200):
    """
    Takes a path and an optional specification
    of hostname and port for (elasticsearch) to
    load transformed data to.

    Consumes values from a generator and passes them
    to streaming bulk handler provided by elasticsearch
    client
    """

    source_data = fp

    PIPELINE_LOGGER.info('Menus ETL Pipeline: Starting run …')
    c = Counter()

    try:
        client = server(host, port)
        actioner = load_and_tranform(source_data)

        PIPELINE_LOGGER.info('Preparing data to load into Elasticsearch …')
        for ok, result in helpers.streaming_bulk(client, actioner, chunk_size=1000):
            action, result = result.popitem()
            doc_id = '/menus/item/{0}'.format(result['_id'])
            if not ok:
                PIPELINE_LOGGER.error('Failed to {0} document {1}: {2}'.format(action, doc_id, result['error']))
            else:
                c[doc_id] += 1
                
            PIPELINE_LOGGER.info('{0} action succeeded for {1} documents'.format(action, sum(c.values())))
            
    except BaseException as e:
        PIPELINE_LOGGER.error(
            'Something went wrong: {exception_class} ({exception_docstring}): {exception_message}'.format(
                exception_class = e.__class__,
                exception_docstring = e.__doc__,
                exception_message = e))

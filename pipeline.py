
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

# Set up logging
LOG_FILENAME = os.path.join(os.environ['MENUS_LOG_HOME'],
                            'nypl_menus_data_transform.log')

pipeline_logger = logging.getLogger('MenusDataTransformLogger')
pipeline_logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME,
    maxBytes=500000,
    backupCount=10)
handler.setFormatter(formatter)
pipeline_logger.addHandler(handler)

UTC = pytz.utc
os.environ['MENUS_ES_HOSTNAME'] = 'localhost'
os.environ['MENUS_ES_HOST_PORT'] = '5000'


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


def server(hostname=os.environ['MENUS_ES_HOSTNAME'], host_port=os.environ['MENUS_ES_HOST_PORT']):
    """
    Makes sure that an elasticsearch server is running.
    Sets up the appropriate index and mapping.
    """
    
    pipeline_logger.info('Verifying Elasticsearch server is ready to receive data …')
    es = elasticsearch.Elasticsearch([{'host': hostname, 'port': host_port}])

    index_name = 'menus'
    type_name = 'item'

    # Check if the index already exists and, if so, delete it to keep things idempotent
    if es.indices.exists(index_name):
        pipeline_logger.info("deleting '{0}' index...".format(index_name))
        res = es.indices.delete(index = index_name)
        pipeline_logger.info(" response: '{0}'".format(res))

    pipeline_logger.info("creating '{0}' index...".format(index_name))
    res = es.indices.create(index = index_name)
    pipeline_logger.info(" response: '{0}'".format(res))

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

    pipeline_logger.info("Creating mapping for '{0}' index...".format(index_name))
    res = es.indices.put_mapping(index='menus', doc_type='item', body=item_mapping)
    pipeline_logger.info(" response: '{0}'".format(res))

    return es


def load_and_tranform(fpath):
    """
    Takes a file path where data file(s) to be processed is/are located
    and returns a generator that yield dictionaries representing
    documents to be indexed
    """

    srcfile = [x for x in os.listdir(fpath) if os.path.splitext(x)[1] == '.tgz'][0]
    tar = tarfile.open(os.path.join(fpath, srcfile))

    pipeline_logger.info('Extracting source files …')
    pipeline_logger.info('Listing contents of the tar package …')
    for tf in tar.getmembers():
        pipeline_logger.info('Name: {0} \t Last Modified: {1}'.format(tf.name, time.ctime(tf.mtime)))

    pipeline_logger.info('Untarring and unzipping …')
    tar.extractall(path=fpath)

    for f in os.listdir(fpath):
        if f.endswith('csv'):
            if os.path.isfile(os.path.join(fpath, f)) == True:
                pipeline_logger.info('{0} … \u2713'.format(f))

    tar.close()
    
    pipeline_logger.info('Loading data from source files into memory …')

    LATEST_DISH_DATA_DF = pd.DataFrame.from_csv(os.path.join(fpath, 'Dish.csv'), index_col='id')
    LATEST_ITEM_DATA_DF = pd.DataFrame.from_csv(os.path.join(fpath, 'MenuItem.csv'), index_col='dish_id')
    LATEST_PAGE_DATA_DF = pd.DataFrame.from_csv(os.path.join(fpath, 'MenuPage.csv'), index_col='id')
    LATEST_MENU_DATA_DF = pd.DataFrame.from_csv(os.path.join(fpath, 'Menu.csv'), index_col='id')

    pipeline_logger.info('Data loaded. Starting transformations …')
    
    # =================================
    # 
    #  Dish.csv
    #
    # =================================

    pipeline_logger.info('Working on Dish.csv …')
    NULL_APPEARANCES = LATEST_DISH_DATA_DF[LATEST_DISH_DATA_DF.times_appeared == 0]
    pipeline_logger.info('Data set contains {0} dishes that appear 0 times …'.format(len(NULL_APPEARANCES)))
    
    NON_NULL_DISH_DATA_DF = LATEST_DISH_DATA_DF[LATEST_DISH_DATA_DF.times_appeared != 0]
    discarded_columns = [n for n in NON_NULL_DISH_DATA_DF.columns if n not in ['name', 'menus_appeared', 'times_appeared']]
    pipeline_logger.info('Discarding columns from Dish.csv …')
    for discard in discarded_columns:
        pipeline_logger.info('{0} … removed'.format(discard))
        
    TRIMMED_DISH_DATA_DF = NON_NULL_DISH_DATA_DF[['name', 'menus_appeared', 'times_appeared']]
    pipeline_logger.info('Dish.csv contains {0} potentially-unique dish names before any normalization'.
                     format(TRIMMED_DISH_DATA_DF.name.nunique()))
    
    TRIMMED_DISH_DATA_DF['normalized_name'] = TRIMMED_DISH_DATA_DF.name.map(normalize_names)
    pipeline_logger.info(
    'Dish.csv contains {0} potentially-unique dish names after normalizing whitespace and punctuation'
    .format(TRIMMED_DISH_DATA_DF.normalized_name.nunique())
    )
    
    TRIMMED_DISH_DATA_DF['fingerprint'] = TRIMMED_DISH_DATA_DF.normalized_name.map(fingerprint)
    pipeline_logger.info(
    'Dish.csv contains {0} unique fingerprint values'
    .format(TRIMMED_DISH_DATA_DF.fingerprint.nunique())
    )
    #TRIMMED_DISH_DATA_DF.head()
    
    # =================================
    # 
    # MenuItem.csv
    #
    # =================================
    
    pipeline_logger.info('Working on MenuItem.csv …')
    pipeline_logger.info('Reformatting item dates …')
    LATEST_ITEM_DATA_DF['item_created_at'] = LATEST_ITEM_DATA_DF.created_at.map(reformat_dates)
    LATEST_ITEM_DATA_DF['item_updated_at'] = LATEST_ITEM_DATA_DF.updated_at.map(reformat_dates)
    pipeline_logger.info('Date reformatting complete …')
    
    discarded_columns2 = [n for n in LATEST_ITEM_DATA_DF.columns if n not in 
                      ['id', 'menu_page_id', 'xpos', 'ypos', 'item_created_at', 'item_updated_at']]
    pipeline_logger.info('Discarding columns from MenuItem.csv …')
    for discard2 in discarded_columns2:
        pipeline_logger.info('{0} … removed'.format(discard2))
        
    TRIMMED_ITEM_DATA_DF = LATEST_ITEM_DATA_DF[['id', 'menu_page_id', 'xpos', 'ypos',
                                           'item_created_at', 'item_updated_at']]
    #TRIMMED_ITEM_DATA_DF.head()
    
    # =================================
    # 
    # MenuPage.csv
    #
    # =================================
    
    pipeline_logger.info('Working on MenuPage.csv …')
    LATEST_PAGE_DATA_DF[['full_height', 'full_width']].astype(int, raise_on_error=False)
    
    # =================================
    # 
    # Menu.csv
    #
    # =================================
    
    pipeline_logger.info('Working on Menu.csv …')
    discarded_columns3 = [n for n in LATEST_MENU_DATA_DF.columns if n not in 
                      ['sponsor', 'location', 'date', 'page_count', 'dish_count']]
    pipeline_logger.info('Discarding columns from Menu.csv …')
    for discard3 in discarded_columns3:
        pipeline_logger.info('{0} … removed'.format(discard3))
    
    TRIMMED_MENU_DATA_DF = LATEST_MENU_DATA_DF[['sponsor', 'location', 'date',
                                            'page_count', 'dish_count']]
    #TRIMMED_MENU_DATA_DF.head()
    
    # =================================
    # 
    # Merged DataFrames
    #
    # =================================
    
    pipeline_logger.info('Merging dataframes …')
    MERGED_ITEM_PAGES_DF = pd.merge(TRIMMED_ITEM_DATA_DF, LATEST_PAGE_DATA_DF, 
                                left_on='menu_page_id', right_index=True, )
    
    MERGED_ITEM_PAGES_DF.columns = ['item_id', 'menu_page_id', 'xpos', 'ypos', 
                                'item_created_at', 'item_updated_at', 'menu_id', 'page_number', 
                                'image_id', 'full_height', 'full_width', 'uuid']
    #MERGED_ITEM_PAGES_DF.head()
    
    MERGED_ITEM_PAGES_MENUS_DF = pd.merge(TRIMMED_MENU_DATA_DF, MERGED_ITEM_PAGES_DF, 
                                      left_index=True, right_on='menu_id')
    
    FULL_MERGE = pd.merge(MERGED_ITEM_PAGES_MENUS_DF, TRIMMED_DISH_DATA_DF, 
                      left_index=True, right_index=True)
    #FULL_MERGE.head()
    
    FOR_JSON_OUTPUT = FULL_MERGE.reset_index()
    
    FOR_JSON_OUTPUT.columns
    renamed_columns = ['dish_id', 'menu_sponsor', 'menu_location', 'menu_date', 'menu_page_count', 
                   'menu_dish_count', 'item_id', 'menu_page_id', 'item_xpos', 'item_ypos', 
                   'item_created_at', 'item_updated_at', 'menu_id', 'menu_page_number', 'image_id', 
                   'page_image_full_height', 'page_image_full_width', 'page_image_uuid', 'dish_name', 
                   'dish_menus_appeared', 'dish_times_appeared', 'dish_normalized_name', 'dish_name_fingerprint']
    FOR_JSON_OUTPUT.columns = renamed_columns
    
    FOR_JSON_OUTPUT[['menu_page_number', 'dish_id', 'item_id', 'menu_page_id', 'menu_id']].astype(int, raise_on_error=False)
    
    FOR_JSON_OUTPUT['dish_uri']= FOR_JSON_OUTPUT.dish_id.map(lambda x: 'http://menus.nypl.org/dishes/{0}'.format(int(x)))
    FOR_JSON_OUTPUT['item_uri']= FOR_JSON_OUTPUT.item_id.map(lambda x: 'http://menus.nypl.org/menu_items/{0}/edit'
                                               .format(int(x)))
    FOR_JSON_OUTPUT['menu_page_uri'] = FOR_JSON_OUTPUT.menu_page_id.map(lambda x: 'http://menus.nypl.org/menu_pages/{0}'
                                                          .format(int(x)))
    FOR_JSON_OUTPUT['menu_uri'] = FOR_JSON_OUTPUT.menu_id.map(lambda x:'http://menus.nypl.org/menus/{0}'
                                                .format(int(x)))
    
    FOR_JSON_OUTPUT.fillna('null')
    
    pipeline_logger.info('Merged dataframe ready')
    #FOR_JSON_OUTPUT.head()
    
    # df.iterrows is a generator that yields a positional index and a Series,
    # call the to_json method on the series
    return (reshape_data(row.to_json()) for i, row in FOR_JSON_OUTPUT.iterrows())


if __name__ == '__main__':

    pipeline_logger.info('Menus ETL Pipeline: Starting run …')
    c = Counter()

    try:
        client = server()
        actioner = load_and_tranform(os.environ['MENUS_SOURCE_DATA'])

        pipeline_logger.info('Preparing data to load into Elasticsearch …')
        for ok, result in helpers.streaming_bulk(client, actioner, chunk_size=1000):
            action, result = result.popitem()
            doc_id = '/menus/item/{0}'.format(result['_id'])
            if not ok:
                pipeline_logger.error('Failed to {0} document {1}: {2}'.format(action, doc_id, result['error']))
            else:
                c[doc_id] += 1
                
            pipeline_logger.info('{0} action succeeded for {1} documents'.format(action, sum(c.values())))
            
    except BaseException as e:
        pipeline_logger.error('Something went wrong: {0}'.format(str(e)))
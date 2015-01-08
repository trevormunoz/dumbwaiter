
# coding: utf-8

# # A Repeatable Extract-Tranform-Load Pipeline for NYPL Menus Data
# 
# **Created:** 17 October 2014
# 
# **Updated:** 28 October 2014, 19 November 2014, 3 December 2014, 14 December 2014, 8 January 2015
# 
# **Authors:** Trevor Muñoz and Katie Rawson
# 
# &nbsp;

# Have you set environment variables?: 
# 
# * MENUS_LOG_HOME
# * MENUS_SOURCE_DATA
# * MENUS_OUTPUT_DIR
# * MENUS_ES_HOSTNAME, 
# * MENUS_ES_HOST_PORT

# ### Acquiring Data (Extract)

# At the moment, the anti-bot protections on the New York Public Library's Web site make it impossible to grab the latest data download link directly. So, we download the data (as gzipped tar file) and package it with this script. 
# 
# The first stage of the pipeline unzips and untars the archive into four CSV files:
# 
# * Dish.csv
# * Menu.csv
# * MenuItem.csv
# * MenuPage.csv
# 
# &nbsp;

# In[1]:

import os
import datetime
import time
import tarfile


# Set up logging …

# In[2]:

import logging
import logging.handlers


# In[3]:

LOG_FILENAME = os.path.join(os.environ['MENUS_LOG_HOME'], 'nypl_menus_data_transform.log')

pipeline_logger = logging.getLogger('MenusDataTransformLogger')
pipeline_logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=500000, backupCount=10)
handler.setFormatter(formatter)
pipeline_logger.addHandler(handler)


# In[5]:

srcfile = [x for x in os.listdir(os.environ['MENUS_SOURCE_DATA']) if os.path.splitext(x)[1] == '.tgz'][0]


# In[6]:

tar = tarfile.open(os.path.join(os.environ['MENUS_SOURCE_DATA'], srcfile))


# In[7]:

pipeline_logger.info('Listing contents of the tar package …')
for tf in tar.getmembers():
    pipeline_logger.info('Name: {0} \t Last Modified: {1}'.format(tf.name, time.ctime(tf.mtime)))


# In[8]:

pipeline_logger.info('Extracting files to data directory.')
tar.extractall(path=os.environ['MENUS_SOURCE_DATA'])

for f in os.listdir(os.environ['MENUS_SOURCE_DATA']):
    if f.endswith('csv'):
        if os.path.isfile(os.path.join(os.environ['MENUS_SOURCE_DATA'], f)) == True:
            pipeline_logger.info('{0} … \u2713'.format(f))


# In[9]:

tar.close()


# ### Working with Data in DataFrames (Tranform)
# 
# &nbsp;

# In[10]:

import re
import json
import pytz
import pandas as pd


# In[11]:

pipeline_logger.info('Loading data into memory …')

LATEST_DISH_DATA_DF = pd.DataFrame.from_csv(os.path.join(os.environ['MENUS_SOURCE_DATA'], 'Dish.csv'), 
                                            index_col='id')
LATEST_ITEM_DATA_DF = pd.DataFrame.from_csv(os.path.join(os.environ['MENUS_SOURCE_DATA'], 'MenuItem.csv'), 
                                            index_col='dish_id')
LATEST_PAGE_DATA_DF = pd.DataFrame.from_csv(os.path.join(os.environ['MENUS_SOURCE_DATA'], 'MenuPage.csv'), 
                                            index_col='id')
LATEST_MENU_DATA_DF = pd.DataFrame.from_csv(os.path.join(os.environ['MENUS_SOURCE_DATA'], 'Menu.csv'),
                                             index_col='id')

pipeline_logger.info('Data loaded')


# ##### Dish.csv
# 

# In[12]:

NULL_APPEARANCES = LATEST_DISH_DATA_DF[LATEST_DISH_DATA_DF.times_appeared == 0]


# In[13]:

pipeline_logger.info('Data set contains {0} dishes that appear 0 times …'.format(
    len(NULL_APPEARANCES))
)


# In[14]:

NON_NULL_DISH_DATA_DF = LATEST_DISH_DATA_DF[LATEST_DISH_DATA_DF.times_appeared != 0]


# In[15]:

discarded_columns = [n for n in NON_NULL_DISH_DATA_DF.columns if n not in 
                     ['name', 'menus_appeared', 'times_appeared']]


# In[16]:

pipeline_logger.info('Discarding columns from Dish.csv …')
for discard in discarded_columns:
    pipeline_logger.info('{0} … removed'.format(discard))


# In[17]:

TRIMMED_DISH_DATA_DF = NON_NULL_DISH_DATA_DF[['name', 'menus_appeared', 'times_appeared']]


# In[18]:

pipeline_logger.info('Dish.csv contains {0} potentially-unique dish names before any normalization'.
                     format(TRIMMED_DISH_DATA_DF.name.nunique()))


# In[19]:

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


# In[20]:

TRIMMED_DISH_DATA_DF['normalized_name'] = TRIMMED_DISH_DATA_DF.name.map(normalize_names)


# In[21]:

pipeline_logger.info(
    'Dish.csv contains {0} potentially-unique dish names after normalizing whitespace and punctuation'
    .format(TRIMMED_DISH_DATA_DF.normalized_name.nunique())
)


# In[22]:

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


# In[23]:

TRIMMED_DISH_DATA_DF['fingerprint'] = TRIMMED_DISH_DATA_DF.normalized_name.map(fingerprint)


# In[24]:

pipeline_logger.info(
    'Dish.csv contains {0} unique fingerprint values'
    .format(TRIMMED_DISH_DATA_DF.fingerprint.nunique())
)


# In[25]:

#TRIMMED_DISH_DATA_DF.head()


# ##### MenuItem.csv
# 

# In[26]:

# TOFIX: This code is very slow
utc = pytz.utc
def reformat_dates(obj):
    naive_date_in = datetime.datetime.strptime(obj, '%Y-%m-%d %H:%M:%S %Z')
    date_in = naive_date_in.replace(tzinfo=utc)
    date_out = date_in.strftime('%Y%m%dT%H%M%S%z')
    return date_out


# In[27]:

pipeline_logger.info('Reformatting item dates …')
LATEST_ITEM_DATA_DF['item_created_at'] = LATEST_ITEM_DATA_DF.created_at.map(reformat_dates)
LATEST_ITEM_DATA_DF['item_updated_at'] = LATEST_ITEM_DATA_DF.updated_at.map(reformat_dates)
pipeline_logger.info('Date reformatting complete …')


# In[28]:

discarded_columns2 = [n for n in LATEST_ITEM_DATA_DF.columns if n not in 
                      ['id', 'menu_page_id', 'xpos', 'ypos', 'item_created_at', 'item_updated_at']]


# In[29]:

pipeline_logger.info('Discarding columns from MenuItem.csv …')
for discard2 in discarded_columns2:
    pipeline_logger.info('{0} … removed'.format(discard2))


# In[30]:

TRIMMED_ITEM_DATA_DF = LATEST_ITEM_DATA_DF[['id', 'menu_page_id', 'xpos', 'ypos',
                                           'item_created_at', 'item_updated_at']]


# In[31]:

#TRIMMED_ITEM_DATA_DF.head()


# ##### MenuPage.csv

# In[32]:

#LATEST_PAGE_DATA_DF.head()


# In[33]:

LATEST_PAGE_DATA_DF[['full_height', 'full_width']].astype(int, raise_on_error=False)


# ##### Menu.csv

# In[34]:

LATEST_MENU_DATA_DF.columns


# In[35]:

discarded_columns3 = [n for n in LATEST_MENU_DATA_DF.columns if n not in 
                      ['sponsor', 'location', 'date', 'page_count', 'dish_count']]


# In[36]:

pipeline_logger.info('Discarding columns from Menu.csv …')
for discard3 in discarded_columns3:
    pipeline_logger.info('{0} … removed'.format(discard3))


# In[37]:

TRIMMED_MENU_DATA_DF = LATEST_MENU_DATA_DF[['sponsor', 'location', 'date',
                                            'page_count', 'dish_count']]


# In[38]:

#TRIMMED_MENU_DATA_DF.head()


# ##### Merging DataFrames

# In[39]:

MERGED_ITEM_PAGES_DF = pd.merge(TRIMMED_ITEM_DATA_DF, LATEST_PAGE_DATA_DF, 
                                left_on='menu_page_id', right_index=True, )


# In[40]:

MERGED_ITEM_PAGES_DF.columns = ['item_id', 'menu_page_id', 'xpos', 'ypos', 
                                'item_created_at', 'item_updated_at', 'menu_id', 'page_number', 
                                'image_id', 'full_height', 'full_width', 'uuid']


# In[41]:

#MERGED_ITEM_PAGES_DF.head()


# In[42]:

MERGED_ITEM_PAGES_MENUS_DF = pd.merge(TRIMMED_MENU_DATA_DF, MERGED_ITEM_PAGES_DF, 
                                      left_index=True, right_on='menu_id')


# In[43]:

FULL_MERGE = pd.merge(MERGED_ITEM_PAGES_MENUS_DF, TRIMMED_DISH_DATA_DF, 
                      left_index=True, right_index=True)


# In[44]:

FULL_MERGE.head()


# In[45]:

FOR_JSON_OUTPUT = FULL_MERGE.reset_index()


# In[46]:

FOR_JSON_OUTPUT.columns


# In[47]:

renamed_columns = ['dish_id', 'menu_sponsor', 'menu_location', 'menu_date', 'menu_page_count', 
                   'menu_dish_count', 'item_id', 'menu_page_id', 'item_xpos', 'item_ypos', 
                   'item_created_at', 'item_updated_at', 'menu_id', 'menu_page_number', 'image_id', 
                   'page_image_full_height', 'page_image_full_width', 'page_image_uuid', 'dish_name', 
                   'dish_menus_appeared', 'dish_times_appeared', 'dish_normalized_name', 'dish_name_fingerprint']


# In[48]:

FOR_JSON_OUTPUT.columns = renamed_columns


# In[49]:

FOR_JSON_OUTPUT[['menu_page_number', 'dish_id', 'item_id', 'menu_page_id', 'menu_id']].astype(int, raise_on_error=False)


# In[50]:

FOR_JSON_OUTPUT['dish_uri']= FOR_JSON_OUTPUT.dish_id.map(lambda x: 'http://menus.nypl.org/dishes/{0}'.format(int(x)))


# In[51]:

FOR_JSON_OUTPUT['item_uri']= FOR_JSON_OUTPUT.item_id.map(lambda x: 'http://menus.nypl.org/menu_items/{0}/edit'
                                               .format(int(x)))


# In[52]:

FOR_JSON_OUTPUT['menu_page_uri'] = FOR_JSON_OUTPUT.menu_page_id.map(lambda x: 'http://menus.nypl.org/menu_pages/{0}'
                                                          .format(int(x)))


# In[53]:

FOR_JSON_OUTPUT['menu_uri'] = FOR_JSON_OUTPUT.menu_id.map(lambda x:'http://menus.nypl.org/menus/{0}'
                                                .format(int(x)))


# In[54]:

#FOR_JSON_OUTPUT.head()


# In[55]:

#FINAL_JSON_FP = os.path.join(os.environ['OUTPUTS_DIR'], 'transformed-menus-data-{0}.json'
#                             .format(datetime.date.today()))


# In[56]:

from io import StringIO
buf = StringIO()


# In[57]:

pipeline_logger.info('Generating JSON …')
FOR_JSON_OUTPUT.to_json(path_or_buf=buf, orient='index', force_ascii=False)
#pipeline_logger.info('JSON saved to {0}'.format(FINAL_JSON_FP))
pipeline_logger.info('JSON ready')


# This JSON needs to be reshaped …

# In[58]:

pipeline_logger.info('Loading JSON …')
ALL_JSON_DATA = json.loads(buf.getvalue())
pipeline_logger.info('JSON Loaded …')


# In[ ]:

def reshape_json(obj):
    '''
    Takes a dictionary, spits out a dictionary of slightly different shape
    needed for bulk import by Elasticsearch
    '''
    action = {
              "_index": "menus",
              "_type": "item",
              "_id": int(obj['item_id']),
              "_source": obj
              }
    return action


# In[ ]:

#Why is this operation taking so long???

pipeline_logger.info('Preparing data to load into Elasticsearch …')

actions = [reshape_json(d) for d in ALL_JSON_DATA.values()]

pipeline_logger.info('{0} actions ready for loading …'.format(len(actions)))


# In[ ]:

#actions[0]


# ### Load (to Elasticsearch)

# In[ ]:

import elasticsearch


# In[ ]:

os.environ['MENUS_ES_HOSTNAME'] = 'localhost'
os.environ['MENUS_ES_HOST_PORT'] = '5000'


# In[ ]:

es = elasticsearch.Elasticsearch([{'host': os.environ['MENUS_ES_HOSTNAME'], 'port': os.environ['MENUS_ES_HOST_PORT']}])


# In[ ]:

INDEX_NAME = 'menus'
TYPE_NAME = 'item'


# In[ ]:

#es.info()


# Check if the index already exists and, if so, delete it to keep things idempotent

# In[ ]:

if es.indices.exists(INDEX_NAME):
    pipeline_logger.info("deleting '{0}' index...".format(INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    pipeline_logger.info(" response: '{0}'".format(res))


# In[ ]:

pipeline_logger.info("creating '{0}' index...".format(INDEX_NAME))
res = es.indices.create(index = INDEX_NAME)
pipeline_logger.info(" response: '{0}'".format(res))


# In[ ]:

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


# In[ ]:

pipeline_logger.info("Creating mapping for '{0}' index...".format(INDEX_NAME))
res = es.indices.put_mapping(index='menus', doc_type='item', body=item_mapping)
pipeline_logger.info(" response: '{0}'".format(res))


# In[ ]:

def chunk_actions(l, size):
    for i in range(0, len(l), size):
        yield l[i:i+size]


# In[ ]:

from elasticsearch import helpers

pipeline_logger.info('Loading data into Elasticsearch …')

for batch in chunk_actions(actions, 50000):
    pipeline_logger.info(es.indices.stats(INDEX_NAME))
    helpers.bulk(es, batch)

pipeline_logger.info('Elasticsearch index ready!')


# In[ ]:




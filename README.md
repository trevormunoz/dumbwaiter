# Dumbwaiter
##### *An ETL pipeline for NYPL menus data*

Script for processing open data released by the 
New York Public Library's 
[*What's on the Menu?*](http://menus.nypl.org) project.

#### Setup

`dumbwaiter` depends on the [`pandas`](http://pandas.pydata.org/) data analysis library, which in turn depends on several of the numerical and scientific computing libraries available for python. Since these can be a pain to install, we recommend using the [Anaconda](https://store.continuum.io/cshop/anaconda/) distribution to manage and isolate these dependencies:

1. [Install Anaconda](http://continuum.io/downloads) for your platform
2. Create a new environment `conda create -n dumbwaiter python=3.4 pandas pip pytz --yes`
3. Follow the instructions in your terminal to activate and use this environment
4. Proceed with the standard python install below

```
git clone https://github.com/trevormunoz/dumbwaiter.git
cd dumbwaiter
pip install -r requirements.txt
python setup.py install
```

#### Usage
When you install you will get a command line program `dumbwaiter` which takes a required path to a directory containing a copy of the NYPL's data file as a `tar.gz` (this file is available [to download here](http://menus.nypl.org/data)).

```
$ dumbwaiter /path/to/data/dir
```

`dumbwaiter` expects an elasticsearch server to be running to accept data as bulk uploads &mdash; you can optionally specify the hostname and port to use:

```
$ dumbwaiter /path/to/data/dir -s localhost -p 9200
```


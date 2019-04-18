"""
Helper functions that can display leaflet maps inline in an ipython notebook
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import *

import branca.element as bre
 
def inline_map(m):
    """
    Embeds the HTML source of the map directly into the IPython notebook.
    
    This method will not work if the map depends on any files (json data). Also this uses
    the HTML5 srcdoc attribute, which may not be supported in all browsers.
    """
    fig = bre.Figure()
    fig.add_subplot(1,1,1).add_child(m)
    return fig

def inline_maps(map_list):
    """
    Embeds the HTML source of the map_list directly into the IPython notebook.
    
    This method will not work if the map depends on any files (json data). Also this uses
    the HTML5 srcdoc attribute, which may not be supported in all browsers.

    map_list: 2-D array of maps. dimensions should be [nRows][nCols]. The method will throw a RuntimeError if not
    nRows: Number of rows
    nCols: Number of columns
    """
    ncols = 2
    nrows = (len(map_list)/ncols) + 1
    fig = bre.Figure()
    for i, m in enumerate(map_list):
        fig.add_subplot(nrows,ncols,i+1).add_child(m)
    return fig

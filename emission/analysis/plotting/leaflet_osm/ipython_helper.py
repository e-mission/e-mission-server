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
import IPython.display as idisp
import html as hgen
 
def inline_map(map):
    """
    Embeds the HTML source of the map directly into the IPython notebook.
    
    This method will not work if the map depends on any files (json data). Also this uses
    the HTML5 srcdoc attribute, which may not be supported in all browsers.
    """
    map._build_map()
    return idisp.HTML('<iframe srcdoc="{srcdoc}" style="width: 100%; height: 510px; border: none"></iframe>'.format(srcdoc=map.HTML.replace('"', '&quot;')))

def inline_maps(map_list):
    """
    Embeds the HTML source of the map_list directly into the IPython notebook.
    
    This method will not work if the map depends on any files (json data). Also this uses
    the HTML5 srcdoc attribute, which may not be supported in all browsers.

    map_list: 2-D array of maps. dimensions should be [nRows][nCols]. The method will throw a RuntimeError if not
    nRows: Number of rows
    nCols: Number of columns
    """
    nRows = len(map_list)
    # nCols = max([len(row) for row in map_list])
    hb = hgen.HTML()
    t = hb.table(width="100%")
    for r in range(nRows):
        row = t.tr
        for c in range(len(map_list[r])):
            currMap = map_list[r][c]
            currMap._build_map()
            row.td('<iframe srcdoc="{srcdoc}" style="width: 100%; height: 510px; border: none"></iframe>'.format(srcdoc=currMap.HTML.replace('"', '&quot;')))
    return idisp.HTML('<iframe srcdoc="{srcdoc}" style="width: 100%; height: {ht}px; border: none"></iframe>'.format(srcdoc=str(t).replace('"', '&quot;'), ht=510*nRows))
 
def embed_map(map, path="map.html"):
    """
    Embeds a linked iframe to the map into the IPython notebook.
    
    Note: this method will not capture the source of the map into the notebook.
    This method should work for all maps (as long as they use relative urls).
    """
    map.create_map(path=path)
    return idisp.IFrame(src="files/{path}".format(path=path), width="100%", height="510")

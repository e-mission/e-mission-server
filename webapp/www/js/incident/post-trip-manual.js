'use strict';

angular.module('emission.incident.posttrip.manual', ['emission.plugin.logger'])
.factory('PostTripManualMarker', function($window, $state, $ionicActionSheet, Logger) {
  var ptmm = {};

  var MULTI_PASS_THRESHOLD = 90;
  var MANUAL_INCIDENT = "manual/incident";

  // BEGIN: Adding incidents

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Returns objects of the form: {
   * gj: geojson representation,
   * latlng: latlng representation,
   * ts: timestamp
   * }
   */

  var getSectionPoints = function(section) {
    var mappedPoints = section.geometry.coordinates.map(function(currCoords, index) {
      var currMappedPoint = {gj: currCoords,
        latlng: L.GeoJSON.coordsToLatLng(currCoords),
        ts: section.properties.times[index]}
      if (index % 100 == 0) {
        Logger.log("Mapped point "+ JSON.stringify(currCoords)+" to "+currMappedPoint);
      }
      return currMappedPoint;
    });
    return mappedPoints;
  }

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Functor passed in to sort points by distance. We use this to find
   * the closest points.
   */

  var sortPointsByDistance = function(a, b) {
    return a.selDistance - b.selDistance;
  }

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Functor passed in to sort points by time. We use this to bin the closest
   * points and determine if there wre multiple distinct passes through the
   * same location.
   */

  var sortPointsByTime = function(a, b) {
    return a.ts - b.ts;
  }

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Get the closest points from the trip to the point that the user clicked.
   * Uses sortPointsByDistance.
   */

  var getClosestPoints = function(selPoint, allPoints)  {
    var selPointLatLng = L.GeoJSON.coordsToLatLng(selPoint.geometry.coordinates);
    // Add distance to the selected point to the properties
    var sortedPoints = angular.copy(allPoints);
    sortedPoints.forEach(function(currPoint) {
        currPoint.selDistance = selPointLatLng.distanceTo(currPoint.latlng);
    });
    sortedPoints.sort(sortPointsByDistance);
    return sortedPoints;
  }

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * It would be great to just use the timestamp of the closest point.
   * HOWEVER, we could have round trips in which we pass by the same point
   * multiple times. So first we need to see how many times we go through
   * the point. We do this by determining the time difference between the n
   * closest points. If we pass by the point only once, we expect that all
   * of the points will be from the same section and will be 30 secs apart
   * But if we pass by the point multiple times, the points will be from
   * different sections and will be greater than 30 secs apart.
   *
   * To make this customizable, we use the MULTI_PASS_THRESHOLD of 90 secs
   * of 30 secs.
   *
   * This uses sortPointsByTime
   */

  var getTimeBins = function(closestPoints) {
      var sortedTsList = angular.copy(closestPoints).sort(sortPointsByTime);
      sortedTsList.forEach(function(currItem, i) {
        if (i == 0) {
            currItem.timeDiff = 0;
        } else {
            currItem.timeDiff = currItem.ts - sortedTsList[i - 1].ts;
        }
      });
      var timeDiffList = sortedTsList.map(function(currItem) {
        return currItem.timeDiff;
      });
      var maxDiff = Math.max.apply(0, timeDiffList);

      // We take the calculated time differences and split them into bins
      // common case: one bin
      // multiple passes: multiple bins
      var timeBins = [];
      var currBin = [];
      timeBins.push(currBin);
      sortedTsList.forEach(function(currItem){
        if (currItem.timeDiff > MULTI_PASS_THRESHOLD) {
          currBin = [];
          timeBins.push(currBin);
        }
        currBin.push(currItem);
      });
      Logger.log("maxDiff = " + maxDiff);
      return timeBins;
  };

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Add a safe (green, stress = 0) entry.
   */

  var addSafeEntry = function(latlng, ts, marker, event, map) {
    // marker.setStyle({color: 'green'});
    return addEntry(MANUAL_INCIDENT, "green", latlng, ts, 0, marker)
  };

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Add a suck (red, stress = 100) entry.
   */

  var addSuckEntry = function(latlng, ts, marker, event, map) {
    return addEntry(MANUAL_INCIDENT, "red", latlng, ts, 100, marker)
  };

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Generic addEntry, called from both safe and suck
   */

  var addEntry = function(key, newcolor, latlng, ts, stressLevel, marker) {
    // marker.setStyle({color: newcolor});
    var value = {
        loc: marker.toGeoJSON().geometry,
        ts: ts,
        stress: stressLevel
    }
    $window.cordova.plugins.BEMUserCache.putMessage(key, value)
    return value;
  }

  /*
   * INTERNAL FUNCTION, not part of factory
   *
   * Allow the user to cancel the insertion of the marker
   */

  var cancelTempEntry = function(latlng, ts, marker, event, map) {
    map.removeLayer(marker);
  }

  /*
   * EXTERNAL FUNCTION
   *
   * Converts the incident to a geojson feature.
   * Maybe we should do something similar for all entries (trip/section)
   * as well.
   */

  ptmm.toGeoJSONFeature = function(incident) {
    console.log("About to convert"+incident.loc);
    var newFeature = L.GeoJSON.asFeature(incident.loc);
    newFeature.properties = angular.copy(incident);
    delete newFeature.properties.loc;
    newFeature.properties.feature_type = "incident";
    return newFeature;
  };

  /*
   * INTERNAL FUNCTION:
   *
   */

  var showSheet = function(section, latlng, ts, marker, e, map) {
    var safe_suck_cancel_actions = [{text: "Safe",
                                     action: addSafeEntry},
                                    {text: "Suck",
                                     action: addSuckEntry}]

    $ionicActionSheet.show({titleText: "lat: "+latlng.lat.toFixed(6)
              +", lng: " + latlng.lng.toFixed(6)
              + " at " + getFormattedTime(ts),
          cancelText: 'Cancel',
          cancel: function() {
            cancelTempEntry(latlng, ts, marker, e, map);
          },
          buttons: safe_suck_cancel_actions,
          buttonClicked: function(index, button) {
              var newEntry = button.action(latlng, ts, marker, e, map);
              /*
               * The markers are now displayed using the trip geojson. If we only
               * store the incidents to the usercache and don't add it to the geojson
               * it will look like the incident is deleted until we refresh the trip
               * information by pulling to refresh. So let's add to the geojson as well.
               */
              var newFeature = ptmm.toGeoJSONFeature(newEntry);
              // var trip = Timeline.getTrip(section.properties.trip_id.$oid);
              // trip.features.push(newFeature);
              // And one that is done, let's remove the temporary marker
              cancelTempEntry(latlng, ts, marker, e, map);
              return true;
          }
    });
  }

  var getFormattedTime = function(ts_sec) {
    return moment(ts_sec * 1000).format('LT');
  }

  /*
   * EXTERNAL FUNCTION, part of factory, bound to the section to report
   * an incident on it. Note that this is a function that takes in the feature,
   * but it needs to return a curried function that takes in the event.
   */

  ptmm.startAddingIncident = function(feature, layer) {
      Logger.log("section "+feature.properties.start_fmt_time
                  + " -> "+feature.properties.end_fmt_time
                  + " bound incident addition ")

      return function(e) {
          Logger.log("section "+feature.properties.start_fmt_time
                      + " -> "+feature.properties.end_fmt_time
                      + " received click event, adding stress popup at "
                      + e.latlng);
          if ($state.$current == "root.main.diary") {
            Logger.log("skipping incident addition in list view");
            return;
          }
          var map = layer._map;
          var latlng = e.latlng;
          var marker = L.circleMarker(latlng).addTo(map);

          var sortedPoints = getClosestPoints(marker.toGeoJSON(), getSectionPoints(feature));
          var closestPoints = sortedPoints.slice(0,10);
          Logger.log("Closest 10 points are "+ closestPoints.map(JSON.stringify));

          var timeBins = getTimeBins(closestPoints);
          Logger.log("number of bins = " + timeBins.length);


          if (timeBins.length == 1) {
            // Common case: find the first item in the first time bin, no need to
            // prompt
            var ts = timeBins[0][0].ts;
            showSheet(feature, latlng, ts, marker, e, map);
          } else {
            // Uncommon case: multiple passes - get the closest point in each bin
            // Note that this may not be the point with the smallest time diff
            // e.g. if I am going to school and coming back, when sorted by time diff,
            // the points will be ordered from home to school on the way there and
            // from school to home on the way back. So if the incident happened
            // close to home, the second bin, sorted by time, will have the first
            // point as the point closest to school, not to home. We will need to
            // re-sort based on distance. Or we can just accept an error in the timestamp,
            // which will be a max of 5 * 30 secs = 2.5 minutes
            // Let's accept the error for now and fix later.
            // Example: 8:06 - 8:48 on 16 Nov on iPhone3, around 3pm on 16 Nov on
            var tsOptions = timeBins.map(function(bin) {
              return bin[0].ts;
            });
            Logger.log("tsOptions = " + tsOptions);
            var timeSelActions = tsOptions.map(function(ts) {
              return {text: getFormattedTime(ts),
                      selValue: ts};
            });
            $ionicActionSheet.show({titleText: "Choose incident time",
              buttons: timeSelActions,
              buttonClicked: function(index, button) {
                var ts = button.selValue;
                showSheet(feature, latlng, ts, marker, e, map);
                return true;
              }
            });
          }
      };
  };

  // END: Adding incidents

  // BEGIN: Displaying incidents

  // Once the incidents have made it to the server, they will show up as part
  // of the timeline geojson. But for newly added incidents that have not yet
  // been pushed, we should read from the usercache and display so that we don't
  // end up "missing incidents" until they are pushed.

  /*
   * EXTERNAL FUNCTION: part of factory, invoked to display unpushed incidents
   * for a particular trip
   */
  ptmm.addUnpushedIncidents = function(trip) {
    getUnpushedIncidents(trip).then(function(incidentList) {
      return filterAlreadyAdded(trip, incidentList);
    }).then(function(incidentList) {
      return displayIncidents(trip, incidentList);
    });
  };


  // We need to get all entries from the cache because we really want to filter
  // by the data ts not the write_ts, but the data_ts is part of the json and
  // cannot be queried
  var getUnpushedIncidents = function(trip) {
    return $window.cordova.plugins.BEMUserCache.getMessagesForInterval(MANUAL_INCIDENT,
      $window.cordova.plugins.BEMUserCache.getAllTimeQuery(),
      false).then(function(allIncidentList) {
        var filteredList = allIncidentList.filter(function(item) {
          return (trip.properties.start_ts <= item.ts) &&
                 (item.ts <= trip.properties.end_ts);
        });
        Logger.log("After filtering, entries went from "+allIncidentList.length
          + " to " + filteredList.length);
        return filteredList;
    });
  };

  var filterAlreadyAdded = function(trip, incidentList) {
    var existingTs = trip.features.filter(function(currFeature) {
      return (currFeature.type == "Feature") &&
        (currFeature.properties.feature_type == "incident");
    }).map(function(currFeature) {
      return currFeature.properties.ts;
    });
    Logger.log("existing incident ts = "+existingTs
      +" with length "+existingTs.length);
    var retList = incidentList.filter(function(currFeature) {
      return existingTs.indexOf(currFeature.ts) == -1;
    });
    Logger.log("After filtering existing length went from = "
      +existingTs.length +" to "+retList.length);
    return retList;
  };

  var displayIncidents = function(trip, incidentList) {
    console.log("About to display " + incidentList.map(JSON.stringify));
    var mappedList = incidentList.map(ptmm.toGeoJSONFeature);
    Logger.log("After mapping, "+mappedList.map(JSON.stringify))
    mappedList.forEach(function(newFeature) {
      trip.features.push(newFeature);
    });
  };

  /*
   * EXTERNAL FUNCTION: part of factory, invoked to display incident details
   */

  ptmm.displayIncident = function(feature, layer) {
    return layer.bindPopup(""+getFormattedTime(feature.properties.ts));
  };

  /*
   * EXTERNAL FUNCTION: format the incident as a colored marker
   */

  ptmm.incidentMarker = function(feature, latlng) {
    var m = L.circleMarker(latlng);
    if (feature.properties.stress == 0) {
      m.setStyle({color: "green"});
    } else {
      m.setStyle({color: "red"});
    }
    return m;
  };

  // END: Displaying incidents

  return ptmm;
});

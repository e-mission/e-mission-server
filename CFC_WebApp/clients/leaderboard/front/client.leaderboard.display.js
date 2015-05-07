angular.module('e-mission-leaderboard', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicPopup, $http) {
    // alert("RootCtrl initialized");

    /*
     * From http://stackoverflow.com/questions/3895478/does-javascript-have-a-range-equivalent
     * but not tested
     */
    $scope.function showscores() {
      var listContainer = document.createElement("div");
      document.getElementsByTagName("body")[0].appendChild(listContainer);
      var listElement = document.createElement("ul");
      listElement.className = "item";
      listContainer.appendChild(listElement);
      var numItems = otherCurrScoreList.length;
      var prev = otherCurrScoreList[numItems-1];
      var insert =  0;
      var user = 1;
      for (var i = numItems; i > 0; i--){
        var listItemContainer = document.createElement("li");
        listItemContainer.className = "item";
        listItemContainer.style.backgroundColor = "#d3d3d3";
        var listItem = document.createElement("i");
        listElement.appendChild(listItemContainer);
        listItem.innerHTML = otherCurrScoreList[i-1];
        listItem.style.color = "#545454";
        listItem.style.cssFloat = "right";
        if((currScore >= otherCurrScoreList[i-1]) && insert < 1){
          var scoreContainer = document.createElement("li");
          scoreContainer.className = "item";
          scoreContainer.style.backgroundColor="#7ebd96";
          listElement.appendChild(scoreContainer);
          var yourScore = document.createElement("i");
          yourScore.innerHTML = currScore;
          yourScore.style.fontWeight = "900";
          yourScore.style.cssFloat = "right";
          var youName = document.createElement("i");
          youName.style.fontWeight = "900";
          youName.style.cssFloat = "left";
          youName.innerHTML = "You";
          scoreContainer.appendChild(youName);
          scoreContainer.appendChild(yourScore);
          insert++;
      }
      prev = otherCurrScoreList[i-1];
      var anon = document.createElement("i");
      anon.style.color = "#545454";
      anon.style.cssFloat = "left";
      anon.innerHTML = "User" + user;
      user ++;
      listItemContainer.appendChild(anon);
      listItemContainer.appendChild(listItem);
      if (i == 1 && insert < 1){
        var scoreContainer = document.createElement("li");
        scoreContainer.className="item";
        scoreContainer.style.backgroundColor="#7ebd96";
        listElement.appendChild(scoreContainer);
        var yourScore = document.createElement("i");
        yourScore.innerHTML = currScore;
        yourScore.style.fontWeight = "900";
        yourScore.style.cssFloat = "right";
        var youName = document.createElement("i");
        youName.style.fontWeight = "900";
        youName.style.cssFloat = "left";
        youName.innerHTML = "You";
        scoreContainer.appendChild(youName);
        scoreContainer.appendChild(yourScore);
        }
      }
    }
    function range(start, stop, step){
      var a=[start], b=start;
      while(b<stop){b+=step;a.push(b)}
      return a;
    };
});

/*
angular.module("e-mission-gamified", ['ionic'])


.controller('RootCtrl', function($scope) {
});
*/

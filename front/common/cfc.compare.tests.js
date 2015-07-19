test ("hello test", function() {
  ok( 1 == "1", "Passed!" );
});

test ("test getSum", function() {
  testObj = {'a': 1, 'b': 2, 'c': 0, 'd': 10};
  ok( cfc.compare.getSum(testObj) == 13, "Passed!");
});

test ("test filterZeros", function() {
  testObjA = {'a': 1, 'b': 2, 'c': 0, 'd': 0};
  testObjB = {'a': 1, 'b': 0, 'c': 0, 'd': 10};
  filteredArrays = cfc.compare.filterZeros(testObjA, testObjB);
  // Make sure that the result is returned in the right order
  ok(filteredArrays[0]['d'] == 0, "Passed!");
  ok(filteredArrays[1]['d'] == 10, "Passed!");

  ok(filteredArrays[1]['b'] == 0, "Passed!");
  ok(filteredArrays[0]['b'] == 2, "Passed!");

  filteredObjAKeys = []
  for (key in filteredArrays[0]) filteredObjAKeys.push(key);

  filteredObjBKeys = []
  for (key in filteredArrays[1]) filteredObjBKeys.push(key);

  ok( filteredObjAKeys.length == 3, "Passed!");
  ok( filteredObjBKeys.length == 3, "Passed!");
});

test ("test scaleValues", function() {
  testObjA = {'a': 1000, 'b': 2000, 'c': 0, 'd': 3000};
  testObjB = {'a': 1000, 'b': -1000, 'c': 0, 'd': 10000};

  scaledMapA = cfc.compare.scaleValues(testObjA, 1000);
  scaledMapB = cfc.compare.scaleValues(testObjB, 1000);

  ok(scaledMapA['a'] == 1, "Passed!");
  ok(scaledMapA['b'] == 2, "Passed!");
  ok(scaledMapA['c'] == 0, "Passed!");
  ok(scaledMapA['d'] == 3, "Passed!");

  ok(scaledMapB['a'] == 1, "Passed!");
  ok(scaledMapB['b'] == -1, "Passed!");
  ok(scaledMapB['c'] == 0, "Passed!");
  ok(scaledMapB['d'] == 10, "Passed!");
});

import matplotlib.pyplot as plt
import numpy as np

def showCategoryChart(nameList, countListList, countLabelList, countColorList, ylabel, title, cleanNameDict = None, figsize = None, width=0.35, barLabelSize=None):
    N = len(countListList[0])
    print(N)

    ind = np.arange(N)  # the x locations for the groups

    if figsize:
      fig, ax = plt.subplots(figsize=figsize)
    else: 
      fig, ax = plt.subplots()

    rectList = []
    for i, countList in enumerate(countListList):
      # print "Added bar for %s" % countList
      currInd = np.arange(len(countList))
      if countColorList != None:
        currColor = countColorList[i]
      else:
        currColor = np.random.rand(3,1)
      rectList.append(ax.bar(currInd + i * width, countList, width, color = currColor))
      # rectList.append(ax.bar(ind, countList, width, color = 'r'))
    
    cleanedNameList = []
    if cleanNameDict:
      for name in nameList:
          if name in cleanNameDict:
              cleanedNameList.append(cleanNameDict[name])
          else:
              cleanedNameList.append(name)
    else:
      cleanedNameList = nameList

    # add some
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(ind+width/2)
    # ax.set_xticklabels(cleanedNameList, rotation = 45, rotation_mode = "anchor")
    ax.set_xticklabels(cleanedNameList)

    print("len(rectList) = %d, len(countLabelList) = %d" % (len(rectList), len(countLabelList)))
    if len(countLabelList) > 1:
      if len(countLabelList) > 3:
        nCols = len(countLabelList) // 3
      else:
        nCols = len(countLabelList)
      plt.legend(rectList, countLabelList, loc="best", framealpha=0.3, ncol = nCols + 1)
      # ax.legend(rectList, countLabelList)

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                    ha='center', va='bottom', size=barLabelSize)

    if not barLabelSize == None:
      for rect in rectList:
        autolabel(rect)
    return (fig, ax)


def showHorizCategoryChart(nameList, countListList, countLabelList, countColorList, ylabel, title, cleanNameDict = None, figsize = None, width=0.35, barLabelSize=None):
    N = len(countListList[0])
    print(N)

    ind = np.arange(N)  # the x locations for the groups

    if figsize:
      fig, ax = plt.subplots(figsize=figsize)
    else: 
      fig, ax = plt.subplots()

    rectList = []
    for i, countList in enumerate(countListList):
      currInd = np.arange(len(countList))
      rectList.append(ax.barh(currInd + i * width, countList, width, color = countColorList[i]))
      # rectList.append(ax.bar(ind, countList, width, color = 'r'))
    
    cleanedNameList = []
    if cleanNameDict:
      for name in nameList:
          if name in cleanNameDict:
              cleanedNameList.append(cleanNameDict[name])
          else:
              cleanedNameList.append(name)
    else:
      cleanedNameList = nameList

    # add some
    ax.set_xlabel(ylabel)
    ax.set_title(title)
    ax.set_yticks(ind+width/2)
    # ax.set_xticklabels(cleanedNameList, rotation = 45, rotation_mode = "anchor")
    ax.set_yticklabels(cleanedNameList)

    if len(countLabelList) > 1:
      # ax.legend(rectList, countLabelList)
      plt.legend(rectList, countLabelList, loc="best", framealpha=0.5)

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            bw = rect.get_width()
            print(rect.get_y(), rect.get_height(), bw)
            ax.text(bw + 0.02 * (ax.get_xlim()[1] - ax.get_xlim()[0]),
                    rect.get_y()+rect.get_height()/2.,'%d'%int(bw),
                    ha='left', va='center', size=barLabelSize)

    for rect in rectList:
      autolabel(rect)
    return (fig, ax)

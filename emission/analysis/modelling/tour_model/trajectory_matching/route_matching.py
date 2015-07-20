# Standard imports
import numpy as np
import urllib,json,csv
import xml.etree.cElementTree as ET
import urllib2
import time

# Our imports
import emission.core.common as ec
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.trajectory_matching as eatm

def find_near(lst,pnt,radius):
    near=[]
    for i in range(len(lst)):
        # print(ec.calDistance(lst[i],pnt))
        if ec.calDistance(lst[i],pnt)<radius:
            near.append(i)
    return near

def find_nearest(lst,pnt):
    nearest=lst[0]
    dis=99999999
    for i in range(len(lst)):
        # print(ec.calDistance(lst[i],pnt))
        new_dis=ec.calDistance(lst[i],pnt)
        if new_dis<dis:
            dis=new_dis
            nearest=lst[i]
    print(dis)
    return nearest

def cal_matching_score(lst1,lst2,radius):
    len1=len(lst1)
    len2=len(lst2)
    max_len=max(len1,len2)
    count=0
    print('cal_matching start')
    # print(len1)
    # print(len2)
    # print(max_len)
    for i in range(max_len):
        # print(int(i/max_len*len1))
        # print(int(i/max_len*len2))
        # print(lst1[int(i/max_len*len1)])
        # print(lst2[int(i/max_len*len2)])
        if ec.Is_place_2(lst1[int(i/max_len*len1)],lst2[int(i/max_len*len2)],radius):
            count+=1
    score=count/max_len
    return score

def route_matching(lst1,lst2,step,radius,len_match,min_score):
    # input 2 lists of tracking points, each tracking points is geojson format
    # the two lists must have at least two tracking points
    if len(lst1)<2 or len(lst2)<2:
        return False
    start_pnt1=lst1[0]
    end_pnt1=lst1[-1]
    start_pnt2=lst2[0]
    end_pnt2=lst2[-1]
    # Case 1, lst2 is part of lst1:
    lst1_extended=[]
    for i in range(len(lst1)-1):
        dis=ec.calDistance(lst1[i]['track_location']['coordinates'],lst1[i+1]['track_location']['coordinates'])
        num_inter=int(round(dis/step))
        if num_inter==0:
            lst1_extended.append(lst1[i]['track_location']['coordinates'])
        else:
            lon_list=np.linspace(lst1[i]['track_location']['coordinates'][0],lst1[i+1]['track_location']['coordinates'][0],num_inter,False)
            lat_list=np.linspace(lst1[i]['track_location']['coordinates'][1],lst1[i+1]['track_location']['coordinates'][1],num_inter,False)
            for j in range(len(lon_list)):
                lst1_extended.append([lon_list[j],lat_list[j]])
    lst1_extended.append(end_pnt1['track_location']['coordinates'])
    lst2_extended=[]
    for i in range(len(lst2)-1):
        dis=ec.calDistance(lst2[i]['track_location']['coordinates'],lst2[i+1]['track_location']['coordinates'])
        num_inter=int(round(dis/step))
        if num_inter==0:
            lst2_extended.append(lst2[i]['track_location']['coordinates'])
        else:
            lon_list=np.linspace(lst2[i]['track_location']['coordinates'][0],lst2[i+1]['track_location']['coordinates'][0],num_inter,False)
            lat_list=np.linspace(lst2[i]['track_location']['coordinates'][1],lst2[i+1]['track_location']['coordinates'][1],num_inter,False)
            for j in range(len(lon_list)):
                lst2_extended.append([lon_list[j],lat_list[j]])
    lst2_extended.append(end_pnt2['track_location']['coordinates'])

    # print(len(lst1_extended))
    # print(len(lst2_extended))
    near_start2=find_near(lst1_extended,start_pnt2['track_location']['coordinates'],radius)
    near_end2=find_near(lst1_extended,end_pnt2['track_location']['coordinates'],radius)

    near_start1=find_near(lst2_extended,start_pnt1['track_location']['coordinates'],radius)
    near_end1=find_near(lst2_extended,end_pnt1['track_location']['coordinates'],radius)

    # print(near_start2)
    # print(near_end2)
    # print(near_start1)
    # print(near_end1)
    best_score=[]

    if len(near_start2)>0 and len(near_end2)>0:
        print("start of case 1")
        for near_s in near_start2:
            for near_e in near_end2:
                if min(abs(near_e-near_s)+1,len(lst2_extended))/max(abs(near_e-near_s)+1,len(lst2_extended))>=len_match:
                    print("possible near_s is %s" % near_s)
                    print("possible near_e is %s" % near_e)

                    if near_e>near_s:
                        print("start index is %d" % near_s)
                        print("end index is %d" % near_e)
                        route1=lst1_extended[near_s:near_e+1:1]

                        route2=lst2_extended
                        print("route1 is %s" % route1)
                        print("route2 is %s" % route2)
                    else:
                        print("start index is %d" % near_s)
                        print("end index is %d" % near_e)
                        route1=lst1_extended[near_e:near_s+1:1][::-1]
                        route2=lst2_extended
                        print("route1 is %s" % route1)
                        print("route2 is %s" % route2)
                    best_score.append(cal_matching_score(route1,route2,radius))

    if len(near_start1)>0 and len(near_end1)>0:
        print("start of case 2")
        for near_s in near_start1:
            for near_e in near_end1:
                if min(abs(near_e-near_s)+1,len(lst1_extended))/max(abs(near_e-near_s)+1,len(lst1_extended))>=len_match:
                    if near_e>near_s:
                        print("start index is %d" % near_s)
                        print("end index is %d" % near_e)
                        route1=lst1_extended
                        route2=lst2_extended[near_s:near_e+1:1]
                        print("route1 is %s" % route1)
                        print("route2 is %s" % route2)
                    else:
                        route1=lst1_extended
                        route2=lst2_extended[near_e:near_s+1:1][::-1]
                    best_score.append(cal_matching_score(route1,route2,radius))
    print(best_score)
    if len(best_score)>0 and max(best_score)>min_score:
        return True
    else:
        return False

def route_matching_2(lst1,lst2,step,radius,min_score):
    # input 2 lists of tracking points, each tracking points is geojson format
    # the two lists must have at least two tracking points
    if len(lst1)<2 or len(lst2)<2:
        return False
    start_pnt1=lst1[0]
    end_pnt1=lst1[-1]
    start_pnt2=lst2[0]
    end_pnt2=lst2[-1]
    # Case 1, lst2 is part of lst1:
    lst1_extended=[]
    for i in range(len(lst1)-1):
        dis=ec.calDistance(lst1[i]['track_location']['coordinates'],lst1[i+1]['track_location']['coordinates'])
        num_inter=int(round(dis/step))
        if num_inter==0:
            lst1_extended.append(lst1[i]['track_location']['coordinates'])
        else:
            lon_list=np.linspace(lst1[i]['track_location']['coordinates'][0],lst1[i+1]['track_location']['coordinates'][0],num_inter,False)
            lat_list=np.linspace(lst1[i]['track_location']['coordinates'][1],lst1[i+1]['track_location']['coordinates'][1],num_inter,False)
            for j in range(len(lon_list)):
                lst1_extended.append([lon_list[j],lat_list[j]])
    lst1_extended.append(end_pnt1['track_location']['coordinates'])
    lst2_extended=[]
    for i in range(len(lst2)-1):
        dis=ec.calDistance(lst2[i]['track_location']['coordinates'],lst2[i+1]['track_location']['coordinates'])
        num_inter=int(round(dis/step))
        if num_inter==0:
            lst2_extended.append(lst2[i]['track_location']['coordinates'])
        else:
            lon_list=np.linspace(lst2[i]['track_location']['coordinates'][0],lst2[i+1]['track_location']['coordinates'][0],num_inter,False)
            lat_list=np.linspace(lst2[i]['track_location']['coordinates'][1],lst2[i+1]['track_location']['coordinates'][1],num_inter,False)
            for j in range(len(lon_list)):
                lst2_extended.append([lon_list[j],lat_list[j]])
    lst2_extended.append(end_pnt2['track_location']['coordinates'])

    # print(len(lst1_extended))
    # print(len(lst2_extended))
    best_score=[]
    score_2_in_1=0
    for point2 in lst2:
        if ec.Include_place_2(lst1_extended,point2['track_location']['coordinates'],radius):
            score_2_in_1+=1
    best_score.append(score_2_in_1/len(lst2))
    score_1_in_2=0
    for point1 in lst1:
        if ec.Include_place_2(lst2_extended,point1['track_location']['coordinates'],radius):
            score_1_in_2+=1
    best_score.append(score_1_in_2/len(lst1))
    print(best_score)
    if max(best_score)>min_score:
        return True
    else:
        return False

def getRoute(section_id):
    route=[]
    Sections=edb.get_section_db()
    section=Sections.find_one({'_id':section_id})
    for point in section['track_points']:
        route.append(point['track_location']['coordinates'])
    return route

def refineRoute(lst1,step):
    if lst1 ==[]:
        return lst1

    # print(len(lst1))
    lst1_extended=[]
    for i in range(len(lst1)-1):
        dis=ec.calDistance(lst1[i],lst1[i+1])
        num_inter=int(round(dis/step))
        if num_inter==0:
            lst1_extended.append(lst1[i])
        else:
            lon_list=np.linspace(lst1[i][0],lst1[i+1][0],num_inter,False)
            lat_list=np.linspace(lst1[i][1],lst1[i+1][1],num_inter,False)
            for j in range(len(lon_list)):
                lst1_extended.append([lon_list[j],lat_list[j]])
    lst1_extended.append(lst1[-1])
    # print(len(lst1))
    # print(len(lst1_extended))
    return lst1_extended

def storeTransitStop(type,route):
    Transit=edb.get_transit_db()
    todo={}
    stops=[]
    tree = ET.ElementTree(file=urllib2.urlopen('http://api.bart.gov/api/stn.aspx?cmd=stns&key=MW9S-E7SL-26DU-VV8V'))
    root = tree.getroot()
    # print(root[1][0].find('name').text)
    file_name='/Users/Mogeng/Berkeley/Semester2/E-Mission/Transit_routes/'+type+'_'+route+'.csv'
    with open(file_name, 'rU') as csvfile:
        r = csv.reader(csvfile,delimiter=',',quotechar='|')
        for row in r:
            print(row[0])
            for i in range(len(root[1])):
                if row[0].replace(' / ','/').replace('Street','St.').replace('International',"Int'l")==root[1][i].find('name').text:
                    print(float(root[1][i].find('gtfs_longitude').text),float(root[1][i].find('gtfs_latitude').text))
                    stops.append([float(root[1][i].find('gtfs_longitude').text),float(root[1][i].find('gtfs_latitude').text)])
                    break

    todo['type']=type
    todo['route']=route
    todo['stops']=stops
    Transit.insert(todo)


def storeCalTrainStop():

    Transit=edb.get_transit_db()
    todo={}
    stops=[]
    edb.get_transit_db().remove({'type':'CalTrain'})
    # print(root[1][0].find('name').text)
    file_name='/Users/Mogeng/Berkeley/Semester2/E-Mission/Transit_routes/CalTrain.csv'
    with open(file_name, 'rU') as csvfile:
        r = csv.reader(csvfile,delimiter=',',quotechar='|')
        for row in r:
            time.sleep(1)
            print(row[0])
            # print(add)
            url='https://maps.googleapis.com/maps/api/geocode/json?address='+urllib.quote_plus(row[0]+' caltrain station')
            print(url)
            geo= json.load(urllib.urlopen(url))
            result=geo['results'][0]
            print(result['geometry']['location'])
            stops.append([result['geometry']['location']['lng'],result['geometry']['location']['lat']])

    todo['type']='CalTrain'
    todo['route']='CalTrain'
    todo['stops']=stops
    Transit.insert(todo)

## START ROUTE MATCHING!!! ##
def existingMatchDistance(route1,route2,step1=100000,step2=100000,method='lcs',radius1=2000):
    ## see how if "route1" can match with an existing route "route2"
    ## will be mainly used in matching with transit route
    # print(lst[0],lst[-1])
    # print(route)
    dis=999999
    if len(route1) < 2 or len(route2) < 2:
        return dis

    for start_route2 in range(len(route2)):
        coverage_start=find_near(route1,route2[start_route2],radius1)
        if coverage_start!=[]:
            break
    for end_route2 in range(len(route2)-1,-1,-1):
        coverage_end=find_near(route1,route2[end_route2],radius1)
        if coverage_end!=[]:
            break
    # print(start_route2,end_route2)
    # print(coverage_start,coverage_end)
    if abs(start_route2-end_route2)>1 and start_route2!=len(route2) and end_route2!=0:
        start_route1=coverage_start[0]
        end_route1=coverage_end[-1]

        if abs(start_route1-end_route1)>=1:
        ## using DTW Iteration
            if method=='dtw':
                if start_route1<end_route1:
                    new_dis=eatm.DTW.dynamicTimeWarp(refineRoute(route1[start_route1:end_route1+1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)
                elif end_route1<start_route1:
                    new_dis=eatm.DTW.dynamicTimeWarp(refineRoute(route1[end_route1:start_route1+1][::-1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)
        ## using DTW Recursion
            if method=='DTW':
                if start_route1<end_route1:
                    aa=eatm.DTW.Dtw(refineRoute(route1[start_route1:end_route1+1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)
                elif end_route1<start_route1:
                    aa=eatm.DTW.Dtw(refineRoute(route1[end_route1:start_route1+1][::-1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)

                new_dis=aa.calculate_distance()
        ## using symmetric DTW
            if method=='DTWSym':
                if start_route1<end_route1:
                    aa=eatm.DTW.Dtw(refineRoute(route1[start_route1:end_route1+1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)
                elif end_route1<start_route1:
                    aa=eatm.DTW.Dtw(refineRoute(route1[end_route1:start_route1+1][::-1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)

                new_dis=aa.calculate_distance()
        ## using DTW
            if method=='DTWAsym':
                if start_route1<end_route1:
                    aa=eatm.DTW.Dtw(refineRoute(route1[start_route1:end_route1+1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)
                elif end_route1<start_route1:
                    aa=eatm.DTW.Dtw(refineRoute(route1[end_route1:start_route1+1][::-1],step1),refineRoute(route2[start_route2:end_route2+1],step2),ec.calDistance)

                new_dis=aa.calculate_distance()
        ## using Frechet
            if method=='Frechet':
                if start_route1<end_route1:
                    new_dis=eatm.Frechet.Frechet(refineRoute(route1[start_route1:end_route1+1],step1),refineRoute(route2[start_route2:end_route2+1],step2))
                elif end_route1<start_route1:
                    new_dis=eatm.Frechet.Frechet(refineRoute(route1[end_route1:start_route1+1][::-1],step1),refineRoute(route2[start_route2:end_route2+1],step2))
        ## using lcs
            if method=='lcs':
                if start_route1<end_route1:
                    new_dis=eatm.LCS.lcsScore(refineRoute(route1[start_route1:end_route1+1],step1),refineRoute(route2[start_route2:end_route2+1],step2),radius1)
                elif end_route1<start_route1:
                    # print(route1[start_route1:end_route1-1])
                    # print(start_route1,end_route1)
                    # print(len(route1[start_route1:end_route1-1:-1]))
                    new_dis=eatm.LCS.lcsScore(refineRoute(route1[end_route1:start_route1+1][::-1],step1),refineRoute(route2[start_route2:end_route2+1],step2),radius1)
            if new_dis<dis:
                dis=new_dis

    # print(dis)
    return [start_route2,end_route2,dis]

def fullMatchDistance(route1,route2,step1=100000,step2=100000,method='lcs',radius1=2000):
    ## see how if "route1" can partially match with "route"
    ## will be mainly used in matching with transit route
    # print(lst[0],lst[-1])
    # print(route)
    dis=999999
    if len(route1) < 2 or len(route2) < 2:
        return dis

## using DTW Iteration
    if method=='dtw':
        new_dis=eatm.DTW.dynamicTimeWarp(refineRoute(route1,step1),refineRoute(route2,step2),ec.calDistance)
## using DTW Recursion
    if method=='DTW':
        aa=eatm.DTW.Dtw(refineRoute(route1,step1),refineRoute(route2,step2),ec.calDistance)
        new_dis=aa.calculate_distance()
## using symmetric DTW
    if method=='DTWSym':
        aa=eatm.DTW.DtwSym(refineRoute(route1,step1),refineRoute(route2,step2),ec.calDistance)
        new_dis=aa.calculate_distance()
## using Asymmetric DTW
    if method=='DTWAsym':
        aa=eatm.DTW.DtwAsym(refineRoute(route1,step1),refineRoute(route2,step2),ec.calDistance)
        new_dis=aa.calculate_distance()
## using Frechet
    if method=='Frechet':
        new_dis=eatm.Frechet.Frechet(refineRoute(route1,step1),refineRoute(route2,step2))
## using lcs
    if method=='lcs':
        new_dis=eatm.LCS.lcsScore(refineRoute(route1,step1),refineRoute(route2,step2),radius1)
    if new_dis<dis:
        dis=new_dis

    # print(dis)
    return dis

def matchTransitRoutes(lst,route,step1=100000,step2=100000,method='lcs',radius1=2500,threshold=0.5):
    [final_start,final_end,dis]=existingMatchDistance(lst,route,step1,step2,method,radius1)
    if dis<=threshold:
        return 1
    else:
        return 0

def matchTransitStops(lst,route,radius1=2000):
    if ec.Include_place_2(route,lst[0],radius1) and ec.Include_place_2(route,lst[-1],radius1):
        return 1
    else:
        return 0

def matchTwoRoutes(route1,route2,step1=100000,step2=100000,method='lcs',radius1=2000,threshold=0.6):
    [final_start,final_end,dis]=fullMatchDistance(route1,route2,step1,step2,method,radius1)
    if dis<=threshold:
        return 1
    else:
        return 0

def update_user_routeDistanceMatrix(user_id,data_feature,step1=100000,step2=100000,method='lcs',radius1=1000):
    ids = data_feature.keys()
    """
    user_query=edb.get_routeDistanceMatrix_db().find_one({'$and':[{'user':user_id},{'method':method}]})
    if user_query==None:
        user_disMat={}
        for _id in ids:
            user_disMat[_id] = {}
        edb.get_routeDistanceMatrix_db().insert({'user':user_id,'method':method,'disMat':user_disMat})
    else:
        user_disMat=user_query['disMat']
    """
    user_disMat = edb.get_routeDistanceMatrix_db(user_id, method)

    a=0

    # print(len(ids))
    for _id in ids:
        if a % 100 == 0:
            print "In update_user_routeDistanceMatrix, a = %d" % a
        a+=1
        for key in ids:
            try:
                user_disMat[_id][key]
                #print("found it")
            except KeyError:
                #print('Updating matrix for the trip ' + _id + '. Doing calculations.')
                dis=fullMatchDistance(data_feature[_id], data_feature[key],step1,step2,method,radius1)
                #user_disMat[_id] = {}
                if _id not in user_disMat:
                    user_disMat[_id] = {}
                user_disMat[_id][key] = dis
                #print('Update successful.')
                #print(user_disMat[_id])

    #edb.get_routeDistanceMatrix_db().update({'$and':[{'user':user_id},{'method':method}]},{'user':user_id,'method':method,'disMat':user_disMat})
    print(type(user_disMat))
    user_disMat = update_routeDistanceMatrix_db(user_id, method, user_disMat)
    return user_disMat

    # for entry in edb.get_routeDistanceMatrix_db().find():
    #     print(entry)

def update_user_routeClusters(user_id,clusters,method='lcs'):
    user_query=edb.get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':method}]})
    if user_query==None:
        edb.get_routeCluster_db().insert({'user':user_id,'method':method,'clusters':clusters})
    else:
        edb.get_routeCluster_db().update({'user':user_id,'method':method},{'user':user_id,'method':method,'clusters':clusters})


def get_common_routes_for_user(user_id,method='lcs'):
    common_idxs = []
    Sections = edb.get_section_db()
    user_route_clusters = edb.get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':method}]})['clusters']
    for idx in user_route_clusters.keys():
        # print(idx)
        if len(user_route_clusters[idx]) >= 3:
            section=Sections.find_one({'_id': idx})
            # print(section)
            if section['distance'] > 2000 and len(getRoute(idx)) > 10 and section['duration'] > 600:
                common_idxs.append(idx)
    return common_idxs

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import numpy as np
import math
from scipy.ndimage import measurements

# Our imports
import emission.core.common as ec

# Tristan Ursell
# Frechet Distance between two curves
# May 2013

#  f = frechet(X1,Y1,X2,Y2)
#  f = frechet(X1,Y1,X2,Y2,res)

#  (X1,Y1) are the x and y coordinates of the first curve (list).
#  (X2,Y2) are the x and y coordinates of the second curve (list).

#  The lengths of the two curves do not have to be the same.
# 
#  'res' is an optional parameter to set the resolution of 'f', the time to
#  compute scales linearly with 'res'. 'res' must be positive, and if 'res'
#  is larger than the largest distance between any two points on the curve
#  the function will throw a warning. If 'res' is unspecified, the function
#  will select a reasonable value, given the inputs.
# 
#  This function estimates the Frechet Distance, which is a measure of the
#  dissimilarity between two curves in space (in this case in 2D).  It is a
#  scalar value that is symmetric with respect to the two curves (i.e.
#  switching X1->X2 and Y1->Y2 does not change the value).  Roughly
#  speaking, this distance metric is the minimum length of a line that
#  connects a point on each curve, and allows one to traverse both curves
#  from start to finish.  (wiki:  Frechet Distance)
# 
#  The function requires column input vectors, and the function 'bwlabel'
#  from the image processing toolbox.
# 
# 
# EXAMPLE: compare three curves to find out which two are most similar
# 
#  # curve 1
# t1=0:1:50;
# X1=(2*cos(t1/5)+3-t1.^2/200)/2;
# Y1=2*sin(t1/5)+3;
# 
#  # curve 2
# X2=(2*cos(t1/4)+2-t1.^2/200)/2;
# Y2=2*sin(t1/5)+3;
# 
#  # curve 3
# X3=(2*cos(t1/4)+2-t1.^2/200)/2;
# Y3=2*sin(t1/4+2)+3;
# 
# f12=frechet(X1',Y1',X2',Y2');
# f13=frechet(X1',Y1',X3',Y3');
# f23=frechet(X2',Y2',X3',Y3');
# f11=frechet(X1',Y1',X1',Y1');
# f22=frechet(X2',Y2',X2',Y2');
# f33=frechet(X3',Y3',X3',Y3');
# 
# figure;
# subplot(2,1,1)
# hold on
# plot(X1,Y1,'r','linewidth',2)
# plot(X2,Y2,'g','linewidth',2)
# plot(X3,Y3,'b','linewidth',2)
# legend('curve 1','curve 2','curve 3','location','eastoutside')
# xlabel('X')
# ylabel('Y')
# axis equal tight
# box on
# title(['three space curves to compare'])
# legend
# 
# subplot(2,1,2)
# imagesc([[f11,f12,f13];[f12,f22,f23];[f13,f23,f33]])
# xlabel('curve')
# ylabel('curve')
# cb1=colorbar('peer',gca);
# set(get(cb1,'Ylabel'),'String','Frechet Distance')
# axis equal tight
# 

def Frechet(R1,R2,varargin=None):

    # get path point length
    L1=len(R1);
    L2=len(R2);

    frechet1=np.zeros((L1,L2))
    # print(frechet1)
    # calculate frechet distance matrix
    for i in range(L1):
        for j in range(L2):
            # frechet1[i,j]=math.sqrt(math.pow(R1[i][0]-R2[j][0],2)+math.pow(R1[i][1]-R2[j][1],2))
            frechet1[i,j]=ec.calDistance(R1[i],R2[j])
    fmin=frechet1.min();
    fmax=frechet1.max();
    # print(fmin)
    # print(fmax)

    # handle resolution
    if varargin==None:
        varargin=1000
    # print(frechet1<=3)
    # print(varargin)
    # compute frechet distance
    # print(np.linspace(fmin,fmax,varargin))
    for q3 in np.linspace(fmin,fmax,varargin):
        # print(q3)
        im1=np.asarray(measurements.label(frechet1<=q3))[0]
        # print(im1.shape)
        # print(im1[-1,-1])
        # get region number of beginning and end points
        if im1[0,0]!=0 and im1[0,0]==im1[-1,-1]:
            f=q3
            break
    return f
# R1=[]
# R2=[]
# for aa in range(0,51):
#     # R1.append([(2*math.cos(aa/5)+3-math.pow(aa,2)/200)/2,2*math.sin(aa/5)+3])
#     # R2.append([(2*math.cos(aa/4)+2-math.pow(aa,2)/200)/2,2*math.sin(aa/5)+3])
#     R1.append([aa,aa])
#     R2.append([aa,aa+8])
# # print(R1)
# # print(R2)
# print(Frechet(R1,R2))

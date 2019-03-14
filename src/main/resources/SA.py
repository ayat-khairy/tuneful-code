import numpy as np
import sys
import time
from numpy import newaxis
#import matplotlib.pyplot as plt
#import matplotlib.cm as cm
import csv
#import matplotlib.ticker as ticker
#from sklearn.svm import SVR
from sklearn.feature_selection import VarianceThreshold, RFE, SelectFromModel
import pandas as  pd
#from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.ensemble import ExtraTreesRegressor
#from sample_30params import *
from sklearn.feature_selection import VarianceThreshold, RFE, SelectFromModel


def variance_threshold_selector(data, threshold):
    selector = VarianceThreshold(threshold)
    selector.fit (data)
    variances = selector.variances_
    print (variances)
    return variances , data[data.columns[selector.get_support(indices=True)]]


#load features data
def load_features (features_fileName, n_params):
  features = []
  target = []
  n_params = int(n_params)
  with open(features_fileName) as csvfile:
      readerCSV = csv.reader(csvfile, delimiter=',')
      header = next(readerCSV)
      header = header [:n_params]
#      print (">>> header >>> " , header)
      for r in readerCSV:
          features = np.append( features , r[0:n_params])
          target = np.append (target , int (r [n_params]))

  print (">>>> length >>> ", len (features))

  features = np.reshape (features, (int(len (features)/n_params),n_params))
  features = features.astype ('float')
  return header , features , target


def scale_data(data):
  from sklearn.preprocessing import MinMaxScaler
  scaler = MinMaxScaler ()
  print (">>>>data to scale >>>>" , data)
  scaled_df =  scaler.fit_transform (data )

#  print ("scaled DF >> " , scaled_df [:,11])
  return scaled_df



### scale the features
def get_sig_params (header , features , target , fraction):
  start_time = int(time.time())
  search_time = 0
  from sklearn.feature_selection import VarianceThreshold
  scaled_features = scale_data (features) 
  #print ("length of features  >>>> "  , features.shape)  
  # fit an Extra Trees model to the data
  sig_conf_all_iterations = [] 
  global  n_iterations 
  model = ExtraTreesRegressor ()
  error_all_itr=[]

  n_params  = int(float(fraction) *len (header))
####### build the model 100 time to overcome model randomness ###########
  for x in range (n_iterations):
          all_params = [0] * 30
          model.fit (scaled_features , target)
          normalized_importance =  100 * (model.feature_importances_ /max (model.feature_importances_)) 
          indices  = normalized_importance.argsort()[-n_params:][::-1]
          indices = np.array (indices)
          all_params = np.array (all_params)
          all_params [indices] = 1  # set the indices of the seleced params to 1
  #        print ("all_params >>>> " , all_params)
          sig_conf_all_iterations= np.append(sig_conf_all_iterations , all_params)
  sig_conf_all_iterations = np.reshape (sig_conf_all_iterations ,  (n_iterations , 30))
  sig_conf_all_iterations = np.count_nonzero (sig_conf_all_iterations , axis = 0)    # count the occurances of each param in the sig params over all the interations
  header = np.array (header) 
  indices = sig_conf_all_iterations.argsort()[-n_params:][::-1] # select the params that have the most occurances in the sig params over all the interations
  indices = np.array (indices)
  print (">>>>>>>>>>>>" , indices)
  h_inf_params = header [indices] 
  print (">>>>>>>>>" , h_inf_params)
  search_time += (int(time.time())- start_time)
  return  h_inf_params 
########################################
def get_sig_conf_over_iterations (number_of_iterations , samples_count , conf , sig_conf_indices):

    sig_conf =[]
    all_iterations_sig_conf = []
    percentile_90th = []
    indices = []
    header = []
    
    for i in range (number_of_iterations):
        sig_conf = get_sig_conf ( samples_count , conf , sig_conf_indices)
        all_iterations_sig_conf = np.append (all_iterations_sig_conf  , sig_conf)
    all_iterations_sig_conf = np.reshape (all_iterations_sig_conf , (number_of_iterations , 30))
    percentile_90th = np.percentile (all_iterations_sig_conf , 90 , axis = 0)
    normalized_percentile_90th =  100 * (percentile_90th/max (percentile_90th))
    print ("90th percentile >>> " , percentile_90th)
    print ("max percentile >>> " , max (percentile_90th))
    print ("normalized 90th percentile >>> " , normalized_percentile_90th)
    cutoff_percent = 5
    indices = np.nonzero(normalized_percentile_90th >= cutoff_percent)[0]
    #indices = normalized_percentile_90th.argsort()[-6:][::-1] # get 6 conf wz the highest scores on the 90th percentile
    h_inf_conf  = get_inf_params(normalized_percentile_90th) # return the name of the high inf conf found
    print ("h_inf_conf >>> " , h_inf_conf)
  #  print ("l_inf_conf >>> " , l_inf_conf)
    inf_conf = h_inf_conf
    #### store the found conf per SA run ######
    write_to_file (i , inf_conf)
    
    ############## calculate error over SA_runs #############
    #### compare found fixed_conf to the GT
    
    
    return inf_conf
##################################################### 

########################################

def get_inf_params(scores):
    sig_conf = scores.argsort()[-6:][::-1] # get 6 conf wz the highest scores on the 90th percentile 
    h_inf_params = []
    l_inf = []
    h_inf_threshold = 80
    l_inf_threshold = 50
    index = 0
    for x in sig_conf:
        if scores [x] >= h_inf_threshold:
              h_inf_params = np.append (h_inf_params , header [x])
        elif  scores[x] >= l_inf_threshold:
              l_inf = np.append (l_inf , header[x])
        

########################################
#####################################################
def perform_SA(samples_file, result_file , n_params  , fraction ):
    sig_conf = []
    sig_conf_indices = []
    header , samples , target = load_features(samples_file , n_params)
    sig_params  = get_sig_params (header , samples , target , fraction)
    write_to_file (sig_params ,  result_file)
    return sig_conf
########################################
########################################################
def write_to_file ( sig_params ,  res_file):
   
   file = open (res_file , "w")
   for x in sig_params :
        file.write ( x + " , " )
   file.write (  " \n" )
   file.close()
#######################################
#######################################

###### main #######
n_iterations = 100
samples_file = sys.argv[1]
n_params = sys.argv[2]
fraction = sys.argv[3]
result_file = sys.argv[4]
perform_SA(samples_file, result_file , n_params , fraction)


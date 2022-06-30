'''
Aggregate Features Functions for BPM
'''

import numpy as np
import pandas as pd

'''
Features for BPM series
        bpm_features.mean_bpm,
        bpm_features.sd_bpm,
        bpm_features.min_bpm,
        bpm_features.max_bpm,
        bpm_features.count_without_nans,
        bpm_features.earliest_bpm_date,
        bpm_features.latest_bpm_date
'''

def mean_bpm(bpm_series):
    """
    Mean of bpm measurements  
    """
    return np.nanmean(bpm_series)

def sd_bpm(bpm_series):
    """
    Standard deviation of bpm measurements  
    """
    return np.nanstd(bpm_series)
    
    
def min_bpm(bpm_series):
    """
    Minimum value of bpm measurements  
    """
    return np.nanmin(bpm_series)
    
def max_bpm(bpm_series):
    """
    Maximum value of bpm measurements  
    """
    return np.nanmax(bpm_series)
 
def count_(bpm_series):
    """
    Number of datapoints excluding nans
    """
    return np.count_nonzero(~np.isnan(bpm_series))

def earliest_bpm_date(date_series):
    """
    Earliest date of bpm record
    """
    return np.min(date_series)


def latest_bpm_date(date_series):
    """
    Latest date of bpm record
    """
    return np.max(date_series)
 
'''
    Resting BPM Features
        bpm_features.mean_resting_bpm,
        bpm_features.sd_resting_bpm,
        bpm_features.min_resting_bpm,
        bpm_features.max_resting_bpm,
    ]
'''
 
def mean_resting_bpm(bpm_series):
    """
    Mean of resting bpm measurements  
    """
    return np.nanmean(bpm_series)

def sd_resting_bpm(bpm_series):
    """
    Standard deviation of resting bpm measurements  
    """
    return np.nanstd(bpm_series)
    
    
def min_resting_bpm(bpm_series):
    """
    Minimum value of resting bpm measurements  
    """
    return np.nanmin(bpm_series)
    
def max_resting_bpm(bpm_series):
    """
    Maximum value of resting bpm measurements  
    """
    return np.nanmax(bpm_series)
 
'''
    Heart Rate Zones
        bpm_features.min_hrzone_bpm
        bpm_features.max_hrzone_bpm
    ]
'''
 
def min_hrzone_bpm(bpm_series):
    """
    Minimum value of resting bpm measurements  
    """
    return np.nanmin(bpm_series)
    
def max_hrzone_bpm(bpm_series):
    """
    Maximum value of resting bpm measurements  
    """
    return np.nanmax(bpm_series) 
    
'''
    Heart Rate Variability
        bpm_features.mean_rmssd
        bpm_features.mean_sdrr
    ]
'''
def mean_rmssd(rmssd_series):
    """
    Mean of rmssd measurements  
    """
    return np.nanmean(rmssd_series)

# def mean_sdrr(sdrr_series):
#     """
#     Standard deviation of sdrr values 
#     """
#     return np.nanstd(sdrr_series)

def mean_sdrr(sdrr_series):
    """
    Standard deviation of sdrr values 
    """
    return np.nanmean(sdrr_series)


def avg_metric_24h(beat_interval_ms_series):
    """
    Standard deviation of sdrr values 
    """
    return np.nanmean(beat_interval_ms_series)




if __name__ == "__main__":
    pass

# -*- coding: utf-8
'''
Time series functions such as auto-correlations and cross-correlations.
'''
import numpy as np

def cross_cov(x, y, h):
    
    x = np.asarray(x)
    y = np.asarray(x)
    
    assert x.shape == y.shape
    assert x.ndim == 1
    assert h >= 0
    
    n = x.shape[0]
    
    x_mean = x.mean()
    y_mean = y.mean()
    
    x = x[h:]
    if h > 0:
        y = y[:-h]
    
    return ((x - x_mean)  * (y - y_mean)).sum() / n
    
def auto_cov(x, h):
    
    return cross_cov(x, x, h)

def cross_corr(x, y, h):
    
    return cross_cov(x, y, h) / np.sqrt(auto_cov(x, 0) * auto_cov(y, 0))

def auto_corr(x, h):
    
    return cross_corr(x, x, h)

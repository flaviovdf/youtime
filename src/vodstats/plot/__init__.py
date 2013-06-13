import matplotlib
matplotlib.use("Agg")

from matplotlib import rc
rc('axes', unicode_minus=False)

#Para o artigo
from math import sqrt

fig_width_pt  = 240
inches_per_pt = 1.0/72.27
golden_mean   = (sqrt(5)-1.0)/2.0
fig_width     = fig_width_pt*inches_per_pt
fig_height    = fig_width*golden_mean
fig_size      = [fig_width,fig_height]

rc('legend', fontsize=18)
rc('text', usetex=True)
rc('font', family='serif')
rc('ps', usedistiller='xpdf')
rc('xtick', labelsize=20)
rc('ytick', labelsize=20)
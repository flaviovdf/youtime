# -*- coding: utf-8
from __future__ import division

from itertools import cycle

from matplotlib import cm, pyplot as plt
from matplotlib.patches import Polygon
from matplotlib.projections import register_projection
from matplotlib.projections.polar import PolarAxes

import numpy as np

"""
    Styles defined bellow (Cheat sheet)
    
    ================    ===============================
    character           description
    ================    ===============================
    ``'-'``             solid line style
    ``'--'``            dashed line style
    ``'-.'``            dash-dot line style
    ``':'``             dotted line style
    ``'.'``             point marker
    ``','``             pixel marker
    ``'o'``             circle marker
    ``'v'``             triangle_down marker
    ``'^'``             triangle_up marker
    ``'<'``             triangle_left marker
    ``'>'``             triangle_right marker
    ``'1'``             tri_down marker
    ``'2'``             tri_up marker
    ``'3'``             tri_left marker
    ``'4'``             tri_right marker
    ``'s'``             square marker
    ``'p'``             pentagon marker
    ``'*'``             star marker
    ``'h'``             hexagon1 marker
    ``'H'``             hexagon2 marker
    ``'+'``             plus marker
    ``'x'``             x marker
    ``'D'``             diamond marker
    ``'d'``             thin_diamond marker
    ``'|'``             vline marker
    ``'_'``             hline marker
    ================    ===============================

    The following color abbreviations are supported:
    
    ==========  ========
    character   color
    ==========  ========
    'b'         blue
    'g'         green
    'r'         red
    'c'         cyan
    'm'         magenta
    'y'         yellow
    'k'         black
    'w'         white
    ==========  ========
"""

#####################################
#####      COMMON              ######
#####################################

def close_plot(**kwargs):
    outputf = kwargs.pop('outputf', None)
    dpi = kwargs.pop('dpi', 300)
    
    if outputf:
        plt.savefig(outputf, dpi=dpi)
    else:
        plt.show()
    plt.close()

def __style(**kwargs):
    xlabel = kwargs.pop('xlabel', '')
    ylabel = kwargs.pop('ylabel', '')
    
    title = kwargs.pop('title', '')
    fontsize = kwargs.pop('fontsize', 20)
    xrotation = kwargs.pop('xrotation', None)
    yrotation = kwargs.pop('yrotation', None)
    
    xmin = kwargs.pop('xmin', None) 
    xmax = kwargs.pop('xmax', None) 
    ymin = kwargs.pop('ymin', None) 
    ymax = kwargs.pop('ymax', None)
    
    grid = kwargs.pop('grid', False)
    gridcolor = kwargs.pop('gridcolor', 'lightgrey')
    legloc = kwargs.pop('legloc', 'best')
    legborder = kwargs.pop('legborder', False)
    
    plt.xlabel(xlabel, fontsize=fontsize)
    plt.ylabel(ylabel, fontsize=fontsize)
    plt.title(title, fontsize=fontsize)

    if grid:
        plt.grid(grid, color=gridcolor, zorder=1)

    plt.xlabel(xlabel, fontsize=fontsize)
    plt.ylabel(ylabel, fontsize=fontsize)
    plt.title(title, fontsize=fontsize)
    
    lg = plt.legend(loc=legloc)
    if not legborder and lg:
        lg.get_frame().set_linewidth(0)
            
    if xmin:
        plt.xlim(xmin=xmin)

    if xmax:
        plt.xlim(xmax=xmax)
        
    if ymin:
        plt.ylim(ymin=ymin)

    if ymax:
        plt.ylim(ymax=ymax)

    logx = kwargs.pop('logx', None)
    logy = kwargs.pop('logy', None)
    
    ax = plt.gca()
    if logy:
        ax.set_yscale('log')
            
    if logx:
        ax.set_xscale('log')
    
    labels = ax.get_xticklabels()
    for label in labels:
        label.set_rotation(xrotation) 
    #   label.set_fontsize(fontsize)
        
    labels = ax.get_yticklabels()
    for label in labels:
        label.set_rotation(yrotation)
    #   label.set_fontsize(fontsize)
    
def plot_decorator(function):
    def decorator(*args, **kwargs):
        function(*args, **kwargs)
        __style(**kwargs)
        close_plot(**kwargs)
    return decorator

#####################################
#####      Plot Objects        ######
#####################################        

class XYPoints(object):
    
    def __init__(self, xpoints, ypoints, name='', style='', linewidth=2):
        self.xpoints = xpoints
        self.ypoints = ypoints
        self.name = name
        self.style = style
        self.lw = linewidth

class YPoints(object):
    
    def __init__(self, ypoints, name='', style='', linewidth=2):
        self.ypoints = ypoints
        self.xpoints = range(len(ypoints))
        self.name = name
        self.style = style
        self.lw = linewidth
                
#####################################
#####      XY      PLOT        ######
#####################################

@plot_decorator
def xy_plot(*args, **kwargs):
    z=2
    for l in args:
        plt.plot(l.xpoints, l.ypoints, l.style, label=l.name, linewidth=l.lw, zorder=z)
        z+=1
        
#####################################
#####      IMG     PLOT        ######
#####################################

@plot_decorator
def hexbin(*args, **kwargs):
    assert len(args) == 2

    xmin = kwargs.pop('xmin', None) 
    xmax = kwargs.pop('xmax', None) 
    ymin = kwargs.pop('ymin', None) 
    ymax = kwargs.pop('ymax', None)
    
    extent = [xmin, xmax, ymin, ymax]

    plt.hexbin(args[0], args[1], cmap=plt.get_cmap('jet'))
    plt.axis(extent)
    cb = plt.colorbar()
    cb.set_label('Number of Videos')

@plot_decorator
def img(*args, **kwargs):
    assert len(args) == 1

    matrix = args[0]
    xmin = kwargs.pop('xmin', None) 
    xmax = kwargs.pop('xmax', None) 
    ymin = kwargs.pop('ymin', None) 
    ymax = kwargs.pop('ymax', None)
    
    extent = [xmin, xmax, ymin, ymax]
    cmap = kwargs.pop('cm', cm.get_cmap('bone'))
    plt.imshow(matrix, extent=extent, cmap=cmap)

def colorbar(*args, **kwargs):
    cb = plt.colorbar()
#    cb.set_label('Mean fraction of days between y and x')

#####################################
#####       BOX PLOTS          ######
#####################################

class Box(object):
    def __init__(self, ypoints, name='', fillcolor='b', edgecolor='k', wstyle='k-'):
        self.ypoints = ypoints
        self.name = name
        self.fillcolor = fillcolor
        self.edgecolor = edgecolor
        self.wstyle = wstyle

@plot_decorator
def box_plot(*args, **kwargs):
    mat = []
    labels = []
    for b in args:
        mat.append(b.ypoints)
        labels.append(b.name)
    
    fig = plt.figure(frameon=False)

    n_boxes = len(args)

    bp = plt.boxplot(mat, sym='', whis=0)
    ax = plt.gca()

    #Hack me up!
    trans = {
        'EXTERNAL':'EXT.',
        'FEATURED':'FEAT.',
        'INTERNAL':'INT.',
        'SOCIAL':'SOC.',
        'SEARCH':'SEAR.', 
        'MOBILE':'MOB.',
        'VIRAL':'VIRAL'
    }

    plt.setp(ax, 'xticklabels', [trans[l] for l in labels])
    plt.setp(bp['whiskers'], color='k', linestyle='-')

    #set colors
    for i in xrange(n_boxes):
        box = bp['boxes'][i]
        whisker = bp['whiskers'][i]

        box_x = []
        box_y = []
        for j in xrange(5):
            box_x.append(box.get_xdata()[j])
            box_y.append(box.get_ydata()[j])
        box_coords = zip(box_x, box_y)

        box_polygon = Polygon(box_coords, facecolor=args[i].fillcolor)
        ax.add_patch(box_polygon)

        plt.setp(box, color=args[i].edgecolor)

        # Now draw the median lines back over what we just filled in
        med = bp['medians'][i]
        median_x = []
        median_y = []
        for j in xrange(2):
            median_x.append(med.get_xdata()[j])
            median_y.append(med.get_ydata()[j])
            plt.plot(median_x, median_y, args[i].edgecolor)

        #Upper ticks. TODO: remove from here, should be optional. Also, this module should not use numpy
        tick = i + 1
        import numpy as np
        from scipy import stats

        dat = args[i].ypoints
        mean = np.mean(dat)

        #txt = '$\mu=%.2f$\n$\sigma=%.2f$'%(np.mean(dat), np.std(dat))
        #ax.text(tick, 1-(1*0.1), txt,
        #         horizontalalignment='center', weight='bold', color='k', fontsize=16, rotation=90)

        perc9   = stats.scoreatpercentile(dat, 9)
        perc25  = stats.scoreatpercentile(dat, 25)
        perc75  = stats.scoreatpercentile(dat, 75)
        perc91  = stats.scoreatpercentile(dat, 91)
        
        if tick == 1:
            l1 = plt.plot([tick], [perc9], 'ks', linewidth=3, markersize=9)
            l2 = plt.plot([tick], [perc91], 'ko', linewidth=3, markersize=9)
            l3 = plt.plot([tick], [mean], 'Dw', linewidth=3, markersize=9)
            #lg = fig.legend((l2, l1), ('$91^{th}$ Percentile', '$9^{th}$ Percentile'), loc=(0.64, 0.91))
            lg = plt.legend((l2, l3, l1), ('$91^{th}$ Percentile', 'Mean', '$9^{th}$ Percentile'), loc='best')
            lg.get_frame().set_linewidth(0)
        else:
            plt.plot([tick], [perc9], 'ks', linewidth=3, markersize=9)
            plt.plot([tick], [perc91], 'ko', linewidth=3, markersize=9)
            plt.plot([tick], [mean], 'Dw', linewidth=3, markersize=9)

        plt.plot([tick, tick], [perc9, perc25], 'k-')
        plt.plot([tick, tick], [perc75, perc91], 'k-')


#####################################
#####        BAR PLOT          ######
#####################################

class Bars(object):
    
    def __init__(self, ypoints, name='', fillcolor='', edgecolor='k', hatch=''):
        self.ypoints = ypoints
        self.name = name
        self.fillcolor = fillcolor
        self.edgecolor = edgecolor
        self.hatch = hatch
        
@plot_decorator
def barplot(*args, **kwargs):
    if not args:
        return
    
    first = args[0]
    
    loc = range(len(first.ypoints))
    xticklabels = kwargs.pop('xticklabels', loc)
    width = kwargs.pop('width', 1.0 / (len(args) + 1))
    
    samelen = len(first.ypoints)
    assert len(loc) == samelen
    for b in args:
        assert len(b.ypoints) == samelen
    
    
    colors  = cycle(['b', 'g', 'r', 'c',  'm', 'y', 'k'])
    hatches = cycle(['\\', '', '/', '-', '+', 'x', '.'])

    i = 0
    for b in args:
        nloc = [l + (i * width) for l in loc]
        fill = b.fillcolor
        hatch = b.hatch

        if not fill:
            fill = colors.next()

        if not hatch:
            hatch = hatches.next()

        z=i+2
        plt.bar(left=nloc, height=b.ypoints, color=fill, edgecolor=b.edgecolor, width=width, label=b.name, zorder=z, hatch=hatch, alpha=0.50)
        i += 1
    
    labeldelta = (len(args) * width) / 2
    plt.xticks([i + labeldelta for i in xrange(len(xticklabels))], xticklabels)

#####################################
#####       SCATTER MATRIX     ######
#####################################

#####################################
#####       RADAR PLOTS        ######
#####################################


def _radar_factory(num_vars, frame='circle'):
    """Create a radar chart with `num_vars` axes."""
    # calculate evenly-spaced axis angles
    theta = 2*np.pi * np.linspace(0, 1-1./num_vars, num_vars)
    # rotate theta such that the first axis is at the top
    theta += np.pi/2

    def draw_poly_frame(self, x0, y0, r):
        # TODO: use transforms to convert (x, y) to (r, theta)
        verts = [(r*np.cos(t) + x0, r*np.sin(t) + y0) for t in theta]
        return plt.Polygon(verts, closed=True, edgecolor='k')

    def draw_circle_frame(self, x0, y0, r):
        return plt.Circle((x0, y0), r)

    frame_dict = {'polygon': draw_poly_frame, 'circle': draw_circle_frame}
    if frame not in frame_dict:
        raise ValueError, 'unknown value for `frame`: %s' % frame

    class RadarAxes(PolarAxes):
        """Class for creating a radar chart (a.k.a. a spider or star chart)

        http://en.wikipedia.org/wiki/Radar_chart
        """
        name = 'radar'
        # use 1 line segment to connect specified points
        RESOLUTION = 1
        # define draw_frame method
        draw_frame = frame_dict[frame]

        def fill(self, *args, **kwargs):
            """Override fill so that line is closed by default"""
            closed = kwargs.pop('closed', True)
            return super(RadarAxes, self).fill(closed=closed, *args, **kwargs)

        def plot(self, *args, **kwargs):
            """Override plot so that line is closed by default"""
            lines = super(RadarAxes, self).plot(*args, **kwargs)
            for line in lines:
                self._close_line(line)

        def _close_line(self, line):
            x, y = line.get_data()
            # FIXME: markers at x[0], y[0] get doubled-up
            if x[0] != x[-1]:
                x = np.concatenate((x, [x[0]]))
                y = np.concatenate((y, [y[0]]))
                line.set_data(x, y)

        def set_varlabels(self, labels):
            self.set_thetagrids(theta * 180/np.pi, labels)

        def _gen_axes_patch(self):
            x0, y0 = (0.5, 0.5)
            r = 0.5
            return self.draw_frame(x0, y0, r)

    register_projection(RadarAxes)
    return theta

class Radar(object):

    def __init__(self, ypoints, name='', edgecolor='', fillcolor='', alpha=0.25):
        self.ypoints = ypoints
        self.name = name
        self.edgecolor = edgecolor
        self.fillcolor = fillcolor
        self.alpha = alpha

@plot_decorator
def radar_plot(n_axes, ax_labels=None, *args, **kwargs):
    if not ax_labels: ax_labels = []

    radar_ax = _radar_factory(n_axes)
    z=2
    for radar in args:
        plt.plot(radar_ax, radar.ypoints, color=radar.edgecolor, zorder=z, name=radar.name)
        plt.fill(radar_ax, radar.ypoints, facecolor=radar.fillcolor, alpha=0.25, zorder=z)
        z+=1
    ax = plt.gca()
    ax.set_varlabels(ax_labels)

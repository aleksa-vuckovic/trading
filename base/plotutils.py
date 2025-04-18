#1
import matplotlib
from matplotlib import pyplot as plt
import matplotlib.figure


COLORS = ['red', 'blue', 'green', 'orange', 'purple', 'black']

def refresh_interactive_figure(fig: matplotlib.figure.Figure):
    for axes in fig.axes:
        axes.relim()
        axes.autoscale_view()
    fig.canvas.draw_idle()
    fig.canvas.flush_events()
def refresh_interactive_figures(*args: matplotlib.figure.Figure):
    for fig in args: refresh_interactive_figure(fig)

import plotly.plotly as py
import plotly.graph_objs as go

trace1 = go.Scatter(
    x=[0, 1, 2, 3, 4, 5, 6, 7, 8],
    y=[0, 1, 2, 3, 4, 5, 6, 7, 8],
    name='Name of Trace 1'
)
trace2 = go.Scatter(
    x=[0, 1, 2, 3, 4, 5, 6, 7, 8],
    y=[1, 0, 3, 2, 5, 4, 7, 6, 8],
    name='Name of Trace 2'
)
data = [trace1, trace2]
layout = go.Layout(
    title='Plot Title'
)
fig = go.Figure(data=data, layout=layout)
plot_url = py.plot(fig, filename='styling-names')
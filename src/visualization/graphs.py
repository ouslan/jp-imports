import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go

class Graphs:

    def __init__(self, data, HTS_code):
        self.data = data
        self.HTS_code = HTS_code

    # Bollinger Bands
    def bbands(self, data, window=36, no_of_std=1):
        rolling_mean = data.rolling(window).mean()
        rolling_std  = data.rolling(window).std()
        upper_band = rolling_mean + (rolling_std * no_of_std)
        lower_band = rolling_mean - (rolling_std * no_of_std)
        return rolling_mean, upper_band, lower_band

    def gen_graph(self):
        data = self.data[self.data['HTS'] == self.HTS_code]
        desc = data['HTS_desc'].iloc[0]
        if len(desc.split()) > 6:
            desc = desc.split()[:6]
            desc = ' '.join(desc)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=data['date'], y=data['value_growth'], name='Value Growth'))
        fig.add_trace(go.Scatter(x=data['date'], y=self.bbands(data['value_growth'])[0], name='Mean'))
        fig.add_trace(go.Scatter(x=data['date'], y=self.bbands(data['value_growth'])[1], name='Upper'))
        fig.add_trace(go.Scatter(x=data['date'], y=self.bbands(data['value_growth'])[2], name='Lower'))
        
        # upper bound
        fig.add_trace(go.Scatter(x=data[data['value_growth'] > self.bbands(data['value_growth'])[1]]['date'], 
                                 y=data[data['value_growth'] > self.bbands(data['value_growth'])[1]]['value_growth'],
                                 mode='markers', marker=dict(color='Yellow',size=8,line=dict(color='DarkSlateGrey',width=2)), name='Above 1 std'))
        
        # value grouth
        fig.add_trace(go.Scatter(x=data[data['value_growth'] > self.bbands(data['value_growth'],no_of_std=2)[1]]['date'],
                                 y=data[data['value_growth'] > self.bbands(data['value_growth'],no_of_std=2)[1]]['value_growth'],
                                 mode='markers',marker=dict(color='Orange',size=8,line=dict(color='DarkSlateGrey',width=2)), name='Above 2 std'))

        # lower bound
        fig.add_trace(go.Scatter(x=data[data['value_growth'] > self.bbands(data['value_growth'],no_of_std=3)[1]]['date'],
                                 y=data[data['value_growth'] > self.bbands(data['value_growth'],no_of_std=3)[1]]['value_growth'],
                                 mode='markers',marker=dict(color='Red',size=8,line=dict(color='DarkSlateGrey',width=2)),name='Above 3 std'))

        fig.update_layout(title=f'Bollinger Bands for {desc}', xaxis_title='Date', yaxis_title='Value')
        return fig.show()
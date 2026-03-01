#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from project.configuration import Config


pl.Config(tbl_rows=20, tbl_cols=-1)


class Plotting:

    def __init__(self, symbol: str, period: str = 'day', window: int = 10000):
        self.period = period
        self.stock_data = pl.read_parquet(Config.Paths.DataPath / 'stocks' / symbol / f'stock_{self.period}.parquet')[-window:]
        self.ma_data = pl.read_parquet(Config.Paths.DataPath / 'stocks' / symbol / f'ma_{self.period}.parquet')[-window:]
        self.boll_data = pl.read_parquet(Config.Paths.DataPath / 'stocks' / symbol / f'boll_{self.period}.parquet')[-window:]
        self.macd_data = pl.read_parquet(Config.Paths.DataPath / 'stocks' / symbol / f'macd_{self.period}.parquet')[-window:]
        
        self.plot()

    def plot(self):
        if self.period == 'day':
            name = '日线'
        elif self.period == 'week':
            name = '周线'
        elif self.period == 'month':
            name = '月线'
        elif self.period == 'quarter':
            name = '季线'
        else:
            raise ValueError(f'Period {self.period} is not supported')

        # 添加全图画布
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            subplot_titles=('交易', '成交量', 'MACD', 'Boll'),
            row_heights=[0.4, 0.2, 0.2, 0.2],
            vertical_spacing=0.05,
        )

        # 添加 蜡烛图 和 MA 移动平均线
        fig.add_trace(
            go.Candlestick(
                x=self.stock_data['date'], open=self.stock_data['raw_open'], high=self.stock_data['raw_high'], low=self.stock_data['raw_low'], close=self.stock_data['raw_close'], name=name,
                hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>开盘: %{{open}}<br>最高: %{{high}}<br>最低: %{{low}}<br>收盘: %{{close}}<extra></extra>'
            ),
            row=1, col=1
        )
        for key, value in {5: 'red', 10: 'blue', 20: 'green', 30: 'orange', 60: 'purple', 250: 'black'}.items():        
            fig.add_trace(
                go.Scatter(
                    x=self.stock_data['date'], y=self.ma_data[f'ma_{key}'], name=f'MA-{key}', line={'color': value, 'width': 1},
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>MA-{key}: %{{y}}<extra></extra>'
                ),
                row=1, col=1
            )

        # 添加 交易量 柱状图
        fig.add_trace(
            go.Bar(
                x=self.stock_data['date'], y=self.stock_data['volume'], marker_color=self.stock_data['color'], showlegend=False,
                hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>成交量: %{{y}}<extra></extra>'
            ),
            row=2, col=1
        )

        # 添加 MACD 指标和柱状图
        fig.add_traces(
            [
                go.Bar(
                    x=self.stock_data['date'], y=self.macd_data['macd'], name='MACD', marker_color=self.macd_data['color'],
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>MACD: %{{y}}<extra></extra>'
                ),
                go.Scatter(
                    x=self.stock_data['date'], y=self.macd_data['dif'], name='DIF', line={'color': 'black', 'width': 1},
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>DIF: %{{y}}<extra></extra>'
                ),
                go.Scatter(
                    x=self.stock_data['date'], y=self.macd_data['dea'], name='DEA', line={'color': 'peru', 'width': 1},
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>DEA: %{{y}}<extra></extra>'
                ),
            ], rows=[3, 3, 3], cols=[1, 1, 1]
        )

        # 添加 布林线
        fig.add_traces(
            [
                go.Candlestick(
                    x=self.stock_data['date'], open=self.stock_data['raw_open'], high=self.stock_data['raw_high'], low=self.stock_data['raw_low'], close=self.stock_data['raw_close'], showlegend=False,
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>开盘: %{{open}}<br>最高: %{{high}}<br>最低: %{{low}}<br>收盘: %{{close}}<extra></extra>'
                ),
                go.Scatter(
                    x=self.stock_data['date'], y=self.boll_data[f'boll_upper'], name=f'Boll Upper', line={'color': 'red', 'width': 1},
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>Boll Upper: %{{y}}<extra></extra>'
                ),
                go.Scatter(
                    x=self.stock_data['date'], y=self.boll_data[f'boll_mid'], name=f'Boll Mid', line={'color': 'blue', 'width': 1},
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>Boll Mid: %{{y}}<extra></extra>'
                ),
                go.Scatter(
                    x=self.stock_data['date'], y=self.boll_data[f'boll_lower'], name=f'Boll Lower', line={'color': 'green', 'width': 1},
                    hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>Boll Lower: %{{y}}<extra></extra>'
                ),
            ],
            rows=[4, 4, 4, 4], cols=[1, 1, 1, 1]
        )

        # 只在某些位置显示日期标签，避免过于密集
        tickvals, ticktext = [], []
        step = max(1, len(self.stock_data) // 12)  # 最多显示 10 个标签
        for i in range(0, len(self.stock_data), step):
            tickvals.append(self.stock_data['date'][i])
            ticktext.append(self.stock_data['date'][i].strftime("%Y-%m-%d"))

        # 去除休市的日期，保持x轴连续
        dt_all = pl.select(pl.date_range(start=self.stock_data['date'][0], end=self.stock_data['date'][-1], interval='1d')).to_series().to_list()
        dt_all = [d.strftime("%Y-%m-%d") for d in dt_all]
        trading_dates = [d.strftime("%Y-%m-%d") for d in self.stock_data['date'].to_list()]
        dt_breaks = list(set(dt_all) - set(trading_dates))

        # 设置 x 轴标签，只显示部分日期 Do not show OHLC's rangeslider plot
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, rangeslider_visible=False, row=1, col=1, showgrid=True)
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, rangeslider_visible=False, row=2, col=1, showgrid=True)
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, rangeslider_visible=False, row=3, col=1, showgrid=True)
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, rangeslider_visible=False, row=4, col=1, showgrid=True)
        fig.update_xaxes(rangebreaks=[dict(values=dt_breaks)], row=4, col=1) if self.period in ['day'] else None
        # fig.update_layout(hovermode='x unified', dragmode=False)
        fig.show()


class PlottingSpecific:

    def __init__(self, symbol: str):
        self.stock_data = pl.read_parquet(Config.Paths.DataPath / 'stocks' / symbol / f'stock_day.parquet')
        self.ma_data_month = pl.read_parquet(Config.Paths.DataPath / 'stocks' / symbol / f'ma_month.parquet')
        self.plot()

    def plot(self):
        # 添加全图画布
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            subplot_titles=('交易', '成交量'),
            row_heights=[0.7, 0.3],
            vertical_spacing=0.05,
        )

        # 添加 蜡烛图
        fig.add_trace(
            go.Candlestick(
                x=self.stock_data['date'], open=self.stock_data['raw_open'], high=self.stock_data['raw_high'], low=self.stock_data['raw_low'], close=self.stock_data['raw_close'], name='日线',
                hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>开盘: %{{open}}<br>最高: %{{high}}<br>最低: %{{low}}<br>收盘: %{{close}}<extra></extra>'
            ),
            row=1, col=1
        )

        # 添加 MA 移动平均线
        fig.add_trace(
            go.Scatter(
                x=self.ma_data_month['date'], y=self.ma_data_month[f'ma_30'], marker_color='purple', name=f'MA-Month-30', line={'color': 'orange', 'width': 1},
                hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>MA-Month-30: %{{y}}<extra></extra>'
            ),
            row=1, col=1
        )

        # 添加 交易量 柱状图
        fig.add_trace(
            go.Bar(
                x=self.stock_data['date'], y=self.stock_data['volume'], marker_color=self.stock_data['color'], showlegend=False,
                hovertemplate=f'日期: %{{x|%Y-%m-%d}}<br>成交量: %{{y}}<extra></extra>'
            ),
            row=2, col=1
        )

        # 只在某些位置显示日期标签，避免过于密集
        tickvals, ticktext = [], []
        step = max(1, len(self.stock_data) // 12)  # 最多显示 10 个标签
        for i in range(0, len(self.stock_data), step):
            tickvals.append(self.stock_data['date'][i])
            ticktext.append(self.stock_data['date'][i].strftime("%Y-%m-%d"))

        # 去除休市的日期，保持x轴连续
        dt_all = pl.select(pl.date_range(start=self.stock_data['date'][0], end=self.stock_data['date'][-1], interval='1d')).to_series().to_list()
        dt_all = [d.strftime("%Y-%m-%d") for d in dt_all]
        trading_dates = [d.strftime("%Y-%m-%d") for d in self.stock_data['date'].to_list()]
        dt_breaks = list(set(dt_all) - set(trading_dates))

        # 设置 x 轴标签，只显示部分日期 Do not show OHLC's rangeslider plot
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, rangeslider_visible=False, row=1, col=1, showgrid=True)
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, rangeslider_visible=False, row=2, col=1, showgrid=True)
        fig.update_xaxes(rangebreaks=[dict(values=dt_breaks)], row=2, col=1)
        # fig.update_layout(hovermode='x unified', dragmode=False)
        fig.show()



if __name__ == '__main__':
    # Plotting('sh600036', period='day')
    PlottingSpecific('sh600036')

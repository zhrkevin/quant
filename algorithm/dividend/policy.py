#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import copy
import datetime
import polars as pl

from project import Config


class AscendSignalWeekRotation:

    def __init__(self, symbol, selected_date=None, period=2, adjust='raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.period = period
        self.adjust = adjust

        print(f'\n策略分类:')
        print(f'\n1、上升趋势的周线滚动策略')
        self.condition()

    def condition(self):
        print(f'(1) 出月线macd坑内, 每年除权后, 日下周下月中买（底仓）, 日下周下买、周上卖该部分仓位做滚动。')
        selected_date = datetime.date(2026, 1, 2) if not self.selected_date else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=365*self.period)

        macd_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / f'macd_month.parquet')
        selected_macd_month = macd_month.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))
        
        for i_date in selected_macd_month['date']:
            dif_data = selected_macd_month.filter(pl.col('date') <= i_date)['dif']
            dea_data = selected_macd_month.filter(pl.col('date') <= i_date)['dea']

            if len(dif_data) > 1 and len(dea_data) > 1 and dif_data[-1] > 0 and dif_data[-2] < 0 and dea_data[-1] < 0 and dea_data[-2] < 0:
                climbup_date = copy.deepcopy(i_date)
                print(f'日期 {climbup_date} @ 触发: MACD 月线坑内出')
                # print(f'# 当日 Dif ({dif_data[-1]:.4f}) > 0 Dea ({dea_data[-1]:.4f}) < 0')
                # print(f'# 前日 Dif ({dif_data[-2]:.4f}) < 0 Dea ({dea_data[-2]:.4f}) < 0')
                break
        else:
            climbup_date = None
            # print(f'未触发 MACD 月线坑内出')

        if climbup_date:
            stock_day = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'stock_day.parquet')
            stock_week = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'stock_week.parquet')
            stock_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'stock_month.parquet')

            boll_day = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'boll_day.parquet')
            boll_week = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'boll_week.parquet')
            boll_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'boll_month.parquet')

            range_date = stock_day.filter(pl.col('date') >= climbup_date)['date']
            for i_date in range_date:
                selected_stock_day = stock_day.filter(pl.col('date') <= i_date)
                selected_stock_week = stock_week.filter(pl.col('date') <= i_date)
                selected_stock_month = stock_month.filter(pl.col('date') <= i_date)

                selected_boll_day = boll_day.filter(pl.col('date') <= i_date)
                selected_boll_week = boll_week.filter(pl.col('date') <= i_date)
                selected_boll_month = boll_month.filter(pl.col('date') <= i_date)       
                
                stock_day_date = selected_stock_day['date'][-1]
                stock_day_low = selected_stock_day[f'{self.adjust}_low'][-1]
                boll_day_lower = selected_boll_day['boll_lower'][-1]

                stock_week_date = selected_stock_week['date'][-1]
                stock_week_high = selected_stock_week[f'{self.adjust}_high'][-1]
                stock_week_low = selected_stock_week[f'{self.adjust}_low'][-1]
                boll_week_upper = selected_boll_week['boll_upper'][-1]
                boll_week_lower = selected_boll_week['boll_lower'][-1]
                
                stock_month_date = selected_stock_month['date'][-1]
                stock_month_low = selected_stock_month[f'{self.adjust}_low'][-1]
                boll_month_mid = selected_boll_month['boll_mid'][-1]

                if stock_day_low <= boll_day_lower and stock_week_low <= boll_week_lower and stock_month_low <= boll_month_mid:
                    print(f'日期 {i_date} @ 触发: Boll 日下轨 + 周下轨 + 月中轨 * 建议: 买入底仓')
                    # print(f'\n# 日 ({stock_day_date}) 蜡烛最低值 ({stock_day_low:.4f}) 日下轨 ({boll_day_lower:.4f})')
                    # print(f'\n# 周 ({stock_week_date}) 蜡烛最低值 ({stock_week_low:.4f}) 周下轨 ({boll_week_lower:.4f})')
                    # print(f'\n# 月 ({stock_month_date}) 蜡烛最低值 ({stock_month_low:.4f}) 月中轨 ({boll_month_mid:.4f})')

                if stock_day_low <= boll_day_lower and stock_week_low <= boll_week_lower:
                    print(f'日期 {i_date} @ 触发: Boll 日下轨 + 周下轨 * 建议: 买入')
                    # print(f'\n# 日 ({stock_day_date}) 蜡烛最低值 ({stock_day_low:.4f}) 日下轨 ({boll_day_lower:.4f})')
                    # print(f'\n# 周 ({stock_week_date}) 蜡烛最低值 ({stock_week_low:.4f}) 周下轨 ({boll_week_lower:.4f})')

                if stock_day_low <= boll_day_lower and stock_week_high >= boll_week_upper:
                    print(f'日期 {i_date} @ 触发: Boll 日下轨 + 周上轨 * 建议: 卖出')
                    # print(f'\n# 日 ({stock_day_date}) 蜡烛最低值 ({stock_day_low:.4f}) 日下轨 ({boll_day_lower:.4f})')
                    # print(f'\n# 周 ({stock_week_date}) 蜡烛最低值 ({stock_week_low:.4f}) 周上轨 ({boll_week_upper:.4f})')


class FirstBreakthroughSignal:

    def __init__(self, symbol, selected_date=None, period=2, adjust='raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.period = period
        self.adjust = adjust

        print(f'\n2、首突均线策略')
        self.condition()

    def condition(self):
        print(f'(1) 首突: 周 20 突破周 60')
        selected_date = datetime.date(2026, 1, 2) if not self.selected_date else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=365*self.period + 7)

        ma_week = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / f'ma_week.parquet')
        selected_ma_week = ma_week.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        for i_date in selected_ma_week['date'][::-1]:
            ma_week_20 = selected_ma_week.filter(pl.col('date') <= i_date)['ma_20']
            ma_week_60 = selected_ma_week.filter(pl.col('date') <= i_date)['ma_60']
            
            if len(ma_week_20) > 1 and len(ma_week_60) > 1 and ma_week_20[-1] > ma_week_60[-1] and ma_week_20[-2] < ma_week_60[-2]:
                breakthrough_date = copy.deepcopy(i_date)
                print(f'日期 {breakthrough_date} @ 触发 MA-Week-20 首次突破 MA-Week-60')
                # print(f'# 当日 MA-Week-20 ({ma_week_20[-1]:.4f}) > MA-Week-60 ({ma_week_60[-1]:.4f})')
                # print(f'# 前日 MA-Week-20 ({ma_week_20[-2]:.4f}) < MA-Week-60 ({ma_week_60[-2]:.4f})')
                break
        else:
            breakthrough_date = None
            # print(f'未触发 MA-Week-20 突破 MA-Week-60')

        print(f'(2) 首突回踩：首突后回踩月线中轨（最佳买点）')
        if breakthrough_date:
            stock_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'stock_month.parquet')
            boll_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'boll_month.parquet')

            range_date = stock_month.filter(pl.col('date') >= breakthrough_date)['date']
            for i_date in range_date:
                selected_stock_month = stock_month.filter((pl.col('date') >= breakthrough_date) & (pl.col('date') <= i_date))
                selected_boll_month = boll_month.filter((pl.col('date') >= breakthrough_date) & (pl.col('date') <= i_date))
                
                # stock_month_date = selected_stock_month['date'][-1]
                stock_month_low = selected_stock_month[f'{self.adjust}_low'][-1]
                boll_month_mid = selected_boll_month['boll_mid'][-1]

                if stock_month_low <= boll_month_mid:
                    print(f'日期 {i_date} @ 触发: 回踩 Boll 月中轨 * 建议: 买入 (最佳买点)')
                    # print(f'\n# 月 ({stock_month_date}) 蜡烛最低值 ({stock_month_low:.4f}) 月中轨 ({boll_month_mid:.4f})')


class BottomAdjustmentSignal:

    def __init__(self, symbol, selected_date=None, period=2, adjust='raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.period = period
        self.adjust = adjust

        print(f'\n3、底部调整启动信号')
        self.condition1()
        self.condition2()
        self.condition3()

    def condition1(self):
        print(f'(1) 月线下轨+macd水下金叉 (同步看估值等)')
        selected_date = datetime.date(2026, 1, 2) if not self.selected_date else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=365*self.period + 7)

        boll_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / f'boll_month.parquet')
        selected_boll_month = boll_month.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))
        stock_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'stock_month.parquet')
        selected_stock_month = stock_month.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        for i_date in selected_boll_month['date'][::-1]:
            stock_month_low = selected_stock_month.filter(pl.col('date') <= i_date)[f'{self.adjust}_low']
            boll_month_lower = selected_boll_month.filter(pl.col('date') <= i_date)['boll_lower']

            if stock_month_low[-1] <= boll_month_lower[-1]:
                print(f'日期 {i_date} @ 触发: Boll 月下轨 * 建议: 买入 (同步确认 MACD 月线水下金叉并查看估值)')
                # print(f'\n# 月 ({i_date}) 蜡烛最低值 ({stock_month_low[-1]:.4f}) 月下轨 ({boll_month_lower[-1]:.4f})')
                break
        else:
            pass
            # print(f'未触发 Boll 月下轨')

        macd_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / f'macd_month.parquet')
        selected_macd_month = macd_month.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        for i_date in selected_macd_month['date'][::-1]:
            dif_data = selected_macd_month.filter(pl.col('date') <= i_date)['dif']
            dea_data = selected_macd_month.filter(pl.col('date') <= i_date)['dea']

            if len(dif_data) > 1 and len(dea_data) > 1 and dif_data[-2] < dea_data[-2] < 0 and dea_data[-1] < dif_data[-1] < 0:
                print(f'日期 {i_date} @ 触发: MACD 月线水下金叉 * 建议: 买入 (同步确认 Boll 月下轨并查看估值)')
                # print(f'\n# 前日 Dif ({dif_data[-2]:.4f}) < Dea ({dea_data[-2]:.4f}) < 0'
                #       f'\n# 当日 Dea ({dea_data[-1]:.4f}) < Dif ({dif_data[-1]:.4f}) < 0')
                break
        else:
            pass
            # print(f'未触发 MACD 月线水下金叉')
    
    def condition2(self):
        print(f'(2) 季线水下金叉 (稀缺)')
        selected_date = datetime.date(2026, 1, 1) if not self.selected_date else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=365*self.period + 7)

        macd_quarter = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / f'macd_quarter.parquet')
        selected_macd_quarter = macd_quarter.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        for i_date in selected_macd_quarter['date'][::-1]:
            dif_data = selected_macd_quarter.filter(pl.col('date') <= i_date)['dif']
            dea_data = selected_macd_quarter.filter(pl.col('date') <= i_date)['dea']

            if len(dif_data) > 1 and len(dea_data) > 1 and dif_data[-2] < dea_data[-2] < 0 and dea_data[-1] < dif_data[-1] < 0:
                print(f'日期 {i_date} @ 触发: MACD 季线水下金叉 * 建议: 买入 (稀缺)')
                # print(f'\n# 前日 Dif ({dif_data[-2]:.4f}) < Dea ({dea_data[-2]:.4f}) < 0'
                #       f'\n# 当日 Dea ({dea_data[-1]:.4f}) < Dif ({dif_data[-1]:.4f}) < 0')
                break
        else:
            pass
            # print(f'未触发 MACD 季线水下金叉')

    def condition3(self):
        print(f'(3) 站上周250、月60 (分两次确认)')
        selected_date = datetime.date(2026, 1, 1) if not self.selected_date else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=365*self.period + 7)

        stock_week = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'stock_week.parquet')
        selected_stock_week = stock_week.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        ma_week = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / f'ma_week.parquet')
        selected_ma_week = ma_week.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        for i_date in selected_ma_week['date'][::-1]:
            stock_week_low = selected_stock_week.filter(pl.col('date') <= i_date)[f'{self.adjust}_low']
            ma_week_250 = selected_ma_week.filter(pl.col('date') <= i_date)['ma_250']

            if stock_week_low[-2] < ma_week_250[-2] and stock_week_low[-1] >= ma_week_250[-1]:
                print(f'日期 {i_date} @ 触发: 站上 MA-Week-250 * 建议: 买入 (同步确认是否站上 MA-Month-60 )')
                # print(f'\n# 前周最低 ({stock_week_low[-2]:.4f}) < MA-Week-250 ({ma_week_250[-2]:.4f})'
                #       f'\n# 当周最低 ({stock_week_low[-1]:.4f}) >= MA-Week-250 ({ma_week_250[-1]:.4f})')
                break
        else:
            pass
            # print(f'未触发 站上 MA-Week-250')

        stock_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / 'stock_month.parquet')
        selected_stock_month = stock_month.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        ma_month = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / self.symbol / f'ma_month.parquet')
        selected_ma_month = ma_month.filter((pl.col('date') <= selected_date) & (pl.col('date') >= limited_date))

        for i_date in selected_ma_month['date'][::-1]:
            stock_month_low = selected_stock_month.filter(pl.col('date') <= i_date)[f'{self.adjust}_low']
            ma_month_60 = selected_ma_month.filter(pl.col('date') <= i_date)['ma_60']

            if stock_month_low[-2] < ma_month_60[-2] and stock_month_low[-1] >= ma_month_60[-1]:
                print(f'日期 {i_date} @ 触发: 站上 MA-Month-60 * 建议: 买入 (同步确认是否站上 MA-Week-250 )')
                # print(f'\n# 前月最低 ({stock_month_low[-2]:.4f}) < MA-Month-60 ({ma_month_60[-2]:.4f})'
                #       f'\n# 当月最低 ({stock_month_low[-1]:.4f}) >= MA-Month-60 ({ma_month_60[-1]:.4f})')
                break
        else:
            pass
            # print(f'未触发 站上 MA-Month-60')


if __name__ == '__main__':
    from algorithm.basic.plot import Plotting

    AscendSignalWeekRotation('sh600036', period=2)
    FirstBreakthroughSignal('sh600036', period=2)
    BottomAdjustmentSignal('sh600036', period=2)

    # Plotting('sh600036', period='day', window=300)
    Plotting('sh600036', period='week', window=1000)
    Plotting('sh600036', period='month')

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import datetime
import polars as pl
import akshare as ak
import akshare_proxy_patch

from project.configuration import Config


akshare_proxy_patch.install_patch("101.201.173.125", "", 30)
pl.Config(tbl_rows=12, tbl_cols=-1)


class FetchLimitup:

    @classmethod
    def run(cls, selected_date=datetime.date.today()):
        today = selected_date.strftime('%Y%m%d')

        raw_limitup_data = pl.from_pandas(ak.stock_zt_pool_em(date=today))
        raw_limitup_data.write_parquet(Config['Paths']['DataPath'] / 'input' / f'today.parquet')
        print(f'{selected_date} 涨停数据下载完成 \n{raw_limitup_data}')


class WriteData:

    @classmethod
    def run(cls, symbol, refresh=False):
        os.makedirs(Config['Paths']['DataPath'] / 'input' / symbol) if not os.path.exists(Config['Paths']['DataPath'] / 'input' / symbol) else None
        cls.all_stocks_data(symbol, refresh=refresh)
        cls.updated_stocks_data(symbol)

    @classmethod
    def all_stocks_data(cls, symbol='600025', start_date='20220101', end_date='20251231', refresh=False):
        if not refresh and os.path.exists(Config['Paths']['DataPath'] / 'input' / symbol / 'stock.parquet'):
            print(f'{symbol} 全量数据已存在')
        else:
            qfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq'))
            hfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust='hfq'))
            print(f'{symbol} 全量数据下载完成')

            all_data = pl.DataFrame({
                '日期': qfq_data['日期'],
                '开盘': qfq_data['开盘'],
                '最高': qfq_data['最高'], 
                '最低': qfq_data['最低'], 
                '收盘': qfq_data['收盘'],
                '复权开盘': hfq_data['开盘'], 
                '复权最高': hfq_data['最高'], 
                '复权最低': hfq_data['最低'], 
                '复权收盘': hfq_data['收盘'],
                '成交量': qfq_data['成交量'], 
                '成交额': qfq_data['成交额'], 
                '换手率': qfq_data['换手率'],
                '振幅': qfq_data['振幅'], 
                '涨跌幅': qfq_data['涨跌幅'], 
                '复权振幅': hfq_data['振幅'], 
                '复权涨跌幅': hfq_data['涨跌幅'],
                '涨跌额': qfq_data['涨跌额'],
            })
            all_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'stock.parquet')
            print(f'{symbol} 全量数据合并完成 \n{all_data}')

    @classmethod
    def updated_stocks_data(cls, symbol='sh600025'):
        all_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'stock.parquet')
        last_date, today_date,  = all_data['日期'].max(), datetime.date.today()
        print(f'数据日期: {last_date} \n今天日期: {today_date}')

        if last_date >= today_date:
            print(f'{symbol} 增量数据已存在')
        else:
            qfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=last_date.strftime('%Y%m%d'), end_date=today_date.strftime('%Y%m%d'), adjust='qfq'))
            hfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=last_date.strftime('%Y%m%d'), end_date=today_date.strftime('%Y%m%d'), adjust='hfq'))
            print(f'{symbol} 增量数据下载完成')

            update_data = pl.DataFrame({
                '日期': qfq_data['日期'],
                '开盘': qfq_data['开盘'],
                '最高': qfq_data['最高'], 
                '最低': qfq_data['最低'], 
                '收盘': qfq_data['收盘'],
                '复权开盘': hfq_data['开盘'], 
                '复权最高': hfq_data['最高'], 
                '复权最低': hfq_data['最低'], 
                '复权收盘': hfq_data['收盘'],
                '成交量': qfq_data['成交量'], 
                '成交额': qfq_data['成交额'], 
                '换手率': qfq_data['换手率'],
                '振幅': qfq_data['振幅'], 
                '涨跌幅': qfq_data['涨跌幅'], 
                '复权振幅': hfq_data['振幅'], 
                '复权涨跌幅': hfq_data['涨跌幅'],
                '涨跌额': qfq_data['涨跌额'],
            })
            new_data = pl.concat([all_data, update_data]).unique(subset=['日期'], keep='last').sort('日期')
            new_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'stock.parquet')
            print(f'{symbol} 增量数据合并完成 \n{new_data}')


class SplitData:

    @classmethod
    def run(cls, symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'stock.parquet')

        # 按日天统计数据
        day_data = raw_data.with_columns(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.ordinal_day().alias('日'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色')
        ).select(
            '年', '日', pl.exclude('年', '日')
        ).sort('日期')
        day_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'day.parquet')
        print(day_data)

        # 按周度统计数据
        week_data = raw_data.group_by(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.week().alias('周')
        ).agg([
            pl.col('日期').last().alias('日期'),
            pl.col('开盘').first().alias('开盘'),
            pl.col('最高').max().alias('最高'),
            pl.col('最低').min().alias('最低'),
            pl.col('收盘').last().alias('收盘'),
            pl.col('复权开盘').first().alias('复权开盘'),
            pl.col('复权最高').max().alias('复权最高'),
            pl.col('复权最低').min().alias('复权最低'),
            pl.col('复权收盘').last().alias('复权收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            ((pl.col('复权最高') - pl.col('复权最低')) / pl.col('复权最低') * 100).alias('复权振幅'),
            ((pl.col('复权收盘') - pl.col('复权开盘')) / pl.col('复权开盘') * 100).alias('复权涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        week_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'week.parquet')
        print(week_data)

        # 按月度统计数据
        month_data = raw_data.group_by(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.month().alias('月'),
        ).agg([
            pl.col('日期').last().alias('日期'),
            pl.col('开盘').first().alias('开盘'),
            pl.col('最高').max().alias('最高'),
            pl.col('最低').min().alias('最低'),
            pl.col('收盘').last().alias('收盘'),
            pl.col('复权开盘').first().alias('复权开盘'),
            pl.col('复权最高').max().alias('复权最高'),
            pl.col('复权最低').min().alias('复权最低'),
            pl.col('复权收盘').last().alias('复权收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            ((pl.col('复权最高') - pl.col('复权最低')) / pl.col('复权最低') * 100).alias('复权振幅'),
            ((pl.col('复权收盘') - pl.col('复权开盘')) / pl.col('复权开盘') * 100).alias('复权涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        month_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'month.parquet')
        print(month_data)


class Indices:

    @classmethod
    def run(cls, symbol):
        for period in ['day', 'week', 'month']:
            raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'{period}.parquet')
            cls.moving_average(symbol, raw_data)
            cls.moving_average_convergence_divergence(symbol, raw_data)
            cls.bollinger_bands(symbol, raw_data)

    @classmethod
    def moving_average(cls, symbol, raw_data):
        # 计算移动平均线 MA
        new_data = raw_data.select([
            pl.col('日期'),
            pl.col(f'收盘').rolling_mean(5).alias('MA5'),             # 005 trading period
            pl.col(f'收盘').rolling_mean(10).alias('MA10'),           # 010 trading period
            pl.col(f'收盘').rolling_mean(20).alias('MA20'),           # 020 trading period
            pl.col(f'收盘').rolling_mean(30).alias('MA30'),           # 030 trading period
            pl.col(f'收盘').rolling_mean(60).alias('MA60'),           # 060 trading period
            pl.col(f'复权收盘').rolling_mean(5).alias('复权MA5'),      # 005 trading period
            pl.col(f'复权收盘').rolling_mean(10).alias('复权MA10'),    # 010 trading period
            pl.col(f'复权收盘').rolling_mean(20).alias('复权MA20'),    # 020 trading period
            pl.col(f'复权收盘').rolling_mean(30).alias('复权MA30'),    # 030 trading period
            pl.col(f'复权收盘').rolling_mean(60).alias('复权MA60'),    # 060 trading period
        ])
        new_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'ma.parquet')
        print(new_data)

    @classmethod
    def moving_average_convergence_divergence(cls, symbol, raw_data, fast=12, slow=26, span=9):
        # 计算指数移动平均线 EMA、快线 DIF (12日)、慢线 DEA (26日)、柱状图 MACD (9日)
        ema = pl.DataFrame({
            '日期': raw_data['日期'],
            'EMA快线': raw_data.select([pl.col(f'收盘').ewm_mean(span=fast, adjust=False)]),
            'EMA慢线': raw_data.select([pl.col(f'收盘').ewm_mean(span=slow, adjust=False)]),
            '复权EMA快线': raw_data.select([pl.col(f'复权收盘').ewm_mean(span=fast, adjust=False)]),
            '复权EMA慢线': raw_data.select([pl.col(f'复权收盘').ewm_mean(span=slow, adjust=False)]),
        })
        new_data = ema.select([
            pl.col('日期'),
            (pl.col('EMA快线')-pl.col('EMA慢线')).alias('DIF线'),
            (pl.col('EMA快线')-pl.col('EMA慢线')).ewm_mean(span=span, adjust=False).alias('DEA线'),
            ((pl.col('EMA快线')-pl.col('EMA慢线')-(pl.col('EMA快线')-pl.col('EMA慢线')).ewm_mean(span=span, adjust=False))*2).alias('MACD'),
            (pl.col('复权EMA快线')-pl.col('复权EMA慢线')).alias('复权DIF线'),
            (pl.col('复权EMA快线')-pl.col('复权EMA慢线')).ewm_mean(span=span, adjust=False).alias('复权DEA线'),
            ((pl.col('复权EMA快线')-pl.col('复权EMA慢线')-(pl.col('复权EMA快线')-pl.col('复权EMA慢线')).ewm_mean(span=span, adjust=False))*2).alias('复权MACD'),
        ]).with_columns(
            pl.when(pl.col('MACD') >= 0).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
            pl.when(pl.col('复权MACD') >= 0).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('复权颜色'),
        )
        new_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'macd.parquet')
        print(new_data)

    @classmethod
    def bollinger_bands(cls, symbol, raw_data, mean=20, std=2):
        # 计算布林线 20 周期 + 2 倍标准差
        new_data = raw_data.select([
            pl.col('日期'),
            pl.col(f'收盘').rolling_mean(mean).alias('Boll中轨'),
            (pl.col(f'收盘').rolling_mean(mean) + pl.col(f'收盘').rolling_std(mean) * std).alias('Boll上轨'),
            (pl.col(f'收盘').rolling_mean(mean) - pl.col(f'收盘').rolling_std(mean) * std).alias('Boll下轨'),
            pl.col(f'复权收盘').rolling_mean(mean).alias('复权Boll中轨'),
            (pl.col(f'复权收盘').rolling_mean(mean) + pl.col(f'复权收盘').rolling_std(mean) * std).alias('复权Boll上轨'),
            (pl.col(f'复权收盘').rolling_mean(mean) - pl.col(f'复权收盘').rolling_std(mean) * std).alias('复权Boll下轨'),
        ])
        new_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'boll.parquet')
        print(new_data)


if __name__ == '__main__':
    # WriteData().run(symbol='600036')
    SplitData().run(symbol='600036')
    Indices().run(symbol='600036')

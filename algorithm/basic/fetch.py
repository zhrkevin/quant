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

from project import Config
from algorithm.dividend.product import Stocks, ETFs


akshare_proxy_patch.install_patch(auth_ip="101.201.173.125", auth_token="20260403HAHSSA7W")
pl.Config(tbl_rows=12, tbl_cols=8)


class WriteData:

    @classmethod
    def stocks(cls, symbol, refresh=False):
        os.makedirs(Config['Paths']['DataPath'] / 'stock' / symbol) if not os.path.exists(Config['Paths']['DataPath'] / 'stock' / symbol) else None
        cls.all_stocks_data(symbol, refresh=refresh)
        cls.updated_stocks_data(symbol)
    
    @classmethod
    def etfs(cls, symbol, refresh=False):
        os.makedirs(Config['Paths']['DataPath'] / 'etf' / symbol) if not os.path.exists(Config['Paths']['DataPath'] / 'etf' / symbol) else None
        cls.all_etfs_data(symbol, refresh=refresh)
        cls.updated_etfs_data(symbol)

    @classmethod
    def all_stocks_data(cls, symbol, start_date='20150101', end_date='20251231', refresh=False):
        if not refresh and os.path.exists(Config['Paths']['DataPath'] / 'stock' / symbol / 'raw.parquet'):
            print(f'{symbol} 全量数据已存在')
        else:
            raw_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust=''))
            qfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq'))
            hfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust='hfq'))
            print(f'{symbol} 全量数据下载完成')

            all_data = pl.DataFrame({
                '日期': raw_data['日期'],
                '开盘': qfq_data['开盘'],
                '最高': qfq_data['最高'], 
                '最低': qfq_data['最低'], 
                '收盘': qfq_data['收盘'],
                '复权开盘': hfq_data['开盘'], 
                '复权最高': hfq_data['最高'], 
                '复权最低': hfq_data['最低'], 
                '复权收盘': hfq_data['收盘'],
                '除权开盘': raw_data['开盘'],
                '除权最高': raw_data['最高'], 
                '除权最低': raw_data['最低'], 
                '除权收盘': raw_data['收盘'],
                '成交量': qfq_data['成交量'], 
                '成交额': qfq_data['成交额'], 
                '换手率': qfq_data['换手率'],
                '涨跌额': qfq_data['涨跌额'],
                '除权振幅': qfq_data['振幅'],
                '除权涨跌幅': qfq_data['涨跌幅'],
                '振幅': qfq_data['振幅'], 
                '涨跌幅': qfq_data['涨跌幅'],
                '复权振幅': hfq_data['振幅'], 
                '复权涨跌幅': hfq_data['涨跌幅'],
            })
            all_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'raw.parquet')
            print(f'{symbol} 全量数据合并完成 \n{all_data}')

    @staticmethod
    def updated_stocks_data(symbol):
        all_data = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'raw.parquet')
        last_date, today_date = all_data['日期'].max(), datetime.date.today()
        print(f'数据日期: {last_date} 今天日期: {today_date}')

        if last_date >= today_date:
            print(f'{symbol} 增量数据已更新')
        else:
            raw_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=last_date.strftime('%Y%m%d'), end_date=today_date.strftime('%Y%m%d'), adjust=''))
            qfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=last_date.strftime('%Y%m%d'), end_date=today_date.strftime('%Y%m%d'), adjust='qfq'))
            hfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=last_date.strftime('%Y%m%d'), end_date=today_date.strftime('%Y%m%d'), adjust='hfq'))
            print(f'{symbol} 增量数据下载完成')

            updated_data = pl.DataFrame({
                '日期': raw_data['日期'],
                '开盘': qfq_data['开盘'],
                '最高': qfq_data['最高'], 
                '最低': qfq_data['最低'], 
                '收盘': qfq_data['收盘'],
                '复权开盘': hfq_data['开盘'], 
                '复权最高': hfq_data['最高'], 
                '复权最低': hfq_data['最低'], 
                '复权收盘': hfq_data['收盘'],
                '除权开盘': raw_data['开盘'],
                '除权最高': raw_data['最高'], 
                '除权最低': raw_data['最低'], 
                '除权收盘': raw_data['收盘'],
                '成交量': qfq_data['成交量'], 
                '成交额': qfq_data['成交额'], 
                '换手率': qfq_data['换手率'],
                '涨跌额': qfq_data['涨跌额'],
                '除权振幅': qfq_data['振幅'],
                '除权涨跌幅': qfq_data['涨跌幅'],
                '振幅': qfq_data['振幅'], 
                '涨跌幅': qfq_data['涨跌幅'],
                '复权振幅': hfq_data['振幅'], 
                '复权涨跌幅': hfq_data['涨跌幅'],
            })
            new_data = pl.concat([all_data, updated_data]).unique(subset=['日期'], keep='last').sort('日期')
            new_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'raw.parquet')
            print(f'{symbol} 全量 + 增量数据合并完成 \n{new_data}')

    @classmethod
    def all_etfs_data(cls, symbol, refresh=False):  
        if not refresh and os.path.exists(Config['Paths']['DataPath'] / 'etf' / symbol / 'raw.parquet'):
            print(f'{symbol} 全量数据已存在')
        else:
            raw_data = pl.from_pandas(ak.index_zh_a_hist(symbol=symbol, start_date='20150101', end_date='20251231'))
            print(f'{symbol} 全量数据下载完成')
            all_data = pl.DataFrame({
                '日期': raw_data['日期'].cast(pl.Date),
                '开盘': raw_data['开盘'],
                '最高': raw_data['最高'], 
                '最低': raw_data['最低'], 
                '收盘': raw_data['收盘'],
                '成交量': raw_data['成交量'], 
                '成交额': raw_data['成交额'],
                '涨跌额': raw_data['涨跌额'],
                '涨跌幅': raw_data['涨跌幅'],
                '振幅': raw_data['振幅'],  
                '换手率': raw_data['换手率'],
            })
            all_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'raw.parquet')
            print(f'{symbol} 全量数据下载完成 \n{all_data}')

    @classmethod
    def updated_etfs_data(cls, symbol):
        all_data = pl.read_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'raw.parquet')
        last_date, today_date = all_data['日期'].max(), datetime.date.today()
        print(f'{symbol} 数据日期: {last_date} 今天日期: {today_date}')

        if last_date >= today_date:
            print(f'{symbol} 增量数据已更新')
        else:
            raw_data = pl.from_pandas(ak.index_zh_a_hist(symbol=symbol, start_date=last_date.strftime('%Y%m%d'), end_date=today_date.strftime('%Y%m%d')))
            print(f'{symbol} 增量数据下载完成')
            updated_data = pl.DataFrame({
                '日期': raw_data['日期'].cast(pl.Date),
                '开盘': raw_data['开盘'],
                '最高': raw_data['最高'], 
                '最低': raw_data['最低'], 
                '收盘': raw_data['收盘'],
                '成交量': raw_data['成交量'], 
                '成交额': raw_data['成交额'],
                '涨跌额': raw_data['涨跌额'],
                '涨跌幅': raw_data['涨跌幅'],
                '振幅': raw_data['振幅'],  
                '换手率': raw_data['换手率'],
            })
            new_data = pl.concat([all_data, updated_data]).unique(subset=['日期'], keep='last').sort('日期')
            new_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'raw.parquet')
            print(f'{symbol} 全量 + 增量数据合并完成 \n{new_data}')


class SplitData:

    @classmethod
    def stocks(cls, symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'raw.parquet')

        # 按日天统计数据
        day_data = raw_data.with_columns(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.ordinal_day().alias('日'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色')
        ).select(
            '年', '日', pl.exclude('年', '日')
        )
        day_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'day.parquet')
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
            pl.col('除权开盘').first().alias('除权开盘'),
            pl.col('除权最高').max().alias('除权最高'),
            pl.col('除权最低').min().alias('除权最低'),
            pl.col('除权收盘').last().alias('除权收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            ((pl.col('复权最高') - pl.col('复权最低')) / pl.col('复权最低') * 100).alias('复权振幅'),
            ((pl.col('复权收盘') - pl.col('复权开盘')) / pl.col('复权开盘') * 100).alias('复权涨跌幅'),
            ((pl.col('除权最高') - pl.col('除权最低')) / pl.col('除权最低') * 100).alias('除权振幅'),
            ((pl.col('除权收盘') - pl.col('除权开盘')) / pl.col('除权开盘') * 100).alias('除权涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        week_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'week.parquet')
        print(week_data)

        # 按月度统计数据
        month_data  = raw_data.group_by(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.month().alias('月')
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
            pl.col('除权开盘').first().alias('除权开盘'),
            pl.col('除权最高').max().alias('除权最高'),
            pl.col('除权最低').min().alias('除权最低'),
            pl.col('除权收盘').last().alias('除权收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            ((pl.col('复权最高') - pl.col('复权最低')) / pl.col('复权最低') * 100).alias('复权振幅'),
            ((pl.col('复权收盘') - pl.col('复权开盘')) / pl.col('复权开盘') * 100).alias('复权涨跌幅'),
            ((pl.col('除权最高') - pl.col('除权最低')) / pl.col('除权最低') * 100).alias('除权振幅'),
            ((pl.col('除权收盘') - pl.col('除权开盘')) / pl.col('除权开盘') * 100).alias('除权涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        month_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'month.parquet')
        print(month_data)
        
        # 按季度统计数据
        quarter_data = raw_data.group_by(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.quarter().alias('季度')
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
            pl.col('除权开盘').first().alias('除权开盘'),
            pl.col('除权最高').max().alias('除权最高'),
            pl.col('除权最低').min().alias('除权最低'),
            pl.col('除权收盘').last().alias('除权收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            ((pl.col('复权最高') - pl.col('复权最低')) / pl.col('复权最低') * 100).alias('复权振幅'),
            ((pl.col('复权收盘') - pl.col('复权开盘')) / pl.col('复权开盘') * 100).alias('复权涨跌幅'),
            ((pl.col('除权最高') - pl.col('除权最低')) / pl.col('除权最低') * 100).alias('除权振幅'),
            ((pl.col('除权收盘') - pl.col('除权开盘')) / pl.col('除权开盘') * 100).alias('除权涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        quarter_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'quarter.parquet')
        print(quarter_data)

    @classmethod
    def etfs(cls, symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'raw.parquet')
        print(raw_data)

        # 按日天统计数据
        day_data = raw_data.with_columns(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.ordinal_day().alias('日'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色')
        ).select(
            '年', '日', pl.exclude('年', '日')
        )
        day_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'day.parquet')
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
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        week_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'week.parquet')
        print(week_data)

        # 按月度统计数据
        month_data = raw_data.group_by(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.month().alias('月')
        ).agg([
            pl.col('日期').last().alias('日期'),
            pl.col('开盘').first().alias('开盘'),
            pl.col('最高').max().alias('最高'),
            pl.col('最低').min().alias('最低'),
            pl.col('收盘').last().alias('收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')

        month_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'month.parquet')
        print(month_data)
        
        # 按季度统计数据
        quarter_data = raw_data.group_by(
            pl.col('日期').dt.year().alias('年'),
            pl.col('日期').dt.week().alias('周')
        ).agg([
            pl.col('日期').last().alias('日期'),
            pl.col('开盘').first().alias('开盘'),
            pl.col('最高').max().alias('最高'),
            pl.col('最低').min().alias('最低'),
            pl.col('收盘').last().alias('收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        quarter_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / symbol / 'quarter.parquet')
        print(quarter_data)


class Indices:

    @classmethod
    def run(cls):
        for period in ['day', 'week', 'month', 'quarter']:
            for stock in Stocks:
                raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'stock' / stock / f'{period}.parquet')
                
                ma_data = cls.moving_average('stock', raw_data)
                ma_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / stock / f'ma_{period}.parquet')
                
                macd_data = cls.moving_average_convergence_divergence('stock', raw_data)
                macd_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / stock / f'macd_{period}.parquet')
                
                boll_data = cls.bollinger_bands('stock', raw_data)
                boll_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / stock / f'boll_{period}.parquet')

            for etf in ETFs:
                raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'etf' / etf / f'{period}.parquet')
                
                ma_data = cls.moving_average('etf', raw_data)
                ma_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / etf / f'ma_{period}.parquet')
                
                macd_data = cls.moving_average_convergence_divergence('etf', raw_data)
                macd_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / etf / f'macd_{period}.parquet')
                
                boll_data = cls.bollinger_bands('etf', raw_data)    
                boll_data.write_parquet(Config['Paths']['DataPath'] / 'etf' / etf / f'boll_{period}.parquet')

    @classmethod
    def moving_average(cls, product, raw_data):
        # 计算移动平均线 MA 5, 10, 20, 30, 60
        basic_data = raw_data.select([
            pl.col('日期'),
            pl.col(f'收盘').rolling_mean(5).alias('MA5'),    
            pl.col(f'收盘').rolling_mean(10).alias('MA10'), 
            pl.col(f'收盘').rolling_mean(20).alias('MA20'), 
            pl.col(f'收盘').rolling_mean(30).alias('MA30'), 
            pl.col(f'收盘').rolling_mean(60).alias('MA60'), 
            pl.col(f'收盘').rolling_mean(250).alias('MA250'), 
        ])

        if product == 'stock':
            supplyment_data = raw_data.select([
                pl.col('日期'),
                pl.col(f'复权收盘').rolling_mean(5).alias('复权MA5'),    
                pl.col(f'复权收盘').rolling_mean(10).alias('复权MA10'), 
                pl.col(f'复权收盘').rolling_mean(20).alias('复权MA20'), 
                pl.col(f'复权收盘').rolling_mean(30).alias('复权MA30'), 
                pl.col(f'复权收盘').rolling_mean(60).alias('复权MA60'), 
                pl.col(f'复权收盘').rolling_mean(250).alias('复权MA250'), 
                pl.col(f'除权收盘').rolling_mean(5).alias('除权MA5'),    
                pl.col(f'除权收盘').rolling_mean(10).alias('除权MA10'), 
                pl.col(f'除权收盘').rolling_mean(20).alias('除权MA20'), 
                pl.col(f'除权收盘').rolling_mean(30).alias('除权MA30'), 
                pl.col(f'除权收盘').rolling_mean(60).alias('除权MA60'), 
                pl.col(f'除权收盘').rolling_mean(250).alias('除权MA250'), 
            ])
            new_data = basic_data.join(supplyment_data, on='日期', how='left')
        else:
            new_data = basic_data
        
        print(new_data)
        return new_data

    @classmethod
    def moving_average_convergence_divergence(cls, product, raw_data, fast=12, slow=26, span=9):
        # 计算指数移动平均线 EMA、快线 DIF、慢线 DEA、柱状图 MACD 
        basic_ema = pl.DataFrame({
            '日期': raw_data['日期'],
            'EMA快线': raw_data.select([pl.col(f'收盘').ewm_mean(span=fast, adjust=False)]),
            'EMA慢线': raw_data.select([pl.col(f'收盘').ewm_mean(span=slow, adjust=False)]),
        })
        basic_data = basic_ema.select([
            pl.col('日期'),
            (pl.col('EMA快线')-pl.col('EMA慢线')).alias('DIF'),
            (pl.col('EMA快线')-pl.col('EMA慢线')).ewm_mean(span=span, adjust=False).alias('DEA'),
            ((pl.col('EMA快线')-pl.col('EMA慢线')-(pl.col('EMA快线')-pl.col('EMA慢线')).ewm_mean(span=span, adjust=False))*2).alias('MACD'),
        ]).with_columns(
            pl.when(pl.col('MACD') >= 0).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        )

        if product == 'stock':
            supplyment_ema = pl.DataFrame({
                '日期': raw_data['日期'],
                '复权EMA快线': raw_data.select([pl.col(f'复权收盘').ewm_mean(span=fast, adjust=False)]),
                '复权EMA慢线': raw_data.select([pl.col(f'复权收盘').ewm_mean(span=slow, adjust=False)]),
                '除权EMA快线': raw_data.select([pl.col(f'除权收盘').ewm_mean(span=fast, adjust=False)]),
                '除权EMA慢线': raw_data.select([pl.col(f'除权收盘').ewm_mean(span=slow, adjust=False)]),
            })
            supplyment_data = supplyment_ema.select([
                pl.col('日期'),
                (pl.col('复权EMA快线')-pl.col('复权EMA慢线')).alias('复权DIF'),
                (pl.col('复权EMA快线')-pl.col('复权EMA慢线')).ewm_mean(span=span, adjust=False).alias('复权DEA'),
                ((pl.col('复权EMA快线')-pl.col('复权EMA慢线')-(pl.col('复权EMA快线')-pl.col('复权EMA慢线')).ewm_mean(span=span, adjust=False))*2).alias('复权MACD'),
                (pl.col('除权EMA快线')-pl.col('除权EMA慢线')).alias('除权DIF'),
                (pl.col('除权EMA快线')-pl.col('除权EMA慢线')).ewm_mean(span=span, adjust=False).alias('除权DEA'),
                ((pl.col('除权EMA快线')-pl.col('除权EMA慢线')-(pl.col('除权EMA快线')-pl.col('除权EMA慢线')).ewm_mean(span=span, adjust=False))*2).alias('除权MACD'),
            ]).with_columns(
                pl.when(pl.col('复权MACD') >= 0).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('复权颜色'),
                pl.when(pl.col('除权MACD') >= 0).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('除权颜色'),
            )
            new_data = basic_data.join(supplyment_data, on='日期', how='left')
        else:
            new_data = basic_data

        print(new_data)
        return new_data

    @classmethod
    def bollinger_bands(cls, product, raw_data, mean=20, std=2):
        # 计算布林线 20 周期 + 2 倍标准差
        basic_data = raw_data.select([
            pl.col('日期'),
            pl.col(f'收盘').rolling_mean(mean).alias('Boll中轨'),
            (pl.col(f'收盘').rolling_mean(mean) + pl.col(f'收盘').rolling_std(mean) * std).alias('Boll上轨'),
            (pl.col(f'收盘').rolling_mean(mean) - pl.col(f'收盘').rolling_std(mean) * std).alias('Boll下轨'),
        ])

        if product == 'stock':
            supplyment_data = raw_data.select([
                pl.col('日期'),
                pl.col(f'复权收盘').rolling_mean(mean).alias('复权Boll中轨'),
                (pl.col(f'复权收盘').rolling_mean(mean) + pl.col(f'复权收盘').rolling_std(mean) * std).alias('复权Boll上轨'),
                (pl.col(f'复权收盘').rolling_mean(mean) - pl.col(f'复权收盘').rolling_std(mean) * std).alias('复权Boll下轨'),
                pl.col(f'除权收盘').rolling_mean(mean).alias('除权Boll中轨'),
                (pl.col(f'除权收盘').rolling_mean(mean) + pl.col(f'除权收盘').rolling_std(mean) * std).alias('除权Boll上轨'),
                (pl.col(f'除权收盘').rolling_mean(mean) - pl.col(f'除权收盘').rolling_std(mean) * std).alias('除权Boll下轨'),
            ])
            new_data = basic_data.join(supplyment_data, on='日期', how='left')
        else:
            new_data = basic_data

        print(new_data)
        return new_data


if __name__ == '__main__':
    pass
    # WriteData.etfs('000016', refresh=True)
    SplitData()
    Index.run()
    # Plotting('sh600036', period='day')


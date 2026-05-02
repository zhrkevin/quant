#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import polars as pl
import akshare as ak
import akshare_proxy_patch
from datetime import datetime

from project import Config

akshare_proxy_patch.install_patch(auth_ip="101.201.173.125", auth_token="20260403HAHSSA7W")
pl.Config(tbl_rows=-1, tbl_cols=-1)


class FetchLimitUp:

    @classmethod
    def run(cls, today):
        selected_date = today.strftime('%Y%m%d')
        raw_limitup = pl.from_pandas(ak.stock_zt_pool_em(date=selected_date))
        raw_limitup.write_parquet(Config['Paths']['DataPath'] / 'input' / f'today.parquet')
        print(f'{selected_date} 涨停数据下载完成 \n{raw_limitup}')


class WriteData:

    @classmethod
    def run(cls, symbol, today, windows=365):
        start_date = (today.date() - datetime.timedelta(days=windows)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        print(f'数据日期: {start_date} 今天日期: {end_date}')

        raw_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust=''))
        qfq_data = pl.from_pandas(ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq'))
        print(f'{symbol} 数据下载完成')
        
        new_data = pl.DataFrame({
            '日期': raw_data['日期'],
            '开盘': qfq_data['开盘'],
            '最高': qfq_data['最高'],
            '最低': qfq_data['最低'], 
            '收盘': qfq_data['收盘'],
            '除权开盘': raw_data['开盘'],
            '除权最高': raw_data['最高'],
            '除权最低': raw_data['最低'],
            '除权收盘': raw_data['收盘'],
            '振幅': qfq_data['振幅'],
            '涨跌幅': qfq_data['涨跌幅'],
            '除权振幅': raw_data['振幅'],
            '除权涨跌幅': raw_data['涨跌幅'],
            '涨跌额': qfq_data['涨跌额'],
            '成交量': qfq_data['成交量'],
            '成交额': qfq_data['成交额'],
            '换手率': qfq_data['换手率'],
        })
        new_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'{symbol}.parquet')
        print(f'{symbol} 数据合并完成 \n{new_data}')

    @classmethod
    def split(cls, symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'{symbol}.parquet')

        # 按日天统计数据
        day_data = raw_data.group_by(pl.col('日期').dt.year().alias('年'), pl.col('日期').dt.ordinal_day().alias('日'))
        day_data = cls.aggregation(day_data)
        day_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'{symbol}-day.parquet')
        print(day_data)

        # 按周度统计数据
        week_data = raw_data.group_by(pl.col('日期').dt.year().alias('年'), pl.col('日期').dt.week().alias('周'))
        week_data = cls.aggregation(week_data)
        week_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'{symbol}-week.parquet')
        print(week_data)

        # 按月度统计数据
        month_data  = raw_data.group_by(pl.col('日期').dt.year().alias('年'), pl.col('日期').dt.month().alias('月'))
        month_data = cls.aggregation(month_data)
        month_data.write_parquet(Config['Paths']['DataPath'] / 'input' / symbol / f'{symbol}-month.parquet')
        print(month_data)

    @classmethod
    def aggregation(cls, raw_data):
        raw_data = raw_data.agg([
            pl.col('日期').last().alias('日期'),
            pl.col('开盘').first().alias('开盘'),
            pl.col('最高').max().alias('最高'),
            pl.col('最低').min().alias('最低'),
            pl.col('收盘').last().alias('收盘'),
            pl.col('除权开盘').first().alias('除权开盘'),
            pl.col('除权最高').max().alias('除权最高'),
            pl.col('除权最低').min().alias('除权最低'),
            pl.col('除权收盘').last().alias('除权收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            ((pl.col('复权最高') - pl.col('复权最低')) / pl.col('复权最低') * 100).alias('复权振幅'),
            ((pl.col('复权收盘') - pl.col('复权开盘')) / pl.col('复权开盘') * 100).alias('复权涨跌幅'),
            ((pl.col('除权最高') - pl.col('除权最低')) / pl.col('除权最低') * 100).alias('除权振幅'),
            ((pl.col('除权收盘') - pl.col('除权开盘')) / pl.col('除权开盘') * 100).alias('除权涨跌幅'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        return raw_data


class Indices:

    @classmethod
    def run(cls, product, symbol):
        for period in ['day', 'week', 'month', 'quarter']:
            raw_data = pl.read_parquet(Config['Paths']['DataPath'] / product / symbol / f'{period}.parquet')

            ma_data = cls.moving_average(product, raw_data)
            ma_data.write_parquet(Config['Paths']['DataPath'] / product / symbol / f'ma_{period}.parquet')
            
            macd_data = cls.moving_average_convergence_divergence(product, raw_data)
            macd_data.write_parquet(Config['Paths']['DataPath'] / product / symbol / f'macd_{period}.parquet')
            
            boll_data = cls.bollinger_bands(product, raw_data)
            boll_data.write_parquet(Config['Paths']['DataPath'] / product / symbol / f'boll_{period}.parquet')

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
        elif product == 'etf':
            new_data = basic_data
        else:
            raise ValueError(f"不支持的产品类型: {product}")
        
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
        elif product == 'etf':
            new_data = basic_data
        else:
            raise ValueError(f"不支持的产品类型: {product}")

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
        elif product == 'etf':
            new_data = basic_data
        else:
            raise ValueError(f"不支持的产品类型: {product}")

        print(new_data)
        return new_data


if __name__ == '__main__':
    # WriteData.stocks('000001')
    # SplitData.run('stock', '000001')
    # Indices.run('stock', '000001')
    # Plotting('sh600036', period='day')
    pass

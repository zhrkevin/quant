#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import copy
import datetime
from akshare.futures.symbol_var import symbol
import polars as pl

from project.configuration import Config
from algorithm.basic. import Printf


pl.Config(tbl_rows=-1, tbl_cols=-1)


class Analysis:

    def __init__(self, adjust='raw', output='report-test.xlsx'):
        self.limitup = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / 'today.parquet')
        self.adjust = adjust

        self.sorted()

    def sorted(self):
        # 读取数据
        
        # 只做拆分，不考虑列的位置
        self.limitup = self.limitup.with_columns(
            (pl.col('成交额') / 1e8).alias('成交额(亿)'),
            (pl.col('总市值') / 1e8).alias('总市值(亿)'),
            (pl.col('流通市值') / 1e8).alias('流通市值(亿)'),
            (pl.col('封板资金') / 1e8).alias('封板资金(亿)'),
            pl.col('首次封板时间').str.strptime(pl.Time, '%H%M%S').alias('首次封板时间'),
            pl.col('最后封板时间').str.strptime(pl.Time, '%H%M%S').alias('最后封板时间'),
            pl.col('涨停统计').str.split('/').list[0].cast(pl.Int64).alias('涨停天数'),
            pl.col('涨停统计').str.split('/').list[1].cast(pl.Int64).alias('涨停板数'),
        ).select(
            [
                '序号', '代码', '名称', '最新价', '涨跌幅', '换手率', '成交额(亿)', 
                '流通市值(亿)', '总市值(亿)', '封板资金(亿)', '首次封板时间', '最后封板时间',
                '涨停天数', '涨停板数', '炸板次数', '涨停统计', '连板数', '所属行业',
            ]
        ).sort(
            ['涨停天数', '涨停板数', '炸板次数'], descending=[True, True, False]
        )
        
        Printf.info(f'\n今日 {datetime.date.today()} 涨停个股预览：\n{self.limitup}')
        
    def filter(self):
        for symbol in self.limitup['代码']:
            raw_data = pl.from_pandas(
                ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='')
            )


if __name__ == '__main__':
    Analysis()

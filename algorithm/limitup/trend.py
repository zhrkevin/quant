#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import datetime
import polars as pl

from project import Config
from algorithm.middleware import Logger
from algorithm.limitup.fetch import WriteData, SplitData, Indices


pl.Config(tbl_rows=20, tbl_cols=-1)


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
        
        Logger.info(f'\n今日 {datetime.date.today()} 涨停个股预览：\n{self.limitup}')


class Trend:

    @classmethod
    def run(cls, symbol):
        cls.phase_one(symbol)
        cls.phase_two(symbol)
        cls.phase_three(symbol)
        cls.phase_four(symbol)

    @classmethod
    def phase_one(cls, symbol):
        """
        [第一阶段（底部积累）特征的量化指标]
        1. 30 周均线走平（斜率接近0，表明市场在底部积蓄能量）
        2. 股价在窄幅箱体内震荡（振幅通常 < 20-30%）
        3. 低点逐步抬高（底部吸筹特征）
        4. 成交量萎缩（主力在低位吸筹，但不拉升）
        5. 价格未突破 52 周高点（仍在盘整）
        """
        ma_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'ma.parquet')
        week_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'week.parquet')

        if ma_data.height < 40:
            print(f'数据不足，需要至少 40 周数据')
            return False, {'错误': '数据不足，需要至少40周数据'}

        current_close = week_data.select(pl.col('收盘')).tail(1).item()

        # 1. 30 周均线走平（斜率绝对值 < 2%）
        # 走平的均线是第一阶段的核心特征，表示股票从下跌趋势转为横盘
        recent_ma30 = ma_data.select('MA30').tail(10).to_series()
        ma30_slope = (recent_ma30[-1] - recent_ma30[0]) / recent_ma30[0]
        ma30_flat = abs(ma30_slope) < 0.02

        # 2. 股价箱体震荡（52周振幅 < 30%）
        # 第一阶段的振幅通常较小，表明价格在窄幅区间内波动
        high_52w = week_data.select(pl.col('最高')).tail(52).max().item()
        low_52w = week_data.select(pl.col('最低')).tail(52).min().item()
        amplitude = (high_52w / low_52w) - 1
        price_box = amplitude < 0.3  # 收紧阈值到30%（原50%太宽）

        # 3. 低点抬高（最近20周低点 >= 前20周低点）
        # 底部吸筹特征：虽然整体在盘整，但低点逐步抬高
        low_recent20 = week_data.select(pl.col('最低')).tail(20).min().item()
        low_prev20 = week_data.select(pl.col('最低')).slice(week_data.height - 40, 20).min().item()
        low_higher = low_recent20 >= low_prev20

        # 4. 成交量萎缩（最近10周均量 <= 52周均量的70%）
        # 主力在低位吸筹，成交量应明显萎缩
        vol_ma10 = week_data.select(pl.col('成交量').rolling_mean(10)).tail(1).item()
        vol_ma52 = week_data.select(pl.col('成交量').rolling_mean(52)).tail(1).item()
        vol_shrink = vol_ma10 / vol_ma52 <= 0.7

        # 5. 未突破52周高点（仍在箱体中）
        # 第一阶段价格不应创新高，否则可能已进入第二阶段
        not_break_high = current_close < high_52w * 0.98  # 2%容错空间（原1%）

        # 综合判断：五个条件全部满足才认定为第一阶段
        flag = all([ma30_flat, price_box, low_higher, vol_shrink, not_break_high])
        result = {
            '是否第一阶段': flag,
            '30 周均线走平': ma30_flat,
            '52 周振幅<30%': price_box,
            '低点抬高': low_higher,
            '成交量萎缩': vol_shrink,
            '未突破 52 周高点': not_break_high,
            '52 周振幅(%)': round(amplitude * 100, 2),
            '30 周均线斜率(%)': round(ma30_slope * 100, 2),
        }

        print(result)
        return flag, result

    @classmethod
    def phase_two(cls, symbol):
        """
        [第二阶段（主升浪）特征的量化指标]
        1. 30 周均线持续上升（表明中长期趋势向上）
        2. 价格在 30 周均线之上（上升趋势健康）
        3. 价格在 10 周均线之上（短期趋势同步向上）
        4. 突破前期 52 周高点（创出新高或突破盘整）
        5. 成交量放大确认趋势（量价配合）
        6. 低点逐步抬高（上升趋势中的正常回调低点不断抬高）
        """
        ma_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'ma.parquet')
        week_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'week.parquet')

        if ma_data.height < 40:
            print(f'数据不足，需要至少 40 周数据')
            return False, {'错误': '数据不足，需要至少40周数据'}

        current_price = week_data.select(pl.col('收盘')).tail(1).item()
        current_ma30 = ma_data.select('MA30').tail(1).item()
        current_ma10 = week_data.select(pl.col('收盘').rolling_mean(10)).tail(1).item()

        # 1. 30 周均线上升（斜率 > 2%）
        # 上升的均线是第二阶段的核心特征，确认趋势从横盘转为向上
        recent_ma30 = ma_data.select('MA30').tail(10).to_series()
        ma30_slope = (recent_ma30[-1] - recent_ma30[0]) / recent_ma30[0]
        ma30_rising = ma30_slope > 0.02

        # 2. 价格在 30 周均线之上
        # 价格在均线上方表明上升趋势健康
        price_above_ma30 = current_price > current_ma30

        # 3. 价格在 10 周均线之上（短期趋势健康）
        # 10 周均线反映短期趋势，应与中长期趋势一致
        price_above_ma10 = current_price > current_ma10

        # 4. 突破前期 52 周高点
        # 突破前期高点是第二阶段启动的重要信号
        high_52w = week_data.select(pl.col('最高')).tail(52).max().item()
        high_52w_prev = week_data.select(pl.col('最高')).slice(week_data.height - 104, 52).max().item()
        break_out = high_52w > high_52w_prev * 1.05  # 突破前期52周高点5%以上

        # 5. 成交量放大（最近10周均量 > 52周均量 × 1.2）
        # 放量是趋势确认的必要条件，主升浪需要成交量配合
        vol_ma10 = week_data.select(pl.col('成交量').rolling_mean(10)).tail(1).item()
        vol_ma52 = week_data.select(pl.col('成交量').rolling_mean(52)).tail(1).item()
        vol_expanding = vol_ma10 > vol_ma52 * 1.2

        # 6. 近期低点抬高（最近10周低点 > 前10周低点）
        # 低点抬高是上升趋势的正常特征，表明回调底部不断抬升
        recent_low = week_data.select(pl.col('最低')).tail(10).min().item()
        prev_low = week_data.select(pl.col('最低')).slice(week_data.height - 20, 10).min().item()
        higher_low = recent_low > prev_low

        # 综合判断：五个条件全部满足才认定为第二阶段
        flag = all([ma30_rising, price_above_ma30, price_above_ma10, vol_expanding, higher_low])
        result = {
            '是否第二阶段': flag,
            '30 周均线上升': ma30_rising,
            '价格 > 30 周均线': price_above_ma30,
            '价格 > 10 周均线': price_above_ma10,
            '突破 52 周前期高点': break_out,
            '成交量放大': vol_expanding,
            '低点抬高': higher_low,
            '30 周均线斜率(%)': round(ma30_slope * 100, 2),
        }

        print(result)
        return flag, result

    @classmethod
    def phase_three(cls, symbol):
        """
        [第三阶段（顶部派发）特征的量化指标]
        1. 30 周均线走平或下降（表明上升动力减弱）
        2. 价格在 30 周均线上下反复（无法维持强势）
        3. 未突破 52 周新高（价格受阻）
        4. 近期振幅收窄（上涨乏力）
        5. 成交量在高位放大（主力派发嫌疑）
        6. 价格接近近期低位（重心下移）
        """
        ma_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'ma.parquet')
        week_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'week.parquet')

        if ma_data.height < 40:
            print(f'数据不足，需要至少 40 周数据')
            return False, {'错误': '数据不足，需要至少40周数据'}

        current_price = week_data.select(pl.col('收盘')).tail(1).item()
        current_ma30 = ma_data.select('MA30').tail(1).item()

        # 1. 30 周均线走平或下降（斜率 < 2%）
        # 走平的均线表明上涨动力减弱，可能进入派发阶段
        recent_ma30 = ma_data.select('MA30').tail(10).to_series()
        ma30_slope = (recent_ma30[-1] - recent_ma30[0]) / recent_ma30[0]
        ma30_flat_or_falling = ma30_slope < 0.02

        # 2. 价格在 30 周均线附近反复（±10%以内）
        # 价格围绕均线波动表明市场犹豫，无法维持强势
        price_near_ma30 = abs(current_price - current_ma30) / current_ma30 < 0.1

        # 3. 未突破 52 周新高（价格受阻）
        # 无法创新高是顶部派发的重要特征
        high_52w = week_data.select(pl.col('最高')).tail(52).max().item()
        not_break_new_high = current_price < high_52w * 0.95

        # 4. 近期振幅收窄（10周振幅 < 15%）
        # 振幅收窄表明上涨无力进入最后冲刺阶段
        recent_amplitude = week_data.select(pl.col('最高')).tail(10).max().item() / \
                           week_data.select(pl.col('最低')).tail(10).min().item() - 1
        amplitude_narrow = recent_amplitude < 0.15

        # 5. 成交量在高位放大（最近10周均量 > 52周均量 × 1.3）
        # 高位放量是主力派发的典型特征（量价背离）
        vol_ma10 = week_data.select(pl.col('成交量').rolling_mean(10)).tail(1).item()
        vol_ma52 = week_data.select(pl.col('成交量').rolling_mean(52)).tail(1).item()
        high_volume = vol_ma10 > vol_ma52 * 1.3

        # 6. 收盘价接近近期低位（价格重心下移）
        # 价格接近10周低点表明卖压沉重
        recent_low = week_data.select(pl.col('最低')).tail(10).min().item()
        price_near_low = current_price < recent_low * 1.05

        # 综合判断：四个核心条件满足才认定为第三阶段
        flag = all([ma30_flat_or_falling, not_break_new_high, amplitude_narrow, high_volume])
        result = {
            '是否第三阶段': flag,
            '30 周均线走平/下降': ma30_flat_or_falling,
            '价格在均线附近反复': price_near_ma30,
            '未突破新高': not_break_new_high,
            '振幅收窄': amplitude_narrow,
            '成交量高位放大': high_volume,
            '价格接近低位': price_near_low,
            '30 周均线斜率(%)': round(ma30_slope * 100, 2),
        }

        print(result)
        return flag, result

    @classmethod
    def phase_four(cls, symbol):
        """
        [第四阶段（下跌）特征的量化指标]
        1. 30 周均线持续下降（表明中长期趋势向下）
        2. 价格在 30 周均线之下（下跌趋势确认）
        3. 价格在 10 周均线之下（短期趋势同步向下）
        4. 新低不断（近期低点 < 前期低点）
        5. 从高点下跌超过 20%（确认进入熊市）
        """
        ma_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'ma.parquet')
        week_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'week.parquet')

        if ma_data.height < 40:
            print(f'数据不足，需要至少 40 周数据')
            return False, {'错误': '数据不足，需要至少40周数据'}

        current_price = week_data.select(pl.col('收盘')).tail(1).item()
        current_ma30 = ma_data.select('MA30').tail(1).item()
        current_ma10 = week_data.select(pl.col('收盘').rolling_mean(10)).tail(1).item()

        # 1. 30 周均线下降（斜率 < -2%）
        # 下降的均线是第四阶段的核心特征，确认趋势彻底转弱
        recent_ma30 = ma_data.select('MA30').tail(10).to_series()
        ma30_slope = (recent_ma30[-1] - recent_ma30[0]) / recent_ma30[0]
        ma30_falling = ma30_slope < -0.02

        # 2. 价格在 30 周均线之下
        # 价格在均线下方表明下跌趋势确立
        price_below_ma30 = current_price < current_ma30

        # 3. 价格在 10 周均线之下（短期趋势向下）
        # 短期均线也向下表明下跌趋势短期难以扭转
        price_below_ma10 = current_price < current_ma10

        # 4. 新低不断（近期低点 < 前期低点）
        # 不断创新低是熊市的典型特征
        recent_low = week_data.select(pl.col('最低')).tail(10).min().item()
        prev_low = week_data.select(pl.col('最低')).slice(week_data.height - 40, 10).min().item()
        new_low = recent_low < prev_low

        # 5. 从高点下跌超过 20%
        # 从52周高点下跌超过20%确认进入熊市
        high_52w = week_data.select(pl.col('最高')).tail(52).max().item()
        drawdown = (current_price - high_52w) / high_52w
        significant_drop = drawdown < -0.2

        # 综合判断：五个条件全部满足才认定为第四阶段
        flag = all([ma30_falling, price_below_ma30, price_below_ma10, new_low, significant_drop])
        result = {
            '是否第四阶段': flag,
            '30 周均线下降': ma30_falling,
            '价格 < 30 周均线': price_below_ma30,
            '价格 < 10 周均线': price_below_ma10,
            '新低不断': new_low,
            '从高点下跌超过20%': significant_drop,
            '从高点下跌(%)': round(drawdown * 100, 2),
            '30 周均线斜率(%)': round(ma30_slope * 100, 2),
        }

        print(result)
        return flag, result


class ShortLine:

    pass



if __name__ == '__main__':
    Trend.run(symbol='600036')

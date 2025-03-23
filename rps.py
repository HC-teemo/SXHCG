from hikyuu.indicator import Indicator, IndicatorImp
import pandas as pd
import numpy as np
from datetime import datetime
import h5py
def read_rps_to_dataframe(h5_file, rps_period=10):
    
    print(f"读取 {h5_file} 中的RPS{rps_period}数据...")
    
    # 用于存储所有日期的数据
    all_dates = []
    all_stocks = set()
    date_rps_dict = {}
    
    # 打开HDF5文件
    with h5py.File(h5_file, 'r') as f:
        # 获取所有日期
        dates = list(f.keys())
        dates.sort()  # 确保日期有序
        
        # 遍历每个日期
        for date in tqdm(dates, desc=f"读取RPS{rps_period}数据"):
            date_group = f[date]
            rps_key = f'RPS{rps_period}'
            
            # 检查该日期是否有对应的RPS数据
            if rps_key in date_group:
                rps_dataset = date_group[rps_key]
                
                # 提取该日期的RPS数据
                date_data = {}
                for code, rps in rps_dataset:
                    code_str = code.decode('utf-8') if isinstance(code, bytes) else code
                    rps_float = float(rps.decode('utf-8')) if isinstance(rps, bytes) else float(rps)
                    date_data[code_str] = rps_float
                    all_stocks.add(code_str)
                
                # 保存该日期的数据
                all_dates.append(date)
                date_rps_dict[date] = date_data
    
    print(f"共读取 {len(all_dates)} 个交易日的RPS{rps_period}数据")
    print(f"共涉及 {len(all_stocks)} 只股票")
    
    # 将所有股票代码转换为有序列表
    stock_codes = sorted(list(all_stocks))
    
    # 创建DataFrame
    df_data = []
    
    for date in all_dates:
        row = [date]  # 第一列为日期
        date_data = date_rps_dict[date]
        
        # 添加每只股票的RPS值，如果没有则为NaN
        for code in stock_codes:
            row.append(date_data.get(code, np.nan))
        
        df_data.append(row)
    
    # 创建列名
    columns = ['date'] + stock_codes
    
    # 构建DataFrame
    df = pd.DataFrame(df_data, columns=columns)
    
    # 将日期列转换为datetime类型
    df['date'] = pd.to_datetime(df['date'])
    
    # 设置日期列为索引
    # df = df.set_index('date')
    
    return df

# rpsdf = read_rps_to_dataframe()

def rpswrap_init(self, name, params, result_num=1, prices=None):
    super(self.__class__, self).__init__(name, result_num)
    for k, v in params.items():
        self.set_param(k, v)
    self._prices = prices
    self._params = params
    self._result_num = result_num

def rpswrap_calculate(self, ind):
    result_num = self.get_result_num()
    print(result_num)
    if result_num < 1:
        print("error: result_num must be >= 1!")
        return

    if not self._prices:
        if self.name == "PYTA_OBV":
            if ind.get_result_num() < 2:
                print("error: result_num must be >= 2!")
                return
            inputs = {'close': ind.get_result(0).to_np(), 'volume': ind.get_result(1).to_np()}
        elif self.name in ("PYTA_BETA", "PYTA_CORREL"):
            if ind.get_result_num() < 2:
                print("error: result_num must be >= 2!")
                return
            inputs = {'high': ind.get_result(0).to_np(), 'low': ind.get_result(1).to_np()}
        else:
            inputs = {'close': ind.to_np()}
    else:
        if ind.name != 'KDATA':
            print("error: ind must KDATA")
            return

        inputs = {
            'open': ind.get_result(0).to_np(),
            'high': ind.get_result(1).to_np(),
            'low': ind.get_result(2).to_np(),
            'close': ind.get_result(3).to_np(),
            'volume': ind.get_result(5).to_np()
        }

    params = self.get_parameter()
    param_names = params.get_name_list()
    func_params = {}
    for name in param_names:
        if name != "kdata":
            func_params[name] = self.get_param(name)

    

    outputs = inputs['close']
    
    if result_num == 1:
        for i, val in enumerate(outputs):
            if not np.isnan(val):
                self._set(float(val), i)

    else:
        for i, out in enumerate(outputs):
            for j, val in enumerate(out):
                if not np.isnan(val):
                    self._set(float(val), j, i)

def check_all_true(self):
    return True

def rpswrap_support_ind_param(self):
    return False

def rpswrap_clone(self):
    return crtRpsIndicatorImp(
        self.name, self._params, self._result_num, self._prices, check=self.check
    )

def crtRpsIndicatorImp(name, params={}, result_num=1, prices=None, check=check_all_true):
    meta_x = type(
        name, (IndicatorImp, ), {
            '__init__': rpswrap_init,
            'check': check,
            '_clone': rpswrap_clone,
            '_calculate': rpswrap_calculate,
            'support_ind_param': rpswrap_support_ind_param,
        }
    )
    return meta_x(name, params, result_num, prices)

def RPS10(ind=None):
    imp = crtRpsIndicatorImp('RPS10', params={'timeperiod': 10})
    return Indicator(imp)(ind) if ind else Indicator(imp)

# def PYTA_AD(ind=None):
#     imp = crtRpsIndicatorImp(ta.AD, 'PYTA_AD', prices=['high', 'low', 'close', 'volume'])
#     return Indicator(imp)(ind) if ind else Indicator(imp)


# def PYTA_ADOSC(ind=None, fastperiod=3, slowperiod=10):
#     imp = crtRpsIndicatorImp(
#         ta.ADOSC,
#         'PYTA_ADOSC',
#         params={
#             'fastperiod': fastperiod,
#             'slowperiod': slowperiod
#         },
#         prices=['high', 'low', 'close', 'volume']
#     )
#     return Indicator(imp)(ind) if ind else Indicator(imp)

# PYTA_ADOSC.__doc__ = talib.ADOSC.__doc__

# def PYTA_ADX(ind=None, timeperiod=14):
#     imp = crtRpsIndicatorImp(ta.ADX, 'PYTA_ADX', params={'timeperiod': timeperiod}, prices=['high', 'low', 'close'])
#     return Indicator(imp)(ind) if ind else Indicator(imp)

def read_rps_to_dataframe(h5_file, rps_period=10):
    
    print(f"读取 {h5_file} 中的RPS{rps_period}数据...")
    
    # 用于存储所有日期的数据
    all_dates = []
    all_stocks = set()
    date_rps_dict = {}
    
    # 打开HDF5文件
    with h5py.File(h5_file, 'r') as f:
        # 获取所有日期
        dates = list(f.keys())
        dates.sort()  # 确保日期有序
        
        # 遍历每个日期
        for date in tqdm(dates, desc=f"读取RPS{rps_period}数据"):
            date_group = f[date]
            rps_key = f'RPS{rps_period}'
            
            # 检查该日期是否有对应的RPS数据
            if rps_key in date_group:
                rps_dataset = date_group[rps_key]
                
                # 提取该日期的RPS数据
                date_data = {}
                for code, rps in rps_dataset:
                    code_str = code.decode('utf-8') if isinstance(code, bytes) else code
                    rps_float = float(rps.decode('utf-8')) if isinstance(rps, bytes) else float(rps)
                    date_data[code_str] = rps_float
                    all_stocks.add(code_str)
                
                # 保存该日期的数据
                all_dates.append(date)
                date_rps_dict[date] = date_data
    
    print(f"共读取 {len(all_dates)} 个交易日的RPS{rps_period}数据")
    print(f"共涉及 {len(all_stocks)} 只股票")
    
    # 将所有股票代码转换为有序列表
    stock_codes = sorted(list(all_stocks))
    
    # 创建DataFrame
    df_data = []
    
    for date in all_dates:
        row = [date]  # 第一列为日期
        date_data = date_rps_dict[date]
        
        # 添加每只股票的RPS值，如果没有则为NaN
        for code in stock_codes:
            row.append(date_data.get(code, np.nan))
        
        df_data.append(row)
    
    # 创建列名
    columns = ['date'] + stock_codes
    
    # 构建DataFrame
    df = pd.DataFrame(df_data, columns=columns)
    
    # 将日期列转换为datetime类型
    df['date'] = pd.to_datetime(df['date'])
    
    # 设置日期列为索引
    # df = df.set_index('date')
    
    return df



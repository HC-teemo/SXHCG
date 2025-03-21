import pandas as pd
import numpy as np
from hikyuu.interactive import *

def simple_rps_selector(market='SH', count=5, rps_file=None):
    """
    基于RPS排序的简单选股策略
    
    参数:
        market: 市场，如'SH'、'SZ'等，或者'ALL'表示所有市场
        count: 选择的股票数量
        rps_file: RPS数据文件路径，CSV格式，需要包含code、RPS50、RPS120、RPS250列
                 如果为None，则尝试使用示例数据
    
    返回:
        选股结果列表
    """
    # 1. 加载RPS数据
    if rps_file is None:
        # 如果没有提供RPS文件，创建一些示例数据用于演示
        print("未提供RPS数据文件，使用示例数据...")
        # 获取股票列表
        if market == 'ALL':
            stocks = blocka
        else:
            stocks = get_stock_list(market)
        
        # 创建示例RPS数据
        codes = [stock.code for stock in stocks[:100]]  # 仅使用前100只股票作为示例
        np.random.seed(42)  # 设置随机种子，确保结果可重现
        
        rps_data = pd.DataFrame({
            'code': codes,
            'RPS50': np.random.randint(1, 100, size=len(codes)),
            'RPS120': np.random.randint(1, 100, size=len(codes)),
            'RPS250': np.random.randint(1, 100, size=len(codes))
        })
    else:
        # 从文件加载RPS数据
        rps_data = pd.read_csv(rps_file)
    
    # 2. 计算RPS总和并排序
    rps_data['RPS_SUM'] = rps_data['RPS50'] + rps_data['RPS120'] + rps_data['RPS250']
    rps_data = rps_data.sort_values(by='RPS_SUM', ascending=False)
    
    # 3. 选择前count个股票
    top_stocks = rps_data.head(count)
    
    # 4. 获取股票详细信息
    result = []
    for _, row in top_stocks.iterrows():
        code = row['code']
        try:
            stock = get_stock(code)
            # 获取最新的K线数据
            kdata = stock.get_kdata(Query(-1))
            if len(kdata) > 0:
                last_close = kdata[-1].close
            else:
                last_close = None
                
            result.append({
                'code': code,
                'name': stock.name,
                'RPS50': row['RPS50'],
                'RPS120': row['RPS120'],
                'RPS250': row['RPS250'],
                'RPS_SUM': row['RPS_SUM'],
                'close': last_close
            })
        except Exception as e:
            print(f"处理股票 {code} 时出错: {e}")
    
    return result

# 如何使用RPS数据计算函数（实际应用中需要根据您的数据来源调整）
def calculate_rps(market='ALL', n_days=[50, 120, 250]):
    """
    计算股票的RPS值
    
    参数:
        market: 市场，如'SH'、'SZ'等，或者'ALL'表示所有市场
        n_days: 计算RPS的周期列表，如[50, 120, 250]
    
    返回:
        包含RPS值的DataFrame
    """
    # 获取股票列表
    if market == 'ALL':
        stocks = get_stock_list()
    else:
        stocks = get_stock_list(market)
    
    # 存储结果
    result_data = []
    
    # 遍历每只股票
    for stock in stocks:
        try:
            code = stock.code
            # 获取足够长的K线数据
            max_days = max(n_days) + 20  # 多获取一些数据以确保足够
            kdata = stock.get_kdata(Query(-max_days))
            
            if len(kdata) < max(n_days):
                continue  # 数据不足，跳过
            
            # 计算涨幅
            closes = np.array([k.close for k in kdata])
            
            # 计算各周期的RPS
            rps_values = {}
            for n in n_days:
                if len(closes) >= n:
                    # 计算n日涨幅
                    change_rate = (closes[-1] / closes[-n] - 1) * 100
                    rps_values[f'RPS{n}'] = change_rate
            
            # 添加到结果中
            if len(rps_values) == len(n_days):  # 确保所有周期都有数据
                result_data.append({
                    'code': code,
                    'name': stock.name,
                    **rps_values
                })
        
        except Exception as e:
            print(f"计算股票 {stock.code} 的RPS时出错: {e}")
    
    # 转换为DataFrame
    df = pd.DataFrame(result_data)
    
    # 计算排名并转换为RPS值
    for n in n_days:
        col = f'RPS{n}'
        df[col] = df[col].rank(pct=True) * 100
    
    return df

# 示例使用
if __name__ == "__main__":
    # 初始化Hikyuu
    import hikyuu as hku
    hku.init()
    
    # 方法1：使用预先计算好的RPS数据文件
    # result = simple_rps_selector(market='ALL', count=5, rps_file='rps_data.csv')
    
    # 方法2：先计算RPS，再进行选股
    print("计算RPS数据...")
    rps_df = calculate_rps(market='SH')
    
    # 保存RPS数据（可选）
    rps_df.to_csv('rps_data.csv', index=False)
    
    # 使用计算好的RPS数据进行选股
    print("基于RPS数据选股...")
    result = simple_rps_selector(market='SH', count=5, rps_file='rps_data.csv')
    
    # 显示结果
    print("\n选出的前5只股票:")
    for stock in result:
        print(f"{stock['code']} {stock['name']}: " +
              f"RPS50={stock['RPS50']:.2f}, RPS120={stock['RPS120']:.2f}, " +
              f"RPS250={stock['RPS250']:.2f}, 总和={stock['RPS_SUM']:.2f}, " +
              f"收盘价={stock['close']}")

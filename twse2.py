import time
import requests
import pandas as pd


headers = {
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,ko;q=0.6',
  'Cache-Control': 'no-cache',
  'Connection': 'keep-alive',
  'Pragma': 'no-cache',
  'Referer': 'https://www.twse.com.tw/en/trading/foreign/t86.html',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
  'X-Requested-With': 'XMLHttpRequest',
  'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'Cookie': 'JSESSIONID=D1D5603C29B77DA3619B17DE3D46CA88'
}
rsts = []
rsts1 = []


def fetch(*args, **kwargs):
    count = 0
    while count < 10:
        try:
            res = requests.request('GET', *args, **kwargs).json()
            return res
        except:
            count += 1
            # time.sleep(1)
    else:
        print(kwargs)
        print(f'已达到重试次数')


def get_trade(dt):
    tm = int(time.time() * 1000)
    url = f"https://www.twse.com.tw/rwd/en/marginTrading/TWT93U?date={dt}&response=json&_={tm}"
    res = fetch(url, headers=headers, timeout=60)
    if not res:
        print(f'{dt}获取失败')
        return
    ls = res.get('data', [])
    if not ls:
        print(f'{dt}没有数据')
        return
    for data in ls:
        try:
            code = data[0]
            pdb = data[1]
            ss = data[2]
            sc = data[3]
            sr = data[4]
            cdb = data[5]
            qnd = data[6]
            pdb1 = data[7]
            cdss = data[8]
            cdr = data[9]
            cda = data[10]
            cdb1 = data[11]
            qnd1 = data[12]
            note = data[13].strip()
        except:
            continue
        dic = {
            'Security Code': code,
            'Previous Day Balance': pdb,
            'Short Sales': ss,
            'Short Covering': sc,
            'Stock Redemption': sr,
            'Current Day Balance': cdb,
            'Quota for the Next Day': qnd,
            'Previous Day Balance 1': pdb1,
            'Current Day Short Sales': cdss,
            'Current Day Returns': cdr,
            'Current Day Adjustments': cda,
            'Current Day Balance 1': cdb1,
            'Quota for the Next Day 1': qnd1,
            'Note': note,
            'Date': f'{dt[:4]}-{dt[4:6]}-{dt[6:]}'
        }
        print(dic)
        rsts.append(dic)


def get_sbl(dt):
    tm = int(time.time() * 1000)
    url = f"https://www.twse.com.tw/rwd/en/lending/TWT72U?date={dt}&selectType=SLBNLB&response=json&_={tm}"
    res = fetch(url, headers=headers, timeout=60)
    if not res:
        print(f'{dt}获取失败')
        return
    ls = res.get('data', [])
    if not ls:
        print(f'{dt}没有数据')
        return
    for data in ls:
        try:
            code = data[0]
            pdsbs = data[1]
            cdasb = data[2]
            cdasr = data[3]
            cdsbs1 = data[4]
            cdcp = data[5]
            smvb = data[6]
            mid = data[7]
        except Exception as e:
            print(e)
            continue
        dic = {
            'Stock code': code,
            'Previous Day SBL Balance (1) shares': pdsbs,
            'Current Day Activities shares Borrowing(2)': cdasb,
            'Current Day Activities shares Return(3)': cdasr,
            'Current Day SBL Balance (4)=(1)+(2)-(3) shares': cdsbs1,
            'Current Day Closing Price (5)NT': cdcp,
            'SBL Market Value Balance (6)=(4)*(5)NT': smvb,
            'Market ID': mid,
            'Date': f'{dt[:4]}-{dt[4:6]}-{dt[6:]}'
        }
        print(dic)
        rsts1.append(dic)


def get_range_trade(st, et):
    '''
        https://www.twse.com.tw/en/trading/margin/twt93u.html
    '''
    tls = pd.date_range(st, et, freq='1D')
    for tl in tls:
        dt = str(tl).split()[0].replace('-', '')
        get_trade(dt)
    tm = int(time.time())
    df = pd.DataFrame(rsts)
    df.to_csv(f'trade_{tm}.csv', index=False)


def get_range_sbl(st, et):
    '''
        https://www.twse.com.tw/en/products/sbl/disclosures/twt72u.html
    '''
    tls = pd.date_range(st, et, freq='1D')
    for tl in tls:
        dt = str(tl).split()[0].replace('-', '')
        get_sbl(dt)
    tm = int(time.time())
    df = pd.DataFrame(rsts1)
    df.to_csv(f'sbl_{tm}.csv', index=False)


def main():
    # get_range_trade('2024-09-01', '2024-09-30')
    get_range_sbl('2024-09-01', '2024-09-30')


if __name__ == '__main__':
    main()

import requests
import time
import pandas as pd

rsts = []

headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,ko;q=0.6',
    # 'Cache-Control': 'no-cache',
    # 'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': '__smVisitorID=VNlq8826Cbm; lang=en; JSESSIONID=QzuaZpQqzPEpD4Aw3SXrQaeM683cLepA2lh54mKnozwZyJTqxbFguqE7RWt0dPSH.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAwMQ==',
    'Origin': 'http://data.krx.co.kr',
    # 'Pragma': 'no-cache',
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0203',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    # 'X-Requested-With': 'XMLHttpRequest',
}


def fetch(*args, **kwargs):
    count = 0
    while count < 5:
        try:
            res = requests.request('POST', *args, **kwargs).json()
            return res
        except:
            count += 1
            time.sleep(1)
    else:
        print(kwargs)
        print(f'已达到重试次数')


def get_data(dt):
    sdt = dt.replace('-', '')
    data = {
        'bld': 'dbms/MDC/STAT/srt/MDCSTAT30501',
        'locale': 'en',
        'searchType': 1,
        'mktTpCd': 1,   # 为 2 则为kqsdaq
        'trdDd': sdt,
        'tboxisuCd_finder_srtisu0_0': '',
        'isuCd': '',
        'isuCd2': '',
        'codeNmisuCd_finder_srtisu0_0': '',
        'param1isuCd_finder_srtisu0_0': '',
        # 'strtDd': 20230419,
        # 'endDd': 20230519
        'share': 1,
        'money': 1,
        'csvxls_isNo': 'false'
    }
    res = fetch('http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd', headers=headers, data=data, verify=False)
    if not res:
        print('-------------------------')
        return
    dl = res.get('OutBlock_1', [])
    for data in dl:
        issue_code = data.get('ISU_CD')
        issue_name = data.get('ISU_ABBRV')
        quantity_short_position = data.get('BAL_QTY')
        listed_shares = data.get('LIST_SHRS')
        amount_short_position = data.get('BAL_AMT')
        market_cap = data.get('MKTCAP')
        proportion = data.get('BAL_RTO')
        dic = {
            'Date': dt,
            'Issue code': issue_code,
            'Issue name': issue_name,
            'Quantity Net short position': quantity_short_position,
            'Quantity No.of listed shares': listed_shares,
            'Amount Net short position': amount_short_position,
            'Amount Market cap': market_cap,
            'Proportion': proportion,
        }
        print(dic)
        rsts.append(dic)


def main():
    dates = pd.date_range('2021-05-01', '2023-05-20')
    for dte in dates:
        dt = str(dte).split()[0]
        get_data(dt)
    tm = int(time.time())
    df = pd.DataFrame(rsts)
    df.to_csv(f'krx_{tm}_kospi.csv', index=False)

    # df.to_csv(f'krx_{tm}_kosdaq.csv', index=False)




if __name__ == '__main__':
    main()

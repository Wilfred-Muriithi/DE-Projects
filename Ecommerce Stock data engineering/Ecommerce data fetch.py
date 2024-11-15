import pandas as pd
import yfinance as yf

companies = ['MELI', 'AMZN', 'EBAY', 'BABA', 'SHOP','JD']
data = {}


for company in companies:
    ticker = yf.Ticker(company)
    data[company] = {
        'revenue_growth': ticker.info['revenueGrowth'],
        'gross_margin': ticker.info['grossMargins'],
        'operating_margin': ticker.info['operatingMargins'],
        'eps': ticker.info['trailingEps'],
        'pe_ratio': ticker.info['trailingPE'],
        'free_cash_flow': ticker.info['freeCashflow'],
        'de_ratio': ticker.info['debtToEquity'],
        'roe': ticker.info['returnOnEquity'],

        'stock_price': ticker.history(period="1d")['Close'][0],
        'volume': ticker.history(period="1d")['Close'][0]
    }

    df = pd.DataFrame(data).T
    print(df)

    df.to_csv(r'C:\Users\KIHARA\OneDrive\Documents\PROJECTS\Data Engineering\DE Projects\DE-Projects\Ecommerce Stock data engineering\ecommerce_data.csv', index=True)


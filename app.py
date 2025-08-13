import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime
import time

# Function to calculate days to expiration
def days_to_expiration(exp_date_str):
    exp_date = datetime.strptime(exp_date_str, '%Y-%m-%d')
    today = datetime.today()
    return max((exp_date - today).days, 1)  # Avoid division by zero

st.title("Overpriced Puts Finder App")

st.markdown("""
This app scans S&P 500 stocks for put options where the bid price as a percentage of the strike price meets or exceeds your desired threshold.
This is calculated as (bid / strike) * 100%.

**Note:** A 10% raw return is quite high and may only appear in high-volatility or unusual market conditions. Results may be limited.
You can also opt to filter by annualized return, which adjusts for the time to expiration: ((bid / strike) * (365 / days_to_exp)) * 100%.
""")

# User inputs
desired_percent = st.number_input("Enter desired bid/strike % (e.g., 10 for 10%)", min_value=0.0, value=10.0)
annualize = st.checkbox("Use annualized return instead?", value=False)

if st.button("Scan S&P 500 Stocks"):
    # Fetch S&P 500 tickers
    try:
        sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        sp500 = pd.read_html(sp500_url)[0]['Symbol'].tolist()
        st.write(f"Scanning {len(sp500)} S&P 500 stocks...")
    except Exception as e:
        st.error(f"Error fetching S&P 500 list: {e}")
        st.stop()

    results = []
    progress_bar = st.progress(0)
    total_stocks = len(sp500)

    for i, ticker in enumerate(sp500):
        try:
            stock = yf.Ticker(ticker)
            option_dates = stock.options
            for exp in option_dates:
                chain = stock.option_chain(exp)
                puts = chain.puts
                puts = puts[puts['bid'] > 0]  # Only consider puts with positive bids
                if annualize:
                    puts['days_to_exp'] = days_to_expiration(exp)
                    puts['percent'] = (puts['bid'] / puts['strike']) * (365 / puts['days_to_exp']) * 100
                else:
                    puts['percent'] = (puts['bid'] / puts['strike']) * 100
                puts['expiration'] = exp
                puts['ticker'] = ticker
                filtered = puts[puts['percent'] >= desired_percent]
                if not filtered.empty:
                    results.append(filtered[['ticker', 'expiration', 'strike', 'bid', 'percent', 'lastPrice', 'volume', 'openInterest', 'impliedVolatility']])
        except Exception:
            pass  # Skip errors for individual stocks
        
        # Update progress
        progress_bar.progress((i + 1) / total_stocks)
        time.sleep(0.01)  # Small delay to avoid API rate limits

    if results:
        all_results = pd.concat(results)
        all_results['impliedVolatility'] = all_results['impliedVolatility'] * 100  # Convert to %
        all_results.sort_values(by='percent', ascending=False, inplace=True)
        st.success(f"Found {len(all_results)} put options matching your criteria.")
        st.dataframe(all_results.style.format({
            'percent': '{:.2f}%',
            'impliedVolatility': '{:.2f}%',
            'bid': '{:.2f}',
            'lastPrice': '{:.2f}',
            'strike': '{:.2f}'
        }))
    else:
        st.warning("No put options found matching your criteria. Try a lower percentage or enable/disable annualization.")
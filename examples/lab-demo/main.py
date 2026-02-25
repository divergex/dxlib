import dxlib as dx
# 1. get data
# 2. define strategy
# 3. run backtest
# 4. analyze
# 5. store

from data import cached_data

def main():
    history, portfolio = cached_data()
    print(history)
    print(portfolio)


if __name__ == "__main__":
    main()

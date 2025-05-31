import numpy as np
import pandas as pd
import networkx as nx


def build_rate_matrix(quotes: pd.DataFrame) -> pd.DataFrame:
    currencies = set()
    data = {}

    for pair in quotes.index:
        base, quote = pair.split("/")
        currencies.update([base, quote])
        bid = quotes.loc[pair, "bid"]
        ask = quotes.loc[pair, "ask"]
        data[(base, quote)] = bid
        data[(quote, base)] = 1 / ask if ask > 0 else np.nan

    currencies = sorted(currencies)
    rate_matrix = pd.DataFrame(np.nan, index=currencies, columns=currencies)

    for (i, j), rate in data.items():
        rate_matrix.loc[i, j] = rate

    return rate_matrix


def find_arbitrage_paths(rate_matrix: pd.DataFrame, threshold=1.0001):
    currencies = rate_matrix.index
    G = nx.DiGraph()

    for i in currencies:
        for j in currencies:
            if pd.notna(rate_matrix.loc[i, j]) and rate_matrix.loc[i, j] > 0:
                weight = -np.log(rate_matrix.loc[i, j])
                G.add_edge(i, j, weight=weight, rate=rate_matrix.loc[i, j])

    for start in G.nodes:
        try:
            nx.single_source_bellman_ford(G, start)
        except nx.NetworkXUnbounded:
            cycle = find_negative_cycle(G, start)
            if cycle:
                rate_product = np.prod([
                    G[cycle[i]][cycle[i+1]]['rate'] for i in range(len(cycle)-1)
                ])
                return {
                    "cycle": cycle,
                    "rate_product": rate_product,
                    "position": {f"{cycle[i]}/{cycle[i+1]}": +1 for i in range(len(cycle)-1)}
                }
    return {"cycle": None, "rate_product": None, "position": {}}


def find_negative_cycle(G, start):
    stack = [(start, [start], 0)]

    while stack:
        node, path, log_sum = stack.pop()

        if len(path) > 1 and path[-1] == path[0] and log_sum < 0:
            return path

        for neighbor in G.successors(node):
            if neighbor in path and neighbor != path[0]:
                continue  # don't revisit non-start nodes
            weight = G[node][neighbor]['weight']
            stack.append((neighbor, path + [neighbor], log_sum + weight))

    return None

def generalized_arbitrage_signal(quotes: pd.DataFrame) -> dict:
    """
    Main entrypoint. Receives quotes (bid/ask) for instruments like EUR/USD.
    Returns arbitrage signal with path and trade suggestion.
    """
    rate_matrix = build_rate_matrix(quotes)
    result = find_arbitrage_paths(rate_matrix)
    return result


def test_no_arbitrage():
    data = {
        "bid": [1.1, 0.9, 1.0],
        "ask": [1.2, 1.0, 1.1]
    }
    index = ["USD/EUR", "EUR/GBP", "GBP/USD"]
    quotes = pd.DataFrame(data, index=index)

    result = generalized_arbitrage_signal(quotes)

    # No arbitrage cycle expected
    assert result["cycle"] is None
    assert result["rate_product"] is None
    assert result["position"] == {}


def test_with_arbitrage():
    data = {
        "bid": [1.2, 0.9, 1.1],
        "ask": [1.3, 1.0, 1.15]
    }
    index = ["USD/EUR", "EUR/GBP", "GBP/USD"]
    quotes = pd.DataFrame(data, index=index)

    result = generalized_arbitrage_signal(quotes)

    assert result["cycle"] is not None
    assert result["rate_product"] is not None
    assert result["rate_product"] > 1.0001
    assert isinstance(result["position"], dict)
    assert result["cycle"][0] == result["cycle"][-1]
    return result


if __name__ == "__main__":
    test_no_arbitrage()
    print(test_with_arbitrage())

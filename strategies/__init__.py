from strategies.ma_crossover import MACrossover
from strategies.grid import GridTrading
from strategies.pairs import PairsTrading

REGISTRY = {
    "ma_crossover": MACrossover,
    "grid": GridTrading,
    "pairs": PairsTrading,
}

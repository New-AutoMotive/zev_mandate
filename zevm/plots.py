"""Chart helpers."""
from pathlib import Path

import matplotlib.pyplot as plt

STYLE = Path(__file__).parent / "style" / "newautomotive.mplstyle"


def use_style() -> None:
    """Apply the New AutoMotive matplotlib style."""
    plt.style.use(STYLE)

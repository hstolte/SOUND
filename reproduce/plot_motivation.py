import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from plot import save_fig, LEGEND_LABEL_SIZE, LEGEND_TITLE_SIZE
from plot_checkresult import grid_params, tick_params, label_size

# Plot visual config
plt.style.use("ggplot")
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
matplotlib.rcParams["hatch.linewidth"] = 0.2
matplotlib.rcParams["xtick.labelsize"] = 10
sns.set_palette(sns.color_palette("Set2", n_colors=14, desat=0.9))
sns.set_style("ticks")

if __name__ == "__main__":
    # Plotting
    n_rows = 3
    fig, axes = plt.subplots(
        n_rows,
        1,
        sharex=True,
        figsize=(6.4, (4.8 / 3) * n_rows),
        gridspec_kw={'height_ratios': [2, 1.5, 1.5]}
    )

    t = np.array([1, 2, 4, 5.6, 6, 6.4,  8.33], dtype=float)
    v = np.array([0.5, 0.635, 1.2, 1.8, 0.8, 0.75, 1.15], dtype=float)
    u_up = np.array([1, .66, 0.8, 1.5, 3.95, 3.4, 3.5-0.15], dtype=float)
    u_down = np.array([1, .66, 3.1, .8, 0.65, .6, 0.3+3.5], dtype=float)

    for ax in axes.flat:
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.grid(
            **{
                "color": "#4f8c9d",
                "alpha": 0.8,
                "linestyle": ":",
                "linewidth": 0.5,
            }
        )
        ax.tick_params(
            direction="in",
            which="both",
            right=True,
            top=True,
            labelsize=label_size,
        )

        ax.set_yticks([])



    for ax, annotation in zip(axes.flat, [
        "Data series",
        "Naive evaluation",
        "SOUND",]):

        ax.annotate(
            annotation,
            xy=(0.975, 0.92),
            xycoords="axes fraction",
            fontsize=13,
            horizontalalignment="right",
            verticalalignment="top",
            bbox=dict(
                boxstyle="round,pad=0.3",
                edgecolor="white",
                facecolor="white",
                alpha=0.66,
            ),
            color="black",
        )

    ax = axes.flat[0]

    ax.axhline(y=1, color="grey", linestyle="--")

    ax.set_ylim(-0.8, 3.5)
    ax.set_xlim(0,9.5)

    axes.flat[-1].set_xlabel("Time")

    t -= 0.25
    u_up *= 0.4
    u_down *= 0.4

    # Plot markers and error bars
    ax.errorbar(
        t,
        v,
        yerr=(u_down, u_up),
        fmt='o',  # 'o' for circular markers
        ecolor='black',
        mec='black',  # Marker edge color
        mfc='black',  # Marker face color
        alpha=0.75,
        capsize=3
    )

    fig.tight_layout()
    plt.subplots_adjust(hspace=0)
    # plt.show()
    fig.savefig(
        "motivation.svg",
        pad_inches=0.1,
        bbox_inches="tight",
    )
    # pass
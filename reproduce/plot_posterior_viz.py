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
    # Define the alpha, beta parameters
    alpha = 1
    beta = 1

    # Values for (m, n) such that m + n is constant and small (let's say 10)
    # Case 1: No probability mass is above 0.5
    m1, n1 = 1, 9
    # Case 2: Half of the probability mass is above 0.5
    m2, n2 = 6, 4
    # Case 3: Most probability mass is above 0.5 (two sigma)
    m3, n3 = 10, 0

    # Generate x values
    x = np.linspace(0, 1, 1000)

    # Compute posterior distributions
    pdf1 = stats.beta.pdf(x, alpha + m1, beta + n1)
    pdf2 = stats.beta.pdf(x, alpha + m2, beta + n2)
    pdf3 = stats.beta.pdf(x, alpha + m3, beta + n3)

    # Plotting
    fig = plt.figure(figsize=(6.4, (4.8 / 3) * 2))

    # Define the m, n values and PDFs in lists
    m_values = [m1, m2, m3]
    n_values = [n1, n2, n3]
    pdfs = [pdf1, pdf2, pdf3]

    linestyles = ["-", "-.", ":"]

    names = ["A", "B", "C"]

    # Iterate through the cases
    for i in range(3):
        m = m_values[i]
        n = n_values[i]
        pdf = pdfs[i]

        (line,) = plt.plot(
            x,
            pdf,
            label="(" + names[i] + f") $\\mathrm{{Beta}}(\\alpha + {m}, \\beta + {n})$",
            linestyle=linestyles[i],
        )
        line_color = line.get_color()

        # Shading the area under the curve > 0.5
        plt.fill_between(x, pdf, where=(x > 0.5), color=line_color, alpha=0.5)

        prob = 1 - stats.beta.cdf(0.5, alpha + m, beta + n)
        print(prob)
        if i == 0:  # green
            plt.annotate(
                r"$P(x>0.5) = \sim\!{:.4f}$".format(prob),
                (0.365, 4.8),  # pdf[np.argmin(np.abs(x - 0.5))]
                textcoords="offset points",
                xytext=(-60, 0),
                ha="center",
                fontsize=LEGEND_LABEL_SIZE,
                color=line_color,
            )
        elif i == 1:  # orange
            plt.annotate(
                r"$P(x>0.5) = \sim\!{:.4f}$".format(prob),
                (0.619, 2.6),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=LEGEND_LABEL_SIZE,
                color=line_color,
                bbox=dict(
                    facecolor="white", alpha=0.6, edgecolor="none", boxstyle="round4"
                ),
            )
        else:  # blue
            plt.annotate(
                r"$P(x>0.5) = \sim\!{:.4f}$".format(prob),
                (0.601, 7.5),
                textcoords="offset points",
                xytext=(60, 0),
                ha="center",
                fontsize=LEGEND_LABEL_SIZE,
                color=line_color,
            )

    plt.annotate(
        "Decision Threshold",
        xy=(0.51, 1 - 0.12 / 2),
        xycoords="axes fraction",
        xytext=(4, 0),
        textcoords="offset points",
        rotation=0,
        verticalalignment="top",
        horizontalalignment="left",
        fontsize=LEGEND_LABEL_SIZE,
        color="grey",
    )

    # plt.grid(**grid_params)
    plt.tick_params(
        direction="in",
        which="both",
        right=True,
        top=False,
        labelsize=label_size,
    )

    plt.xlim(0, 1)
    current_ylim = plt.ylim()  # Get current limits
    plt.ylim(0, current_ylim[1])  # Set only the lower limit to 0

    plt.axvline(
        x=0.5,
        linestyle="--",
        color="gray",
    )
    plt.xlabel("x")
    plt.ylabel("Posterior Distribution")
    plt.legend()
    plt.yticks([])
    plt.ylabel("")
    # plt.title("Posterior Distributions with Different Cases")
    # plt.show()
    fig.savefig(
        "posterior_viz.pdf",
        pad_inches=0.1,
        bbox_inches="tight",
    )

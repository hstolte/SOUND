#!/usr/bin/env python3
import argparse
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import scipy
from matplotlib import ticker
from matplotlib.lines import Line2D

from plot import save_fig, LEGEND_LABEL_SIZE, LEGEND_TITLE_SIZE
from pathlib import Path
import os
import yaml

from data_prep.astro_data import sources

# Plot visual config
plt.style.use("ggplot")
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
matplotlib.rcParams["hatch.linewidth"] = 0.2
matplotlib.rcParams["xtick.labelsize"] = 10
sns.set_palette(sns.color_palette("Set2", n_colors=14, desat=0.9))
sns.set_style("ticks")

COL_VIOLATIONPROB = "violationprob"
COL_TIMESTAMP = "timestamp"

label_size = 12

grid_params = {
    "color": "#4f8c9d",
    "alpha": 0.8,
    "linestyle": ":",
    "linewidth": 0.5,
}

tick_params = dict(
    direction="in",
    which="both",
    right=True,
    top=True,
    labelsize=label_size,
)

OUTCOME_INCONCLUSIVE = 0
OUTCOME_SATISFIED = 1
OUTCOME_VIOLATED = 2


def style_mapping(style_int):
    styles = [
        {"linestyle": "-", "marker": "o", "markersize": 3, "color": "slategrey"},  # style 0
        {"linestyle": "-", "marker": "x", "markersize": 3, "color": "slategrey"},  # style 1
        # ... add more styles as needed
    ]
    return styles[style_int]


def find_check_result_files(root):
    for root, _, files in os.walk(root):
        for file in files:
            if file.startswith("checkresult") and file.endswith(".csv"):
                yield os.path.join(root, file)


def do_plot_violationprob(path):
    # with open(f"{path}/experiment.yaml") as infoYaml:
    #     experimentInfo = yaml.load(
    #         "\n".join(infoYaml.readlines()), Loader=yaml.FullLoader
    #     )
    #
    # print(experimentInfo)

    # print(checkresult_path)
    df_all = pd.read_csv(path)

    for key, df in df_all.groupby("key"):
        fig, ax = plt.subplots(figsize=(30, 5))

        # timestamp / must have no duplicated values
        n_duplicated_indices = df[COL_TIMESTAMP].duplicated().sum()
        if n_duplicated_indices > 0:
            print(
                key,
                " index has ",
                n_duplicated_indices,
                " of ",
                len(df),
                " duplicated indices.",
                path,
            )
            continue

        # ax.plot(df[COL_TIMESTAMP], df[COL_VIOLATIONPROB], linewidth=0.5, marker="x")
        # ax.set_ylim(0, 1.1)

        prior_successes = 1
        prior_failures = 1
        observed_successes = n_samples - (df[COL_VIOLATIONPROB].to_numpy() * n_samples)
        n_observations = np.full_like(observed_successes, n_samples)

        # get posterior distribution from bayesian binominal test (beta distribution)
        posterior = scipy.stats.beta(
            prior_successes + observed_successes,
            prior_failures + n_observations - observed_successes,
        )

        # get probability of posterior distribution being smaller than 0.5
        prob_violated = posterior.cdf(0.5)
        # ci_lower, ci_upper = posterior.interval(0.95)
        expected_value = posterior.mean()

        ax.plot(
            df[COL_TIMESTAMP],
            prob_violated,
            "D",
            markersize=3,
            mfc="black",
            mec="black",
        )
        ax.set_ylim(-0.1, 1.1)
        ax.set_xlabel("Time $[$s$]$")
        # ax.set_ylabel(xlabel)
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
            labelsize="x-large",
        )

        out_path = os.path.join(
            Path(path).parent,
            "violationprob",
            Path(path).stem + "_" + key.replace("/", "-") + ".pdf",
        )

        Path.mkdir(Path(out_path).parent, exist_ok=True)

        fig.savefig(
            out_path,
            pad_inches=0.1,
            bbox_inches="tight",
        )

        plt.close(fig)

    # print(df.info())


def get_posterior(df, n_samples):
    # get posterior distribution from bayesian binominal test (beta distribution)
    posterior = scipy.stats.beta(
        df.beta,
        df.alpha,
    )

    return posterior


def do_plot_probdist(path, n_samples):
    # with open(f"{path}/experiment.yaml") as infoYaml:
    #     experimentInfo = yaml.load(
    #         "\n".join(infoYaml.readlines()), Loader=yaml.FullLoader
    #     )
    #
    # print(experimentInfo)

    # print(checkresult_path)
    df_all = pd.read_csv(path)

    for key, df in df_all.groupby("key"):
        fig, ax = plt.subplots(figsize=(30, 5))

        # timestamp / must have no duplicated values
        n_duplicated_indices = df[COL_TIMESTAMP].duplicated().sum()
        if n_duplicated_indices > 0:
            print(
                key,
                " index has ",
                n_duplicated_indices,
                " of ",
                len(df),
                " duplicated indices.",
                path,
            )
            continue

        # ax.plot(df[COL_TIMESTAMP], df[COL_VIOLATIONPROB], linewidth=0.5, marker="x")
        # ax.set_ylim(0, 1.1)

        posterior = get_posterior(df, n_samples)

        # get probability of posterior distribution being smaller than 0.5
        prob_violated = posterior.cdf(0.5)
        # ci_lower, ci_upper = posterior.interval(0.95)
        expected_value = posterior.mean()

        ax.plot(
            df[COL_TIMESTAMP],
            expected_value,
            "D",
            markersize=3,
            mfc="black",
            mec="black",
        )
        ax.set_ylim(-0.1, 1.1)
        ax.set_xlabel("Time $[$s$]$")
        # ax.set_ylabel(xlabel)
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
            labelsize="x-large",
        )
        # ax.set(**gridspec_kw)

        for alpha, confidence_interval in [
            # (0.05, posterior.interval(0.8)),
            # (0.1, posterior.interval(0.9)),
            (0.15, posterior.interval(0.95)),
            (0.2, posterior.interval(0.99)),
        ]:
            ax.fill_between(
                df[COL_TIMESTAMP],
                confidence_interval[0],
                confidence_interval[1],
                alpha=alpha,
                color="grey",
            )

        # n_satisfied = (df[COL_VIOLATIONPROB] < 0.5).sum()
        # n_violated = len(df) - n_satisfied

        # print(n_violated, " of ", len(df), " are violated. (", path, ")")

        out_path = os.path.join(
            Path(path).parent,
            "probdist",
            Path(path).stem + "_" + key.replace("/", "-") + ".pdf",
        )

        Path.mkdir(Path(out_path).parent, exist_ok=True)

        fig.savefig(
            out_path,
            pad_inches=0.1,
            bbox_inches="tight",
        )

        plt.close(fig)

    # print(df.info())


def check_dataset_keys(path):
    df = pd.read_csv(path)
    print(df.info())
    # print(df[id].duplicated().sum())


def get_variant_name(experiment, panel_groups_param, group_key):
    for v in experiment["variants"]:
        if v[panel_groups_param] == group_key:
            return v["name"]

    raise ValueError("Unknown group key" + str(group_key))


def compare_dataframes(df1, df2):
    # Ensure the dataframes are aligned properly
    df1, df2 = df1.align(df2, axis=1)

    # Compute the percentage differences
    percent_diff = ((df2 - df1) / ((df1 + df2) / 2)) * 100

    # Concatenate the dataframes and the percentage differences dataframe
    concatenated_df = pd.concat(
        [df1, df2, percent_diff],
        keys=["DataFrame 1", "DataFrame 2", "Percentage Difference"],
    )

    # Print the concatenated dataframe
    return concatenated_df


def make_VA_plot(
    path,
    panel_groups_param,
    constraint,
    constraint_key,
    xlim,
    param_label,
):
    with open(os.path.join(arguments.path, "experiment.yaml"), mode="r") as f:
        experiment = yaml.safe_load(f)

    checkresult_file_paths = list(find_check_result_files(arguments.path))

    group_keys = experiment["dimensions"][panel_groups_param]

    path_by_groupkey = {
        key: os.path.join(
            path,
            experiment["query"]
            + "_"
            + get_variant_name(experiment, panel_groups_param, key),
            "1",
            f"checkresult_{constraint}_0.csv",
        )
        for key in group_keys
    }

    # for a, b in zip(path_by_groupkey.values(), checkresult_file_paths):
    #     print(a)
    #     print(b)
    #     print("-" * 80)

    df_by_group = {
        k: pd.read_csv(path).assign(group_key=k) for k, path in path_by_groupkey.items()
    }
    df = list(df_by_group.values())[0]

    case_keys = [
        # "4FGL_J0035.9+59504FGL_J0035.9+5950",
        "4FGL_J0035.8+61314FGL_J0035.8+6131",
    ]

    if constraint_key not in case_keys:
        return

    # select only constraint key
    df = df[df["key"] == constraint_key]

    print(constraint_key)

    n_rows = 3
    fig, axes = plt.subplots(
        n_rows,
        1,
        sharex=True,
        figsize=(6.4, (4.8 / 3) * n_rows),
        squeeze=False,
    )

    # df = df_by_group[case_key]

    # select only constraint key
    # df = df[df["key"] == constraint_key]

    n_samples = experiment["n_samples"]
    CI = experiment["CI"]

    plot_constraint_result_panel(axes.flat[0], df, n_samples, xlim, CI)
    axes.flat[-1].set_xticklabels([])
    axes.flat[-1].set_xlabel("Time", fontsize=label_size)
    axes.flat[-1].set_ylabel("Value Mean", fontsize=label_size)
    axes.flat[1].set_ylabel("Value Uncertainty", fontsize=label_size)
    # axes.flat[2].set_ylabel("Data Sparsity", fontsize=label_size)
    axes.flat[0].set_ylabel("Violation Prob.", fontsize=label_size)
    axes.flat[1].plot(
        df[COL_TIMESTAMP],
        df["meanuncertainty_0"],
        **style_mapping(0)
        # markersize=4,
        # mfc="red",
        # mec="red",
    )

    axes.flat[1].plot(
        df[COL_TIMESTAMP],
        df["meanuncertainty_1"],
        **style_mapping(1)
        # markersize=4,
        # mfc="red",
        # mec="red",
    )

    axes.flat[1].plot(
        df[COL_TIMESTAMP],
        df["meanuncertainty_0"],
        **style_mapping(0)
        # markersize=4,
        # mfc="red",
        # mec="red",
    )

    # axes.flat[2].plot(
    #     df[COL_TIMESTAMP],
    #     df["n_points_0"],
    #     **style_mapping(0)
    #     # markersize=4,
    #     # mfc="red",
    #     # mec="red",
    # )
    #
    # axes.flat[2].plot(
    #     df[COL_TIMESTAMP],
    #     df["n_points_1"],
    #     **style_mapping(1)
    #     # markersize=4,
    #     # mfc="red",
    #     # mec="red",
    # )

    axes.flat[2].plot(
        df[COL_TIMESTAMP],
        df["valuemean_0"],
        **style_mapping(0)
        # markersize=4,
        # mfc="red",
        # mec="red",
    )

    axes.flat[2].plot(
        df[COL_TIMESTAMP],
        df["valuemean_1"],
        **style_mapping(1)
        # markersize=4,
        # mfc="red",
        # mec="red",
    )

    # axes.flat[2].set_ylim([-0.5, 3.5])

    out_path = os.path.join(
        path,
        "VA_cases",
        panel_groups_param,
        constraint,
        "figure_" + panel_groups_param + "_" + constraint_key + ".pdf",
    )

    VA_out_path = os.path.join(
        path,
        "VA_cases",
        panel_groups_param,
        constraint,
        "VA" + panel_groups_param + "_" + constraint_key + ".csv",
    )

    # Get smallest positive value for float64
    smallest_pos = np.finfo(float).tiny

    # axes.flat[1].set_yscale("log")
    # axes.flat[2].set_yscale("log")
    axes.flat[1].sharey(axes.flat[-1])
    #
    # ymin, ymax = axes.flat[1].get_ylim()
    #
    # # Ensure ymin and ymax are strictly positive
    # ymin = max(ymin, smallest_pos)
    # y_max = max(ymax, ymax + smallest_pos)
    #
    # # Safely adjust the limits
    # axes.flat[1].set_ylim(ymin * 0.98, ymax * 2.0)

    for ax in axes.flat:
        ax.grid(**grid_params)
        ax.tick_params(**tick_params)
        # ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f'))

    fig.align_labels(axes)

    sdf = df[df[COL_TIMESTAMP].between(*xlim)]
    sdf = sdf.assign(c=sdf.outcome == OUTCOME_VIOLATED)

    # df_violated = sdf[sdf["c"]]
    # df_satisfied = sdf[~sdf["c"]]

    if not sdf["c"].any():
        print(constraint_key, "-", "no constraint violation.")
        return

    idx_first_violation = sdf["c"].idxmax()
    print(f"{df.loc[idx_first_violation, COL_TIMESTAMP]=}")
    int_position = sdf.index.get_loc(idx_first_violation)
    idx_before = sdf.index[int_position - 1] if int_position > 0 else None

    # df_before_violation = sdf.loc[[idx_before]]
    # df_violation = sdf.loc[[idx_first_violation]]

    VA_df = sdf.loc[:idx_first_violation]

    v0_diff = VA_df.iloc[-1]["valuemean_0"] / VA_df.iloc[-2]["valuemean_0"]
    v1_diff = VA_df.iloc[-1]["valuemean_1"] / VA_df.iloc[-2]["valuemean_1"]

    u0_diff = VA_df.iloc[-1]["meanuncertainty_0"] / VA_df.iloc[-2]["meanuncertainty_0"]
    u1_diff = VA_df.iloc[-1]["meanuncertainty_1"] / VA_df.iloc[-2]["meanuncertainty_1"]

    print(f"{v0_diff=}")
    print(f"{v1_diff=}")
    print(f"{u0_diff=}")
    print(f"{u1_diff=}")

    # print("df_before_violation:")
    # print(df_before_violation)
    # print("df_violation:")
    # print(df_violation)

    Path.mkdir(Path(out_path).parent, exist_ok=True, parents=True)

    VA_df.to_csv(VA_out_path)

    x_change_point = np.mean(
        [
            sdf.loc[idx_before, COL_TIMESTAMP],
            sdf.loc[idx_first_violation, COL_TIMESTAMP],
        ]
    )

    for ax in axes.flat:
        # Add vertical line at x position 2643041396
        ax.axvline(x=x_change_point, color="grey", linestyle="--")

    # Add text label at the vertical line
    # axes.flat[1].text(
    #     x_change_point,
    #     0.5,  # This centers the text vertically
    #     "Change Point",
    #     rotation=90,
    #     verticalalignment="center",
    #     fontsize=label_size * 0.8,
    #     transform=axes.flat[1].get_xaxis_transform(),
    #     # horizontalalignment="left",
    #     # This sets the x-coordin ate in data units and the y-coordinate in axes units
    # )
    # Add text label at the vertical line with some spacing
    # axes.flat[1].annotate(
    #     "Change Point",
    #     xy=(x_change_point, 0.5),
    #     xycoords=axes.flat[1].get_xaxis_transform(),
    #     xytext=(3, 0),  # 3 points offset from the line
    #     textcoords="offset points",
    #     rotation=90,
    #     verticalalignment="center",
    #     fontsize=label_size * 0.8,
    #     color="grey",
    # )

    axes.flat[0].annotate(
        "Change Point",
        xy=(x_change_point, 1 - 0.12),
        xycoords=axes.flat[0].get_xaxis_transform(),
        xytext=(-4, 0),
        textcoords="offset points",
        rotation=0,
        verticalalignment="top",
        horizontalalignment="right",
        fontsize=label_size,
        color="grey",
    )

    fig.savefig(
        out_path,
        pad_inches=0.1,
        bbox_inches="tight",
    )

    plt.close(fig)


def plot_constraint_result_panel(ax, df, n_samples, xlim, CI):

    outcome = df.outcome
    # if n_samples == 0:
    #     outcome = np.where(get_posterior(df, n_samples).rvs() > 0.5, 2 ,1 )

    mask_satisfied = outcome == OUTCOME_SATISFIED
    mask_violated = outcome == OUTCOME_VIOLATED
    mask_inconclusive = outcome == OUTCOME_INCONCLUSIVE

    if n_samples > 0:
        posterior = get_posterior(df, n_samples)

        expected_value = posterior.mean()
        # prob_violated = 1 - posterior.cdf(0.5)

        # Get one confidence interval
        confidence_interval = posterior.interval(950 / 1000.0)
        # confidence_interval = posterior.interval(CI / 1000.0)

        # Calculate error relative to the expected value
        lower_error = expected_value - confidence_interval[0]
        upper_error = confidence_interval[1] - expected_value

        # Plot conclusive error bars
        ax.errorbar(
            df[COL_TIMESTAMP],
            expected_value,
            yerr=(lower_error, upper_error),
            fmt="none",
            ecolor="grey",
            alpha=0.75,
            capsize=0,
        )

        ax.set_yticks(np.array([0, 0.5, 1]))

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

    else:
        expected_value = np.full_like(outcome, 0.5, dtype=float)
        ax.set_yticks([0.5, 0.5])
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

    ax.plot(
        df[COL_TIMESTAMP][mask_satisfied],
        expected_value[mask_satisfied],
        "D",
        markersize=4,
        mfc="green",
        mec="green",
        # mfc="#DC143C",
        # mec="#DC143C",
    )

    ax.plot(
        df[COL_TIMESTAMP][mask_inconclusive],
        expected_value[mask_inconclusive],
        "o",
        markersize=4,
        alpha=1.0,
        mfc="none",
        mec="slategrey",
    )

    # Plot for prob_violated <= 0.5
    ax.plot(
        df[COL_TIMESTAMP][mask_violated],
        expected_value[mask_violated],
        "x",
        markersize=4,
        # mfc="#32CD32",
        # mec="#32CD32",
        mfc="red",
        mec="red",
    )
    if xlim is not None:
        ax.set_xlim(*xlim)

    # ax.set_yticks(np.array([0, 0.5, 1]))
    # if len(ax.get_xticks() < 3):
    #     ax.set_yticks(np.sort(np.append(ax.get_yticks(), 0.5)))

    ax.set_ylim(-0.1, 1.1)
    # ax.set_ylim(-0.01, 1.01)
    # ax.set_ylim(-0.00, 1.00)

    # ax.set_xlim(ax.get_xlim()[0], 0.1 * ax.get_xlim()[1])

    # ax.set_ylabel(xlabel)



def make_scenario_panel_plot(
    path,
    panel_groups_param="manual_value_uncertainty",
    constraint="SGF-6",
    constraint_key="0/12",
    xlim=None,
    param_label="value uncertainty",
    n_samples_CI_lables = False,
    supylabel="Violation Probability",
    figsize=None,
    supylabelx=0.04,
    legend_y = None,
    ncols=1,
):
    with open(os.path.join(arguments.path, "experiment.yaml"), mode="r") as f:
        experiment = yaml.safe_load(f)

    # checkresult_file_paths = list(find_check_result_files(arguments.path))

    group_keys = experiment["dimensions"][panel_groups_param]

    path_by_groupkey = {
        key: os.path.join(
            path,
            experiment["query"]
            + "_"
            + get_variant_name(experiment, panel_groups_param, key),
            "1",
            f"checkresult_{constraint}_0.csv",
        )
        for key in group_keys
    }

    # for a, b in zip(path_by_groupkey.values(), checkresult_file_paths):
    #     print(a)
    #     print(b)
    #     print("-" * 80)

    df_by_group = {
        k: pd.read_csv(path).assign(group_key=k) for k, path in path_by_groupkey.items()
    }

    fig, axes = plt.subplots(
        len(group_keys),
        ncols,
        sharex=True,
        figsize=(6.4*ncols, (4.8 / 3) * len(group_keys)),
        squeeze=False,
    )

    has_violations = False

    for key, ax in zip(group_keys, axes.flatten()):
        df = df_by_group[key]

        # select only constraint key
        df = df[df["key"] == constraint_key]

        # print(constraint_key, key, len(df))

        n_samples = key if panel_groups_param == "nSamples" else experiment["n_samples"]
        CI = key if panel_groups_param == "CI" else experiment["CI"]

        sdf = df[df[COL_TIMESTAMP].between(*xlim)]

        has_violations = has_violations or (sdf.outcome == OUTCOME_VIOLATED).any()

        if n_samples == 0:
            sdf = df_by_group[group_keys[-1]]
            sdf = sdf[sdf["key"] == constraint_key]
            sdf = sdf[sdf[COL_TIMESTAMP].between(*xlim)]
            sdf["outcome"] = np.where(sdf['outcome'] == 0, np.random.choice([1, 2], size=len(sdf)), sdf['outcome'])

        plot_constraint_result_panel(ax, sdf, n_samples, xlim, CI)

        if n_samples_CI_lables:
            ci_float = CI/1000.0
            ci_str = f"{ci_float:.2f}" if ci_float == round(ci_float, 2) else str(ci_float)
            annotation = f"$N = {n_samples}, c = {ci_str}$"
        else:
            annotation = f"{param_label} = {key}"

        special_annotations = {
            "sparsity = 0": "Original sparsity",
            "sparsity = 5": "Higher sparsity",
            "sparsity = 3": "High sparsity",
            "n_samples = 0": "Naive constraint evaluation",
            "n_samples = 200": "SOUND",
            "value uncertainty = 0.01": "Low value uncertainty",
            "value uncertainty = 1": "Medium value uncertainty",
            "value uncertainty = 10": "High value uncertainty",

        }

        ax.annotate(
            special_annotations.get(annotation, annotation),
            xy=(0.025, 0.94),
            xycoords="axes fraction",
            fontsize=11,
            horizontalalignment="left",
            verticalalignment="top",
            bbox=dict(
                boxstyle="round,pad=0.3",
                edgecolor="white",
                facecolor="white",
                alpha=0.66,
            ),
            color="black",
        )
        #
        # # ax.set_ylabel(xlabel)
        # ax.grid(
        #     **{
        #         "color": "#4f8c9d",
        #         "alpha": 0.8,
        #         "linestyle": ":",
        #         "linewidth": 0.5,
        #     }
        # )
        # ax.tick_params(
        #     direction="in",
        #     which="both",
        #     right=True,
        #     top=True,
        #     labelsize=label_size,
        # )
        # ax.set(**gridspec_kw)

    if n_samples == 0:
        for ax in axes.flatten():
            ax.set_yticklabels([])

    axes.flatten()[-1].set_xlabel("Time", fontsize=label_size)

    out_path = os.path.join(
        path,
        panel_groups_param,
        constraint,
        "figure_" + panel_groups_param + "_" + constraint_key + ".pdf",
    )

    for a in axes.flat[:-1]:
        a.set_xticklabels([])
        # a.set_xlabel("")

    # add legend
    legend_markers = [
        Line2D(
            [0],
            [0],
            marker="x",
            color="w",
            mfc="red",
            mec="red",
            markersize=4,
            label="violated",
        ),
        Line2D(
            [0],
            [0],
            marker="D",
            color="w",
            mfc="green",
            mec="green",
            markersize=3,
            label="satisfied",
        ),
        Line2D(
            [0],
            [0],
            linewidth=0,
            marker="o",
            markersize=4,
            alpha=1.0,
            mfc="none",
            mec="slategrey",
            label="inconclusive",
        ),
    ]
    leg = fig.legend(
        handles=legend_markers,
        loc="lower center",
        ncol=len(legend_markers),
        bbox_to_anchor=(0.5, legend_y or 2 * -0.025),
        fontsize=LEGEND_LABEL_SIZE,
        title_fontsize=LEGEND_TITLE_SIZE,
        markerscale=1.5,
    )

    if not has_violations:
        leg.get_frame().set_alpha(0.0)
        for lh in leg.legendHandles:
            lh.set_alpha(0)
        for text in leg.get_texts():
            text.set_alpha(0)

    fig.supylabel(
        supylabel,
        # va="center",
        # rotation="vertical",
        fontsize=label_size,
        x=supylabelx,
    )
    # middle_ax = axes[len(axes) // 2]  # Choose a 'middle' axis
    # middle_ax.set_ylabel("Constraint evaluation results", fontsize=label_size)

    Path.mkdir(Path(out_path).parent, exist_ok=True, parents=True)

    fig.savefig(
        out_path,
        pad_inches=0.1,
        bbox_inches="tight",
    )

    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path", type=str, required=True, help="Path to the experiment result"
    )
    parser.add_argument(
        "--plot", type=str, required=True, help="Path to the experiment result"
    )
    parser.add_argument(
        "--export",
        type=str,
        required=False,
        help=f"Path to export the results, in addition to experiment folder",
        default="./figures",
    )
    arguments = parser.parse_args()

    if arguments.plot == "value_uncertainty":
        make_scenario_panel_plot(
            arguments.path,
            panel_groups_param="manual_value_uncertainty",
            constraint="SGF-6",
            constraint_key="0/12",
            xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
            param_label="value uncertainty",
        )
        exit(0)
    elif arguments.plot == "n_samples":
        make_scenario_panel_plot(
            arguments.path,
            panel_groups_param="nSamples",
            constraint="SGF-6",
            constraint_key="0/12",
            xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
            param_label="n_samples",
            n_samples_CI_lables=True,
            legend_y=-0.11,
        )

        exit(0)

    elif arguments.plot == "CI":
        make_scenario_panel_plot(
            arguments.path,
            panel_groups_param="CI",
            constraint="SGF-6",
            constraint_key="0/12",
            xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
            param_label="CI",
        )
        exit(0)
    elif arguments.plot == "value_sparsity":
        keys = [n.replace(" ", "_") + n.replace(" ", "_") for n in sources]
        for constraint in ["A-3", "A-4"]:
            # for key in keys:
            for key in [
                "4FGL_J0007.7+40084FGL_J0007.7+4008",
                "4FGL_J0009.3+50304FGL_J0009.3+5030",
            ]:
                make_scenario_panel_plot(
                    arguments.path,
                    panel_groups_param="manual_sparsity",
                    constraint=constraint,
                    constraint_key=key,
                    xlim=(2.5e9, 3.5e9),
                    # xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
                    param_label="sparsity",
                )
        exit(0)
    elif arguments.plot == "comparison_nosound":
        # for constraint in ["A-2"]:
        #     # for key in (n.replace(" ", "_") for n in sources):
        #     for key in [
        #         "4FGL_J0005.9+38244FGL_J0005.9+3824"
        #     ]:
        #         make_scenario_panel_plot(
        #             arguments.path,
        #             panel_groups_param="nSamples",
        #             constraint=constraint,
        #             constraint_key=key,
        #             xlim=(3.0e9, 3.5e9),
        #             # xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
        #             param_label="n_samples",
        #         )


        keys = [n.replace(" ", "_") + n.replace(" ", "_") for n in sources]
        for constraint in ["A-3"]:
            # for key in keys:
            for key in [
                "4FGL_J0005.9+38244FGL_J0005.9+3824"
            ]:
                make_scenario_panel_plot(
                    arguments.path,
                    panel_groups_param="nSamples",
                    constraint=constraint,
                    constraint_key=key,
                    # xlim=(1.7e9, 2.236e9),
                    xlim=(1.84e9, 2.246e9),
                    # xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
                    param_label="n_samples",
                    supylabel="Evaluation Outcome",
                    supylabelx=0.075,
                )
        exit(0)

    elif arguments.plot == "comparison_nosound_sg":
        keys = [n.replace(" ", "_") + n.replace(" ", "_") for n in sources]
        # for constraint in ["SGF-6"]:
        #     for key in keys:
                # for key in [
                #     "4FGL_J0007.7+40084FGL_J0007.7+4008",
                #     "4FGL_J0009.3+50304FGL_J0009.3+5030",
                # ]:
        make_scenario_panel_plot(
            arguments.path,
            panel_groups_param="nSamples",
            # constraint=constraint,
            # constraint_key=key,
            xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
            # xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
            # xlim=(-0.9 * 1e7 + 1.378e12, -0.25 * 1e7 + 1.378e12),
            param_label="n_samples",
        )
        exit(0)

    elif arguments.plot == "VA":
        keys = [n.replace(" ", "_") + n.replace(" ", "_") for n in sources]
        for constraint in ["A-3", "A-4"]:
            for key in keys:
                make_VA_plot(
                    arguments.path,
                    panel_groups_param="manual_sparsity",
                    constraint=constraint,
                    constraint_key=key,
                    # xlim=(2.7e9, 3.2e9),  # 3.5
                    xlim=(2.45e9, 3.05e9),  # 3.5
                    param_label="sparsity",
                )
        exit(0)

    checkresult_file_paths = list(find_check_result_files(arguments.path))

    with open(os.path.join(arguments.path, "experiment.yaml"), mode="r") as f:
        n_samples = yaml.safe_load(f)["n_samples"]
    print(n_samples)

    for checkresult_path in checkresult_file_paths:
        do_plot_probdist(checkresult_path, n_samples)
        # do_plot_violationprob(path=checkresult_path)

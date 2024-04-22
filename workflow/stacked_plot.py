import marimo

__generated_with = "0.2.5"
app = marimo.App()


@app.cell
def __():
    import pypsa
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    from collections import OrderedDict
    import marimo as mo
    from matplotlib.ticker import FormatStrFormatter
    return FormatStrFormatter, OrderedDict, mo, np, pd, plt, pypsa


@app.cell
def __(pypsa):
    nw = pypsa.Network("/Users/nomio/Documents/Research/Fall_2324/test/pypsa-usa/workflow/results/western/networks/elec_s_30_ec_lv1.25_Co2L1.25.nc")
    return nw,


@app.cell
def __(nw):
    nw
    return


@app.cell
def __():
    selected_cols = [
        "Balancing Authority",
        "UTC Time at End of Hour",
        "Net Generation (MW) from Natural Gas (Adjusted)",
        "Net Generation (MW) from Coal (Adjusted)",
        "Net Generation (MW) from Nuclear (Adjusted)",
        "Net Generation (MW) from All Petroleum Products (Adjusted)",
        "Net Generation (MW) from Hydropower and Pumped Storage (Adjusted)",
        "Net Generation (MW) from Solar (Adjusted)",
        "Net Generation (MW) from Wind (Adjusted)",
        "Net Generation (MW) from Other Fuel Sources (Adjusted)",
    ]
    rename_op = {
        "CCGT": "Natural gas",
        "hydro": "Hydro",
        "oil": "Oil",
        "onwind": "Onshore wind",
        "solar": "Solar",
        "nuclear": "Nuclear",
        "coal": "Coal",
        "geothermal": "Other",
    }
    rename_his = {
        "Net Generation (MW) from Natural Gas (Adjusted)": "Natural gas",
        "Net Generation (MW) from Hydropower and Pumped Storage (Adjusted)": "Hydro",
        "Net Generation (MW) from All Petroleum Products (Adjusted)": "Oil",
        "Net Generation (MW) from Wind (Adjusted)": "Onshore wind",
        "Net Generation (MW) from Solar (Adjusted)": "Solar",
        "Net Generation (MW) from Nuclear (Adjusted)": "Nuclear",
        "Net Generation (MW) from Coal (Adjusted)": "Coal",
        "Net Generation (MW) from Other Fuel Sources (Adjusted)": "Other",
    }
    colors = [
        "purple",
        "dimgray",
        "brown",
        "royalblue",
        "chocolate",
        "green",
        "lightskyblue",
        "crimson",
    ]
    kwargs = dict(color=colors, legend=False, ylabel="Production [TW]", xlabel="")
    return colors, kwargs, rename_his, rename_op, selected_cols


@app.cell
def __(nw, rename_op):
    optimized = (
        nw.generators_t["p"].groupby(axis="columns", by=nw.generators["carrier"])
        .sum()
        #.loc["2019-01-02 00:00:00":"2019-12-30 23:00:00"]
    )
    # Combine CCGT and OCGT
    optimized["CCGT"] = optimized["CCGT"] + optimized["OCGT"]
    optimized_comb = optimized.drop(["OCGT"], axis=1)
    # Rename and rearrange the columns
    optimized = optimized_comb.rename(columns=rename_op)
    optimized = optimized.loc["2019-01-02 00:00:00":"2019-12-30 23:00:00"]
    return optimized, optimized_comb


@app.cell
def __(OrderedDict, nw):
    bas = []
    for i in range(nw.generators.bus.size):
        if nw.generators.bus[i] not in bas:
            bas.append(nw.generators.bus[i])
    bas_clean = [ba.split("0")[0] for ba in bas]
    bas_clean = [ba.split("-")[0] for ba in bas_clean]
    bas = list(OrderedDict.fromkeys(bas_clean))
    bas_carrier = bas.copy()
    bas.pop(1)
    bas += ["SRP", "AZPS"]
    return bas, bas_carrier, bas_clean, i


@app.cell
def __(pd, rename_his, selected_cols):
    def historic_df(csv_path_1, csv_path_2, bas):
        """
        Create a DataFrame from the csv files containing historical data.
        """
        historic_first = pd.read_csv(
            csv_path_1,
            index_col=[0, 1],
            header=0,
            parse_dates=True,
            date_format="%m/%d/%Y %I:%M:%S %p",
            usecols=selected_cols,
        )
        historic_second = pd.read_csv(
            csv_path_2,
            index_col=[0, 1],
            header=0,
            parse_dates=True,
            date_format="%m/%d/%Y %I:%M:%S %p",
            usecols=selected_cols,
        )
        # Clean the data read from csv
        historic_first_df = (
            historic_first.loc[bas]
            .fillna(0)
            .replace({",": ""}, regex=True)
            .astype(float)
        )
        historic_second_df = (
            historic_second.loc[bas]
            .fillna(0)
            .replace({",": ""}, regex=True)
            .astype(float)
        )
        historic = (
            pd.concat([historic_first_df, historic_second_df], axis=0)
            .groupby(["UTC Time at End of Hour"])
            .sum()
        )
        # Consider negative values as 0 for stackplot purposes
        historic[historic < 0] = 0
        historic = historic.rename(columns=rename_his)
        order = (historic.diff().abs().sum() / historic.sum()).sort_values().index
        historic = historic.reindex(order, axis=1, level=1)
        historic = historic.loc["2019-01-02 00:00:00":"2019-12-30 23:00:00"]
        return historic, order
    return historic_df,


@app.cell
def __(bas, historic_df):
    csv_path_1 = "resources/eia/6moFiles/EIA930_BALANCE_2019_Jan_Jun.csv"
    csv_path_2 = "resources/eia/6moFiles/EIA930_BALANCE_2019_Jul_Dec.csv"
    historic = historic_df(csv_path_1, csv_path_2, bas)[0]
    order = historic_df(csv_path_1, csv_path_2, bas)[1]
    return csv_path_1, csv_path_2, historic, order


@app.cell
def __(historic, optimized):
    diff = (optimized - historic).fillna(0).resample("1D").sum()
    return diff,


@app.cell
def __(FormatStrFormatter, diff, historic, kwargs, optimized, order, plt):
    optimized_plot = optimized / 1e6
    historic_plot = historic/ 1e6

    fig, axes = plt.subplots(3, 1, figsize=(10, 15), gridspec_kw={"hspace": 0.5})
    optimized_plot.reindex(order, axis=1, level=1).resample("1D").sum().plot.area(
        ax=axes[0], **kwargs, title="Optimized"
    )
    lim = axes[0].get_ylim()[1]
    historic_plot.resample("1D").sum().plot.area(ax=axes[1], **kwargs, title="Historic")
    axes[1].set_ylim(top=lim)

    diff_plot = diff / 1e6

    diff_plot.clip(lower=0).plot.area(
        ax=axes[2], **kwargs, title=r"$\Delta$ (Optimized -Historic)"
    )

    diff_plot.clip(upper=0).plot.area(ax=axes[2], **kwargs)
    axes[2].set_ylim(bottom=-lim, top=lim)
    axes[2].yaxis.set_major_formatter(FormatStrFormatter('%.1f'))

    h, l = axes[0].get_legend_handles_labels()
    fig.legend(
        h[::-1],
        l[::-1],
        loc="center left",
        bbox_to_anchor=(0.9, 0.5),
        ncol=1,
        frameon=False,
        labelspacing=1,
        fontsize='small'
    )

    plt.show()
    return axes, diff_plot, fig, h, historic_plot, l, lim, optimized_plot


@app.cell
def __():
    # fig, axes = plt.subplots(3, 1, figsize=(9, 9))
    # optimized.reindex(order, axis=1, level=1).resample("1D").sum().plot.area(ax=axes[0], **kwargs, title="Optimized")
    # historic.resample("1D").sum().plot.area(ax=axes[1], **kwargs, title="Historic")

    # diff = (optimized - historic).fillna(0).resample("1D").sum()
    # diff.clip(lower=0).plot.area(
    #     ax=axes[2],
    #     **kwargs,
    #     title=r"$\Delta$ (Optimized - Historic)",
    # )
    # lim = axes[2].get_ylim()[1]
    # diff.clip(upper=0).plot.area(ax=axes[2], **kwargs)
    # axes[2].set_ylim(bottom=-lim, top=lim)

    # h, l = axes[0].get_legend_handles_labels()
    # fig.legend(
    #     h[::-1],
    #     l[::-1],
    #     loc="center left",
    #     bbox_to_anchor=(1, 0.5),
    #     ncol=1,
    #     frameon=False,
    #     labelspacing=1,
    # )
    return


@app.cell
def __(optimized):
    optimized.drop(columns=['load', 'offwind'], inplace=True)
    return


@app.cell
def __():
    # data = pd.concat([historic, optimized], keys=["Historical", "Optimized"], axis=1)
    # data.columns.names = ["Kind", "Carrier"]
    # fig, ax = plt.subplots(figsize=(9, 8))
    # df = data.groupby(level=["Kind", "Carrier"], axis=1).sum().sum().unstack().T
    # df = df / 1e3  # convert to TWh
    # df.plot.barh(ax=ax, xlabel="Electricity Production [TWh]", ylabel="")
    # ax.set_title("Electricity Production by Carriers")
    # ax.grid(axis="y")
    # plt.show()
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()

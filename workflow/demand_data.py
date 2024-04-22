import marimo

__generated_with = "0.2.5"
app = marimo.App()


@app.cell
def _():
    import pypsa
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    from collections import OrderedDict
    import marimo as mo
    return OrderedDict, mo, np, pd, plt, pypsa


@app.cell
def __(mo):
    mo.md("# Demand data")
    return


@app.cell
def _(pypsa):
    nw = pypsa.Network("/Users/nomio/Documents/Research/Fall_2324/pypsa-usa-n/workflow/results/western/networks/elec_s_30_ec_lv1.25_Co2L0.75.nc")
    # Model demand
    model_demand = nw.loads_t.p.sum(axis=1).resample('D').sum()
    model_generation = nw.generators_t["p"].sum(axis=1).resample('D').sum()
    return model_demand, model_generation, nw


@app.cell
def _(historic, model_demand, model_generation, plt):
    fig, axes = plt.subplots(3,1,figsize=(6, 12), sharey=True)
    historic_demand = historic.iloc[:,0]
    historic_generation = historic.iloc[:,3:].sum(axis=1) 

    historic_demand.resample('D').sum().plot(ax=axes[0], label="Historical demand (MW)")
    historic_generation.resample('D').sum().plot(ax=axes[0], label="Historical generation (MW)")
    axes[0].legend(loc='lower left', bbox_to_anchor=(0, 0))

    historic_demand.resample('D').sum().plot(ax=axes[1], label="Historical demand (MW)")
    model_demand.resample('D').sum().plot(ax=axes[1], label="Model demand (MW)")
    axes[1].legend(loc='lower left', bbox_to_anchor=(0, 0))

    model_demand.plot(ax=axes[2], label="Model demand (MW)")
    model_generation.plot(ax=axes[2], linestyle='--', label="Model generation (MW)")
    axes[2].legend(loc='lower left', bbox_to_anchor=(0, 0))

    plt.subplots_adjust(hspace=0.5)
    plt.show()
    return axes, fig, historic_demand, historic_generation


@app.cell
def _(buses, pd):
     # Historic demand
    csv_path_1 = "/Users/nomio/Documents/Research/Fall_2324/pypsa-usa-n/workflow/resources/eia/6moFiles/EIA930_BALANCE_2019_Jan_Jun.csv"
    csv_path_2 = "/Users/nomio/Documents/Research/Fall_2324/pypsa-usa-n/workflow/resources/eia/6moFiles/EIA930_BALANCE_2019_Jul_Dec.csv"
    selected_cols = [
        "Balancing Authority",
        "UTC Time at End of Hour",
        "Demand (MW) (Adjusted)",
        "Total Interchange (MW) (Adjusted)",
        "Net Generation (MW) (Adjusted)",
        "Net Generation (MW) from Natural Gas (Adjusted)",
        "Net Generation (MW) from Coal (Adjusted)",
        "Net Generation (MW) from Nuclear (Adjusted)",
        "Net Generation (MW) from All Petroleum Products (Adjusted)",
        "Net Generation (MW) from Hydropower and Pumped Storage (Adjusted)",
        "Net Generation (MW) from Solar (Adjusted)",
        "Net Generation (MW) from Wind (Adjusted)",
        "Net Generation (MW) from Other Fuel Sources (Adjusted)",
    ]
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
    historic_first_df = (
        historic_first.loc[buses]
        .fillna(0)
        .replace({",": ""}, regex=True)
        .astype(float)
    )
    historic_second_df = (
        historic_second.loc[buses]
        .fillna(0)
        .replace({",": ""}, regex=True)
        .astype(float)
    )
    historic = (
        pd.concat([historic_first_df, historic_second_df], axis=0)
        .groupby(["UTC Time at End of Hour"])
        .sum()
    )

    historic_carrier = historic.iloc[:, 1:].rename(columns=rename_his)
    order = (
        (historic_carrier.diff().abs().sum() / historic_carrier.sum())
        .sort_values()
        .index
    )
    return (
        csv_path_1,
        csv_path_2,
        historic,
        historic_carrier,
        historic_first,
        historic_first_df,
        historic_second,
        historic_second_df,
        order,
        rename_his,
        rename_op,
        selected_cols,
    )


@app.cell
def __(buses):
    buses
    return


@app.cell
def __(historic):
    historic
    return


@app.cell
def _(OrderedDict, nw):
    buses = []
    for i in range(nw.generators.bus.size):
        if nw.generators.bus[i] not in buses:
            buses.append(nw.generators.bus[i])
    buses_clean = [ba.split("0")[0] for ba in buses]
    buses_clean = [ba.split("-")[0] for ba in buses_clean]
    buses = list(OrderedDict.fromkeys(buses_clean))
    buses_carrier = buses.copy()
    buses.pop(1)
    buses += ["SRP", "AZPS"]
    return buses, buses_carrier, buses_clean, i


@app.cell
def _(historic_demand, model_demand):
    # Mean error
    (historic_demand.sum()-model_demand.sum())/len(model_demand)
    return


@app.cell
def _(mo):
    mo.md("# Balancing authorities")
    return


@app.cell
def __():
    # col_colors = {
    #     "Coal":"dimgray",
    #     "Nuclear":"brown",
    #     "Solar":"royalblue",
    #     "Onshore wind":"chocolate",
    #     "Oil":"green",
    #     "Hydro":"lightskyblue",
    #     "Natural gas":"crimson",
    #     "Other":"gray"
    # }

    # # DataFrame for mean errors
    # df = np.zeros((len(buses_carrier), len(order)))
    # mean_error = pd.DataFrame(df, index=buses_carrier, columns=order)

    # for j in range(len(buses_carrier)):
    #     if j == 1:
    #         ba_historic = (
    #         pd.concat([historic_first_df.loc[buses[25]] + historic_first_df.loc[buses[26]], historic_second_df.loc[buses[25]] + historic_second_df.loc[buses[26]]]).groupby(["UTC Time at End of Hour"]).sum()).rename(columns=rename_his).loc["2019-01-02 00:00:00":"2019-12-30 23:00:00"].iloc[:,1:]
    #     else:
    #         ba_historic = (
    #         pd.concat([historic_first_df.filter(like=buses_carrier[j], axis=0), historic_second_df.filter(like=buses_carrier[j], axis=0)], axis=0).groupby(["UTC Time at End of Hour"]).sum()).rename(columns=rename_his).loc["2019-01-02 00:00:00":"2019-12-30 23:00:00"].iloc[:,1:]
    #     ba_optimized = nw.generators_t["p"].filter(like=buses_carrier[j], axis=1).groupby(axis="columns", by=nw.generators["carrier"]).sum().loc["2019-01-02 00:00:00":"2019-12-30 23:00:00"]
    #     if ("CCGT" in ba_optimized.columns) and ("OCGT" in ba_optimized.columns):
    #         ba_optimized["CCGT"] = ba_optimized["CCGT"] + ba_optimized["OCGT"]
    #         ba_optimized = ba_optimized.drop(["OCGT"], axis=1)
    #     ba_optimized = ba_optimized.rename(columns=rename_op)
    #     ba_optimized = ba_optimized.reindex(order, axis=1, level=1)
    #     ba_historic = ba_historic.reindex(order, axis=1, level=1)

    #     # Mean error calculation
    #     mean_error.loc[buses_carrier[j]] = abs(ba_optimized.sum()-ba_historic.sum())/(len(ba_optimized))

    #     # Plotting
    #     ba_historic[ba_historic < 0] = 0
    #     fig_ba, axes = plt.subplots(2, 1, figsize=(9, 9))
    #     ba_optimized.resample("1D").sum().plot.area(ax=axes[0], title="Optimized", color=[col_colors.get(col, 'gray') for col in ba_optimized.columns], xlabel="")
    #     ba_historic.resample("1D").sum().plot.area(ax=axes[1], title="Historic", color=[col_colors.get(col, 'gray') for col in ba_historic.columns], xlabel="")
    #     axes[0].legend(loc='upper left')
    #     axes[1].legend(loc='upper left')
    #     fig_ba.suptitle(f"Balancing Authority: {buses_carrier[j]}")
    #     plt.show()
    return


@app.cell
def __(mean_error):
    mean_error
    return


@app.cell
def __():
    1+1
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()

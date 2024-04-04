import logging
from collections import OrderedDict

import matplotlib.pyplot as plt
import pandas as pd
import pypsa
import seaborn as sns
from pathlib import Path
import geopandas as gpd
import numpy as np

logger = logging.getLogger(__name__)
from _helpers import configure_logging
from constants import EIA_930_REGION_MAPPER

from plot_statistics import (
    plot_region_lmps,
    plot_capacity_factor_heatmap,
    plot_curtailment_heatmap,
    plot_generator_data_panel,
    plot_regional_emissions_bar,
    plot_california_emissions,
)

from plot_network_maps import (
    plot_capacity_map,
    create_title,
    get_bus_scale,
    get_line_scale,
)

sns.set_theme("paper", style="whitegrid")

EIA_carrier_names = {
    "CCGT": "Natural gas",
    "OCGT": "Natural gas",
    "hydro": "Hydro",
    "oil": "Oil",
    "onwind": "Onshore wind",
    "solar": "Solar",
    "nuclear": "Nuclear",
    "coal": "Coal",
    "load": "Load shedding",
}
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
    "Region",
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


def plot_regional_timeseries_comparison(
    n: pypsa.Network,
    colors=None,
):
    """ """
    Path.mkdir(Path(snakemake.output[0]).parents[0]/"regional_timeseries", exist_ok=True)
    regions = n.buses.country.unique()
    regions_clean = [ba.split("-")[0] for ba in regions]
    regions = list(OrderedDict.fromkeys(regions_clean))

    # regions = [ba for ba in regions if ba in ["CISO"]]
    buses = n.buses.copy()
    buses["region"] = [ba.split("-")[0] for ba in buses.country]
    for region in regions:
        region_bus = buses.query(f"region == '{region}'").index
        historic_region, order = create_historical_df(
            snakemake.input.historic_first,
            snakemake.input.historic_second,
            region=region,
        )
        optimized_region = create_optimized_by_carrier(
            n,
            order,
            region=[region],
        )

        plot_timeseries_comparison(
            historic=historic_region,
            optimized=optimized_region,
            save_path=Path(snakemake.output[0]).parents[0]
            /"regional_timeseries"
            / f"{region}_seasonal_stacked_plot.png",
            colors=colors,
        )


def plot_timeseries_comparison(
    historic: pd.DataFrame,
    optimized: pd.DataFrame,
    save_path: str,
    colors=None,
):
    """
    plots a stacked plot for seasonal production for snapshots: January 2 - December 30 (inclusive)
    """
    kwargs = dict(color=colors, ylabel="Production [GW]", xlabel="", linewidth=0)

    fig, axes = plt.subplots(3, 1, figsize=(9, 9))

    optimized.resample("1D").mean().plot.area(
        ax=axes[0],
        **kwargs,
        legend=False,
        title="Optimized",
    )

    historic.resample("1D").mean().plot.area(
        ax=axes[1],
        **kwargs,
        legend=False,
        title="Historic",
    )

    #create new columns for historic for missing carriers in optimized
    for carrier in optimized.columns:
        if carrier not in historic.columns:
            historic[carrier] = 0
    diff = (optimized - historic).fillna(0).resample("1D").mean()
    diff.clip(lower=0).plot.area(ax=axes[2], title=r"$\Delta$ (Optimized - Historic)", legend=False, **kwargs)
    diff.clip(upper=0).plot.area(ax=axes[2], **kwargs,  legend=False)

    lower_lim = min(axes[0].get_ylim()[0], axes[1].get_ylim()[0], axes[2].get_ylim()[0])
    upper_lim = max(axes[0].get_ylim()[1], axes[1].get_ylim()[1], axes[2].get_ylim()[1])
    axes[0].set_ylim(bottom=lower_lim, top=upper_lim)
    axes[1].set_ylim(bottom=lower_lim, top=upper_lim)

    diff_lim_upper = diff.clip(lower=0).sum(axis=1).max()
    diff_lim_lower = diff.clip(upper=0).sum(axis=1).min()
    axes[2].set_ylim(bottom=diff_lim_lower, top=diff_lim_upper)

    h, l = axes[0].get_legend_handles_labels()
    fig.legend(
        h[::-1],
        l[::-1],
        loc="lower right",
        bbox_to_anchor=(1, 0),
        ncol=1,
        frameon=True,
        labelspacing=0.1,
    )
    fig.tight_layout() 
    fig.savefig(save_path)
    plt.close()


def plot_bar_carrier_production(
    historic: pd.DataFrame,
    optimized: pd.DataFrame,
    save_path: str,
):
    # plot by carrier
    data = pd.concat([historic, optimized], keys=["Historic", "Optimized"], axis=1)
    data.columns.names = ["Kind", "Carrier"]
    fig, ax = plt.subplots(figsize=(6, 6))
    df = data.T.groupby(level=["Kind", "Carrier"]).sum().sum().unstack().T
    df = df / 1e3  # convert to TWh
    df.plot.barh(ax=ax, xlabel="Electricity Production [TWh]", ylabel="")
    ax.set_title("Electricity Production by Carriers")
    ax.grid(axis="y")
    fig.savefig(save_path)


def plot_bar_production_deviation(
    historic: pd.DataFrame,
    optimized: pd.DataFrame,
    save_path: str,
):
    # plot strongest deviations for each carrier
    fig, ax = plt.subplots(figsize=(6, 10))
    diff = (optimized - historic).sum() / 1e3  # convert to TW
    diff = diff.dropna().sort_values()
    diff.plot.barh(
        xlabel="Optimized Production - Historic Production [TWh]",
        ax=ax,
    )
    ax.set_title("Strongest Deviations")
    ax.grid(axis="y")
    fig.savefig(save_path)


def create_optimized_by_carrier(n, order, region=None):
    """
    Create a DataFrame from the model output/optimized.
    """
    if region is not None:
        gen_p = n.generators_t["p"].loc[
            :,
            n.generators.bus.map(n.buses.country).str.contains(region[0]),
        ]
        # add imports to region
        region_buses = n.buses[n.buses.country.str.contains(region[0])]
        interface_lines = n.lines[n.lines.bus0.isin(region_buses.index) & ~n.lines.bus1.isin(region_buses.index)]
        flows = n.lines_t.p1.loc[:,interface_lines.index].sum(axis=1)
        imports = flows.apply(lambda x: x if x > 0 else 0)
        exports = flows.apply(lambda x: x if x < 0 else 0)
    else:
        gen_p = n.generators_t["p"]
        imports = None

    optimized = (
        gen_p.T.groupby(by=n.generators["carrier"])
        .sum().T
        .loc["2019-01-02 00:00:00":"2019-12-30 23:00:00"]
    )

    # Combine other carriers into "carrier"
    other_carriers = optimized.columns.difference(EIA_carrier_names.keys())
    # other_carriers = other_carriers.drop("Natural gas")
    optimized["Other"] = optimized[other_carriers].sum(axis=1)
    optimized = optimized.drop(columns=other_carriers)

    # Combine CCGT and OCGT
    if "OCGT" in optimized.columns and "CCGT" in optimized.columns:
        optimized["Natural gas"] = optimized.pop("CCGT") + optimized.pop("OCGT")
    elif "OCGT" in optimized.columns:
        optimized["Natural gas"] = optimized.pop("OCGT")
    elif "CCGT" in optimized.columns:
        optimized["Natural gas"] = optimized.pop("CCGT")


    #adding imports/export to df after cleaning up carriers
    if imports is not None: 
        optimized["imports"] = imports
        optimized["exports"] = exports

    optimized = optimized.rename(columns=EIA_carrier_names)
    return optimized / 1e3


def create_historical_df(
    csv_path_1,
    csv_path_2,
    region=None,
):
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
        low_memory=False,
    )
    historic_first = historic_first[
        historic_first.Region.map(EIA_930_REGION_MAPPER)
        == snakemake.wildcards.interconnect
    ]

    historic_second = pd.read_csv(
        csv_path_2,
        index_col=[0, 1],
        header=0,
        parse_dates=True,
        date_format="%m/%d/%Y %I:%M:%S %p",
        usecols=selected_cols,
        low_memory=False,
    )
    historic_second = historic_second[
        historic_second.Region.map(EIA_930_REGION_MAPPER)
        == snakemake.wildcards.interconnect
    ]

    # Clean the data read from csv
    historic_first_df = (
        historic_first.fillna(0)
        .replace({",": ""}, regex=True)
        .drop(columns="Region", axis=1)
        .astype(float)
    )
    historic_second_df = (
        historic_second.fillna(0)
        .replace({",": ""}, regex=True)
        .drop(columns="Region", axis=1)
        .astype(float)
    )

    historic = pd.concat([historic_first_df, historic_second_df], axis=0)

    if region is not None:
        if region == "Arizona":
            region = ["AZPS"]
        historic = historic.loc[region]

    historic = historic.groupby(["UTC Time at End of Hour"]).sum()

    historic = historic.rename(columns=rename_his)
    historic[historic < 0] = (
        0  # remove negative values for plotting (low impact on results)
    )
    order = (historic.diff().abs().sum() / historic.sum()).sort_values().index
    historic = historic.reindex(order, axis=1, level=1)
    historic = historic / 1e3
    return historic, order


def create_historical(
    demand_path,
    region=None,
):
    """
    Create a DataFrame from the csv files containing historical data.
    """
    historic = pd.read_csv(
        csv_path,
        index_col=[0, 1],
        header=0,
        parse_dates=True,
        date_format="%m/%d/%Y %I:%M:%S %p",
        usecols=selected_cols,
    )
    historic = historic[
        historic.Region.map(EIA_930_REGION_MAPPER) == snakemake.wildcards.interconnect
    ]

    # Clean the data read from csv
    historic_first_df = (
        historic_first.fillna(0)
        .replace({",": ""}, regex=True)
        .drop(columns="Region", axis=1)
        .astype(float)
    )

    if region is not None:
        if region == "Arizona":
            region = ["AZPS"]
        historic = historic.loc[region]

    historic = historic.groupby(["UTC Time at End of Hour"]).sum()

    historic = historic.rename(columns=rename_his)
    historic[historic < 0] = (
        0  # remove negative values for plotting (low impact on results)
    )
    order = (historic.diff().abs().sum() / historic.sum()).sort_values().index
    historic = historic.reindex(order, axis=1, level=1)
    historic = historic / 1e3
    return historic, order


def get_regions(n):
    regions = n.buses.country.unique()
    regions_clean = [ba.split("0")[0] for ba in regions]
    regions_clean = [ba.split("-")[0] for ba in regions_clean]
    regions = list(OrderedDict.fromkeys(regions_clean))
    return regions


def plot_load_shedding_map(
    n: pypsa.Network,
    save: str,
    regions: gpd.GeoDataFrame,
    **wildcards,
):

    load_curtailment = n.generators_t.p.filter(regex="^(.*load).*$")
    load_curtailment_sum = load_curtailment.sum()

    # split the generator name into a multi index where the first level is the bus and the second level is the carrier name
    multi_index = load_curtailment_sum.index.str.rsplit(" ", n=1, expand=True)
    multi_index.rename({0: "bus", 1: "carrier"}, inplace=True)
    load_curtailment_sum.index = multi_index
    bus_values = load_curtailment_sum

    bus_values = bus_values[bus_values.index.get_level_values(1).isin(n.carriers.index)]
    line_values = n.lines.s_nom

    # plot data
    title = create_title("Load Shedding", **wildcards)
    interconnect = wildcards.get("interconnect", None)
    bus_scale = get_bus_scale(interconnect) if interconnect else 1
    line_scale = get_line_scale(interconnect) if interconnect else 1

    fig, _ = plot_capacity_map(
        n=n,
        bus_values=bus_values,
        line_values=line_values,
        link_values=n.links.p_nom.replace(to_replace={pd.NA: 0}),
        regions=regions,
        line_scale=line_scale,
        bus_scale=bus_scale,
        title=title,
    )
    fig.savefig(save)


if __name__ == "__main__":
    if "snakemake" not in globals():
        from _helpers import mock_snakemake

        snakemake = mock_snakemake(  # use Validation config
            "plot_validation_figures",
            interconnect="western",
            clusters=40,
            ll="v1.0",
            opts="Ep",
            sector="E",
        )
    configure_logging(snakemake)
    n = pypsa.Network(snakemake.input.network)

    onshore_regions = gpd.read_file(snakemake.input.regions_onshore)
    offshore_regions = gpd.read_file(snakemake.input.regions_offshore)

    buses = get_regions(n)

    historic, order = create_historical_df(
        snakemake.input.historic_first,
        snakemake.input.historic_second,
    )
    optimized = create_optimized_by_carrier(n, order)

    colors = n.carriers.rename(EIA_carrier_names).color.to_dict()
    colors['Other'] = "#ba91b1"
    colors['imports'] = "#7d1caf"
    colors['exports'] = "#7d1caf"

    plot_timeseries_comparison(
        historic,
        optimized,
        save_path=snakemake.output["seasonal_stacked_plot.pdf"],
        colors=colors,
    )

    plot_regional_timeseries_comparison(
        n,
        colors= colors
    )

    plot_bar_carrier_production(
        historic,
        optimized,
        save_path=snakemake.output["carrier_production_bar.pdf"],
    )

    plot_bar_production_deviation(
        historic,
        optimized,
        save_path=snakemake.output["production_deviation_bar.pdf"],
    )

    # Box Plot
    plot_region_lmps(
        n,
        snakemake.output["val_box_region_lmps.pdf"],
        **snakemake.wildcards,
    )

    # plot_curtailment_heatmap(
    #     n,
    #     snakemake.output["val_heatmap_curtailment.pdf"],
    #     **snakemake.wildcards,
    # )

    plot_capacity_factor_heatmap(
        n,
        snakemake.output["val_heatmap_capacity_factor.pdf"],
        **snakemake.wildcards,
    )

    plot_generator_data_panel(
        n,
        snakemake.output["val_generator_data_panel.pdf"],
        **snakemake.wildcards,
    )

    plot_regional_emissions_bar(
        n,
        snakemake.output["val_bar_regional_emissions.pdf"],
        **snakemake.wildcards,
    )

    n.statistics().to_csv(snakemake.output["val_statistics"])

    plot_load_shedding_map(
        n,
        snakemake.output["val_map_load_shedding.pdf"],
        onshore_regions,
        **snakemake.wildcards,
    )
    # if snakemake.wildcards.interconnect == "western":
    #     plot_california_emissions(
    #         n,
    #         Path(snakemake.output["val_box_region_lmps"]).parents[0]
    #         / "california_emissions.png",
    #         **snakemake.wildcards,
    #     )

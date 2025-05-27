import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from helper import *
from paths import *

### ============== Import Files ============== ###
addresses = pd.read_csv(
    address_path,
    sep=";",
    encoding="cp850",
    usecols=["NUMMER", "SYS_ANLAGE", "QUELLE", "GEBURT", "PLZ", "ANREDE"],
    parse_dates=["SYS_ANLAGE", "GEBURT"],
)
v21056 = pd.read_csv(
    rechnung_path, sep=";", encoding="cp850", parse_dates=["AUF_ANLAGE"]
)
inx = pd.read_excel(
    inx_path, usecols=["NUMMER", "NL_TYPE"]
)  ## To be Updated with the path from the inxmail automated list


### ============== Clean-up Tables ============== ###

addresses = pad_column_with_zeros(addresses, "NUMMER")
inx = pad_column_with_zeros(inx, "NUMMER")
v21056["NUMMER"] = v21056["VERWEIS"].str[2:12]

addresses["SYS_ANLAGE"] = pd.to_datetime(
    addresses["SYS_ANLAGE"], format="%Y-%m-%d", errors="coerce"
)
addresses["GEBURT"] = pd.to_datetime(
    addresses["GEBURT"], format="%Y-%m-%d", errors="coerce"
)
v21056["AUF_ANLAGE"] = pd.to_datetime(
    v21056["AUF_ANLAGE"], format="%Y-%m-%d", errors="coerce"
)


### ============== Merge Address + Rechnung + Inxmail ============== ###
df = pd.merge(addresses, v21056, on="NUMMER", how="left")
df = pd.merge(df, inx, on="NUMMER", how="left")

## Computing Netto Umsatz
df["NETTO_UMSATZ"] = df["BEST_WERT"] - df["MWST1"] - df["MWST2"] - df["MWST3"]
## Cleaning the table
df = df[
    [
        "NUMMER",
        "QUELLE",
        "GEBURT",
        "SYS_ANLAGE",
        "PLZ",
        "ANREDE",
        "NL_TYPE",
        "AUFTRAG_NR",
        "MEDIACODE",
        "BEST_WERT",
        "MWST1",
        "MWST2",
        "MWST3",
        "NETTO_UMSATZ",
        "AUF_ANLAGE",
    ]
]
## Assigning Kundengruppe to Interessenten (Users without Orders)
df = df.sort_values(by="AUF_ANLAGE")
df["AUF_ANLAGE"] = df["AUF_ANLAGE"].fillna(
    "Keine Angabe"
)  # Fill NaT with a default date
df.loc[
    (df["AUF_ANLAGE"] == "Keine Angabe")
    & (df["SYS_ANLAGE"].dt.year >= dt.date.today().year - 1),
    "Kundengruppe",
] = "Neu-Interessenten"
df.loc[
    (df["AUF_ANLAGE"] == "Keine Angabe")
    & (df["SYS_ANLAGE"].dt.year <= dt.date.today().year - 1),
    "Kundengruppe",
] = "Alt-Interessenten"


## Assigning half-year bins for recency
five_years_ago_start, two_years_ago_start, today = get_halfyear_reference_dates()

### ============== Table with all Customer Info ============== ###
grp_rfm = (
    df.groupby("NUMMER")
    .agg(
        geburt=("GEBURT", "first"),
        anrede=("ANREDE", "first"),
        QUELLE=("QUELLE", "first"),
        plz=("PLZ", "first"),
        nl_type=("NL_TYPE", "first"),
        registered_since=("SYS_ANLAGE", "first"),
        recency=("AUF_ANLAGE", "max"),
        gesamt_frequency=("AUFTRAG_NR", "nunique"),
        gesamt_monetary=("NETTO_UMSATZ", "sum"),
        seasonal_ostern=(
            "AUF_ANLAGE",
            seasonal_ostern,
        ),  ## Computing if they only shop during Easter
        seasonal_weihnachten=(
            "AUF_ANLAGE",
            seasonal_weihnachten,
        ),  ## Computing if they only shop during Christmas
        kundengruppe=("Kundengruppe", "first"),
    )
    .reset_index()
)

## Removing those who had only 2 Orders and returned one of their orders (so they are not really seasonal customers)
grp_rfm.loc[grp_rfm["gesamt_frequency"] == 1, "seasonal_ostern"] = False
grp_rfm.loc[grp_rfm["gesamt_frequency"] == 1, "seasonal_weihnachten"] = False

### ============== Separating the Orders of 3-5 years ago from the orders of past 2 years ============== ###
last_5_year = df[
    (df["AUF_ANLAGE"] >= five_years_ago_start)
    & (df["AUF_ANLAGE"] < two_years_ago_start)
]
last_2_year = df[(df["AUF_ANLAGE"] >= two_years_ago_start) & (df["AUF_ANLAGE"] < today)]


### ============== Frequency and Monetary Values for 3-5 years ago and last 2 years ============== ###
last_3_to_5_years = (
    last_5_year.groupby("NUMMER")
    .agg(
        freq_3_to_5_years_ago=("AUFTRAG_NR", "nunique"),
        monetary_3_to_5_years_ago=("NETTO_UMSATZ", "sum"),
    )
    .reset_index()
)
last_2_years = (
    last_2_year.groupby("NUMMER")
    .agg(
        freq_last_2_years=("AUFTRAG_NR", "nunique"),
        monetary_last_2_years=("NETTO_UMSATZ", "sum"),
    )
    .reset_index()
)
## Merging the last 3-5 years and last 2 years dataframes
last_5_2_years = last_3_to_5_years.merge(last_2_years, on="NUMMER", how="outer")
## Merging the last 5 years table with the Complete RFM Table (With gesamt values and other customer details)
grp_rfm_merged = grp_rfm.merge(last_5_2_years, on="NUMMER", how="left")
## Handling missing values and cleaning up negative values (for complete returns)
grp_rfm_merged["freq_3_to_5_years_ago"] = (
    grp_rfm_merged["freq_3_to_5_years_ago"].fillna(0).astype(int)
)
grp_rfm_merged["freq_last_2_years"] = (
    grp_rfm_merged["freq_last_2_years"].fillna(0).astype(int)
)
grp_rfm_merged["monetary_last_2_years"] = (
    grp_rfm_merged["monetary_last_2_years"].fillna(0).astype(int)
)
grp_rfm_merged["monetary_3_to_5_years_ago"] = (
    grp_rfm_merged["monetary_3_to_5_years_ago"].fillna(0).astype(int)
)
grp_rfm_merged["monetary_last_2_years"] = grp_rfm_merged["monetary_last_2_years"].clip(
    lower=0
)
grp_rfm_merged["monetary_3_to_5_years_ago"] = grp_rfm_merged[
    "monetary_3_to_5_years_ago"
].clip(lower=0)
grp_rfm_merged["gesamt_monetary"] = grp_rfm_merged["gesamt_monetary"].clip(lower=0)


### ============== Assigning Age Groups and Anrede and trasnlating Quelle into Sources ============== ###
final_df = grp_rfm_merged.sort_values(by="gesamt_frequency", ascending=False)
final_df["anrede"] = final_df["anrede"].apply(process_anrede)
final_df["anrede"] = final_df["anrede"].replace(anrede)
final_df = assign_age(final_df)
final_df = final_df.rename(columns={"quelle": "QUELLE"})
final_df = assign_sources(final_df)

### ============= Reordering and renaming columns in the df  ============== ###
final_df = final_df[
    [
        "NUMMER",
        "anrede",
        "age_group",
        "SOURCE",
        "plz",
        "nl_type",
        "registered_since",
        "recency",
        "gesamt_frequency",
        "gesamt_monetary",
        "seasonal_ostern",
        "seasonal_weihnachten",
        "kundengruppe",
        "freq_3_to_5_years_ago",
        "monetary_3_to_5_years_ago",
        "freq_last_2_years",
        "monetary_last_2_years",
    ]
].rename(columns={"SOURCE": "quelle"})

### ============== computing the weighted frequency and weighted monetary in the last 5 years ============== ###
final_df["freq_last_5_years"] = round(
    ((final_df["freq_3_to_5_years_ago"] * 0.5) + (final_df["freq_last_2_years"]))
)
final_df["monetary_last_5_years"] = round(
    (
        (final_df["monetary_3_to_5_years_ago"] * 0.5)
        + (final_df["monetary_last_2_years"])
    )
)


#### ============== Computing Recency Scores ============== ###

reference_date = pd.Timestamp(dt.date.today())
final_df["recency"] = pd.to_datetime(final_df["recency"])  # ensure datetime

bin_edges, bin_labels = get_halfyear_bins(reference_date)

final_df["r_score"] = pd.cut(
    final_df["recency"],
    bins=bin_edges,
    labels=bin_labels,
    right=False,
    include_lowest=True,
    ordered=False,
).astype("Int64")


final_df["r_score"] = final_df["r_score"].fillna(0).astype(int)


#### ============== Computing Monetary Scores ============== ###
monetary_bins = [0, 48, 98, 208, 603, float("inf")]
monetary_labels = [1, 2, 3, 4, 5]

# Clip negative values to 0
final_df["monetary_last_5_years"] = final_df["monetary_last_5_years"].clip(lower=0)

# Assign m_score based on manual bins
final_df["m_score"] = pd.cut(
    final_df["monetary_last_5_years"],
    bins=monetary_bins,
    labels=monetary_labels,
    include_lowest=True,
    right=False,  # Make intervals like [0–47), [48–97), ...
).astype("Int64")


#### ============== Computing Frequency Scores ============== ###
freq_bins = [0, 1, 2, 4, 10, float("inf")]
freq_labels = [1, 2, 3, 4, 5]  # Score 1 = low frequency, 5 = high

# Assign frequency score
final_df["f_score"] = pd.cut(
    final_df["freq_last_5_years"],
    bins=freq_bins,
    labels=freq_labels,
    include_lowest=True,
).astype(int)

## Cleaning up m_score and f_score and compute the weighted mf_score
final_df["m_score"] = final_df["m_score"].astype(int)
final_df["f_score"] = final_df["f_score"].astype(int)
final_df["mf_score"] = round(((final_df["m_score"] * 2) + final_df["f_score"]) / 3)

#### ============== Assigning RFM Labels ============== ###
final_df["rfm_label"] = final_df.apply(assign_rfm_label, axis=1)

## Adding the Interessenten labels to the rfm_label columns
final_df.loc[final_df["kundengruppe"] == "Alt-Interessenten", "rfm_label"] = (
    "Alt-Interessenten"
)
final_df.loc[final_df["kundengruppe"] == "Neu-Interessenten", "rfm_label"] = (
    "Neu-Interessenten"
)
final_df = final_df.drop(columns=["kundengruppe"])  ## Remove Kundengruppe column


### ============== Saving the RFM Values to Excel ============== ###
kundengruppe = final_df["rfm_label"].unique()
filtered_final = final_df[
    (final_df["recency"] >= "2015-01-01") | (final_df["recency"] == "Keine Angabe")
]
with pd.ExcelWriter("rfm_segments.xlsx", engine="xlsxwriter") as writer:
    for item in kundengruppe:
        filtered_final[filtered_final["rfm_label"] == item].to_excel(
            writer, sheet_name=item, index=False
        )

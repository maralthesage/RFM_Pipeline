import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from helper import *
from paths import *
import hashlib
## Assigning half-year bins for recency
five_years_ago_start, two_years_ago_start, today = get_halfyear_reference_dates()
half_year_info = get_half_year_info()
### ============== Import Files ============== ###
addresses = pd.read_csv(
    address_path,
    sep=";",
    encoding="cp850",
    usecols=["NUMMER", "SYS_ANLAGE", "QUELLE", "GEBURT", "PLZ", "ANREDE"],
    parse_dates=["SYS_ANLAGE", "GEBURT"],
    low_memory=False
)
v21056 = pd.read_csv(
    rechnung_path, sep=";", encoding="cp850", parse_dates=["AUF_ANLAGE"]
)

inx = pd.read_excel(
    inx_path, usecols=["NUMMER", "NL_TYPE"]
)  ## To be Updated with the path from the inxmail automated list
kw = pd.read_csv('kw.csv',sep=';',encoding='cp850',usecols=['NUMMER','Kundengruppe'])

### ============== Clean-up Tables ============== ###

addresses = pad_column_with_zeros(addresses, "NUMMER")
inx = pad_column_with_zeros(inx, "NUMMER")
kw = pad_column_with_zeros(kw, "NUMMER")
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
address_details = pd.merge(addresses, v21056, on="NUMMER", how="left")
address_details = pd.merge(address_details, inx, on="NUMMER", how="left")


## Computing Netto Umsatz
address_details["NETTO_UMSATZ"] = address_details["BEST_WERT"] - address_details["MWST1"] - address_details["MWST2"] - address_details["MWST3"]
## Cleaning the table
address_details = address_details[
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
address_details = address_details.sort_values(by="AUF_ANLAGE")
address_details["AUF_ANLAGE"] = address_details["AUF_ANLAGE"].fillna(
    pd.Timestamp("1800-01-01")
)  # Fill NaT with a default date
address_details.loc[
    (address_details["AUF_ANLAGE"] == pd.Timestamp("1800-01-01"))
    & (address_details["SYS_ANLAGE"].dt.year >= dt.date.today().year - 1),
    "Kundengruppe",
] = "Neu-Interessenten"
address_details.loc[
    (address_details["AUF_ANLAGE"] == pd.Timestamp("1800-01-01"))
    & (address_details["SYS_ANLAGE"].dt.year <= dt.date.today().year - 1),
    "Kundengruppe",
] = "Alt-Interessenten"




### ============== Table with all Customer Info ============== ###
addresses_grouped = (
    address_details.groupby("NUMMER")
    .agg(
        geburt=("GEBURT", "first"),
        anrede=("ANREDE", "first"),
        QUELLE=("QUELLE", "first"),
        plz=("PLZ", "first"),
        nl_type=("NL_TYPE", "first"),
        registered_since=("SYS_ANLAGE", "first"),
        first_kaufdatum=('AUF_ANLAGE', 'min'),
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
addresses_grouped.loc[addresses_grouped["gesamt_frequency"] == 1, "seasonal_ostern"] = False
addresses_grouped.loc[addresses_grouped["gesamt_frequency"] == 1, "seasonal_weihnachten"] = False
addresses_grouped.loc[
    (addresses_grouped["first_kaufdatum"] > pd.Timestamp(half_year_info['prev_start'])),
    "kundengruppe",
] = "New Customers"
print(addresses_grouped['kundengruppe'].value_counts())
### ============== Separating the Orders of 3-5 years ago from the orders of past 2 years ============== ###
address_detail_5to3 = address_details[
    (address_details["AUF_ANLAGE"] >= five_years_ago_start)
    & (address_details["AUF_ANLAGE"] < two_years_ago_start)
]
address_details_2 = address_details[(address_details["AUF_ANLAGE"] >= two_years_ago_start) & (address_details["AUF_ANLAGE"] < today)]


### ============== Frequency and Monetary Values for 3-5 years ago and last 2 years ============== ###
last_3_to_5_years = (
    address_detail_5to3.groupby("NUMMER")
    .agg(
        freq_3_to_5_years_ago=("AUFTRAG_NR", "nunique"),
        monetary_3_to_5_years_ago=("NETTO_UMSATZ", "sum"),
    )
    .reset_index()
)
last_2_years = (
    address_details_2.groupby("NUMMER")
    .agg(
        freq_last_2_years=("AUFTRAG_NR", "nunique"),
        monetary_last_2_years=("NETTO_UMSATZ", "sum"),
    )
    .reset_index()
)
## Merging the last 3-5 years and last 2 years dataframes
last_5_years = last_3_to_5_years.merge(last_2_years, on="NUMMER", how="outer")
## Merging the last 5 years table with the Complete RFM Table (With gesamt values and other customer details)
addresses_details_last5years = addresses_grouped.merge(last_5_years, on="NUMMER", how="left")
## Handling missing values and cleaning up negative values (for complete returns)
addresses_details_last5years["freq_3_to_5_years_ago"] = (
    addresses_details_last5years["freq_3_to_5_years_ago"].fillna(0).astype(int)
)
addresses_details_last5years["freq_last_2_years"] = (
    addresses_details_last5years["freq_last_2_years"].fillna(0).astype(int)
)
addresses_details_last5years["monetary_last_2_years"] = (
    addresses_details_last5years["monetary_last_2_years"].fillna(0).astype(int)
)
addresses_details_last5years["monetary_3_to_5_years_ago"] = (
    addresses_details_last5years["monetary_3_to_5_years_ago"].fillna(0).astype(int)
)
addresses_details_last5years["monetary_last_2_years"] = addresses_details_last5years["monetary_last_2_years"].clip(
    lower=0
)
addresses_details_last5years["monetary_3_to_5_years_ago"] = addresses_details_last5years[
    "monetary_3_to_5_years_ago"
].clip(lower=0)
addresses_details_last5years["gesamt_monetary"] = addresses_details_last5years["gesamt_monetary"].clip(lower=0)


### ============== Assigning Age Groups and Anrede and trasnlating Quelle into Sources ============== ###
final_addresses = addresses_details_last5years.sort_values(by="gesamt_frequency", ascending=False)
final_addresses["anrede"] = final_addresses["anrede"].apply(process_anrede)
final_addresses["anrede"] = final_addresses["anrede"].replace(anrede)
final_addresses = assign_age(final_addresses)
final_addresses = final_addresses.rename(columns={"quelle": "QUELLE"})
final_addresses = assign_sources(final_addresses)

### ============= Reordering and renaming columns in the df  ============== ###
final_addresses = final_addresses[
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
final_addresses["freq_last_5_years"] = round(
    ((final_addresses["freq_3_to_5_years_ago"] * 0.5) + (final_addresses["freq_last_2_years"]))
)
final_addresses["monetary_last_5_years"] = round(
    (
        (final_addresses["monetary_3_to_5_years_ago"] * 0.5)
        + (final_addresses["monetary_last_2_years"])
    )
)


#### ============== Computing Recency Scores ============== ###

reference_date = pd.Timestamp(dt.date.today())
final_addresses["recency"] = pd.to_datetime(final_addresses["recency"])  # ensure datetime

bin_edges, bin_labels = get_halfyear_bins(reference_date)

final_addresses["r_score"] = pd.cut(
    final_addresses["recency"],
    bins=bin_edges,
    labels=bin_labels,
    right=False,
    include_lowest=True,
    ordered=False,
).astype("Int64")


final_addresses["r_score"] = final_addresses["r_score"].fillna(0).astype(int)


#### ============== Computing Monetary Scores ============== ###
monetary_bins = [0, 48, 98, 208, 603, float("inf")]
monetary_labels = [1, 2, 3, 4, 5]

# Clip negative values to 0
final_addresses["monetary_last_5_years"] = final_addresses["monetary_last_5_years"].clip(lower=0)

# Assign m_score based on manual bins
final_addresses["m_score"] = pd.cut(
    final_addresses["monetary_last_5_years"],
    bins=monetary_bins,
    labels=monetary_labels,
    include_lowest=True,
    right=False,  # Make intervals like [0–47), [48–97), ...
).astype("Int64")


#### ============== Computing Frequency Scores ============== ###
freq_bins = [0, 1, 2, 4, 10, float("inf")]
freq_labels = [1, 2, 3, 4, 5]  # Score 1 = low frequency, 5 = high

# Assign frequency score
final_addresses["f_score"] = pd.cut(
    final_addresses["freq_last_5_years"],
    bins=freq_bins,
    labels=freq_labels,
    include_lowest=True,
).astype(int)

## Cleaning up m_score and f_score and compute the weighted mf_score
final_addresses["m_score"] = final_addresses["m_score"].astype(int)
final_addresses["f_score"] = final_addresses["f_score"].astype(int)
final_addresses["mf_score"] = round(((final_addresses["m_score"] * 2) + final_addresses["f_score"]) / 3)

#### ============== Assigning RFM Labels ============== ###
final_addresses["rfm_label"] = final_addresses.apply(assign_rfm_label, axis=1)

## Adding the Interessenten labels to the rfm_label columns
final_addresses.loc[(final_addresses["kundengruppe"] == "Alt-Interessenten") & (final_addresses['gesamt_monetary'] == 0), "rfm_label"] = (
    "Interessenten"
)
final_addresses.loc[final_addresses["kundengruppe"] == "Neu-Interessenten", "rfm_label"] = (
    "Interessenten"
)
final_addresses.loc[final_addresses["kundengruppe"] == "New Customers", "rfm_label"] = (
    "Neukunden"
)
final_addresses.loc[final_addresses["gesamt_monetary"]==0, "rfm_label"] = "Interessenten"


final_addresses = final_addresses.drop(columns=["kundengruppe"])  ## Remove Kundengruppe column

### ============== Saving the RFM Values to Excel ============== ###
kundengruppe = [
    'Champions',
    'Treue Kunden',
    'Nicht zu verlieren',
    'Potenziell loyale Kunden',
    'Brauchen Aufmerksamkeit',
    'Gefährdete Kunden',
    'Neukunden',
    'Reaktivierte Kunden',
    'Vielversprechende Kunden',
    'Abwandernde Kunden',
    'Schlafende Kunden',
    'Verlorene Kunden',
    'Interessenten',
    'Nicht klassifiziert'
]
# filtered_final = final_addresses[
#     (final_addresses["recency"] >= "2015-01-01") | (final_addresses["recency"] == pd.Timestamp("1800-01-01"))]

kw = kw.rename(columns={"Kundengruppe":"Alte_Kundengeruppe"})
filtered_final_merged = final_addresses.merge(kw,on='NUMMER',how='left')
filtered_final_merged = filtered_final_merged.drop_duplicates(subset='NUMMER')

gesamt_table = filtered_final_merged.groupby(['rfm_label','Alte_Kundengeruppe']).agg(Anzahl_Kunden=('NUMMER','count'),NL_KUNDEN=('nl_type','count')).reset_index()
gesamt_table['rfm_label'] = pd.Categorical(gesamt_table['rfm_label'], categories=kundengruppe, ordered=True)
gesamt_table = gesamt_table.sort_values(by='rfm_label')
gesamt_gesamt = gesamt_table.groupby('rfm_label').agg(Anzahl_Kunden=('Anzahl_Kunden','sum'),NL_KUNDEN=('NL_KUNDEN','sum')).reset_index()
gesamt_gesamt['rfm_label'] = pd.Categorical(gesamt_gesamt['rfm_label'], categories=kundengruppe, ordered=True)
gesamt_gesamt = gesamt_gesamt.sort_values(by='rfm_label')
# with pd.ExcelWriter(f"{sharepoint}rfm_segments.xlsx", engine="xlsxwriter") as writer
#     for item in kundengruppe:
#         filtered_final_merged[filtered_final_merged["rfm_label"] == item].to_excel(
#             writer, sheet_name=item, index=False
#         )


with pd.ExcelWriter(f"{sharepoint}rfm_segments.xlsx", engine="xlsxwriter") as writer:
    for item in kundengruppe:
        df = filtered_final_merged[filtered_final_merged["rfm_label"] == item]
        df.to_excel(writer, sheet_name=item, index=False)

        workbook = writer.book
        worksheet = writer.sheets[item]

        # Define formats
        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})  # YYYY-MM-DD format
        currency_format = workbook.add_format({'num_format': '#,##0 [$€-1];[Red]-#,##0 [$€-1]'})
        yellow_fill = workbook.add_format({'bg_color': '#FFFF00'})
        default_format = workbook.add_format()

        # Set all columns to width 22
        worksheet.set_column("A:X", 22)

        # Apply date format to columns G and H
        worksheet.set_column("G:G", 22, date_format)
        worksheet.set_column("H:H", 22, date_format)

        # Apply currency format to selected columns
        for col in ["J", "N", "P", "R"]:
            worksheet.set_column(f"{col}:{col}", 22, currency_format)

        # Highlight and hide M:P
        worksheet.set_column("M:P", 22, yellow_fill)
        worksheet.set_column("M:P", 22, None, {'hidden': True})

        # Highlight and hide T:U
        worksheet.set_column("T:U", 22, yellow_fill)
        worksheet.set_column("T:U", 22, None, {'hidden': True})

        # Approximate autofit for column X
        worksheet.set_column("X:X", 22)



# Export with formatting
with pd.ExcelWriter(f"{sharepoint}rfm_segments_gesamt.xlsx", engine="xlsxwriter") as writer:
    # Write both tables
    gesamt_gesamt.to_excel(writer, sheet_name='RFM Gesamt Analytik', index=False)
    gesamt_table.to_excel(writer, sheet_name='RFM - Alt Kundengruppe Analytik', index=False)

    workbook = writer.book
    int_format = workbook.add_format({'num_format': '#,##0'})  # Format like 123.456

    ### Format Sheet: RFM Gesamt Analytik
    ws1 = writer.sheets['RFM Gesamt Analytik']
    ws1.set_column("A:A",22)
    ws1.set_column("B:C", 22, int_format)
    

    ### Format Sheet: RFM - Alt Kundengruppe Analytik
    ws2 = writer.sheets['RFM - Alt Kundengruppe Analytik']
    ws2.set_column("A:B", 22)
    ws2.set_column("C:D", 22, int_format)


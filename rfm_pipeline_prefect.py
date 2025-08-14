import pandas as pd
import datetime as dt
from prefect import task, flow
from helper import *
from paths import *

# Constants
LANDS = ["F01", "F02", "F03", "F04"]
KUNDENGRUPPE = [
    "Champions", "Treue Kunden", "Nicht zu verlieren", "Potenziell loyale Kunden",
    "Brauchen Aufmerksamkeit", "Gefährdete Kunden", "Neukunden", "Reaktivierte Kunden",
    "Vielversprechende Kunden", "Abwandernde Kunden", "Schlafende Kunden",
    "Verlorene Kunden", "Interessenten", "Nicht klassifiziert"
]

@task
def get_reference_dates():
    # Use the helper function to get properly typed datetime objects
    five_years_ago_start, two_years_ago_start, today = get_halfyear_reference_dates()
    return five_years_ago_start, two_years_ago_start, pd.Timestamp(today)

@task
def load_data(land):
    # Load addresses
    addresses = pd.read_csv(
        f"/Volumes/MARAL/CSV/{land}/V2AD1001.csv",
        sep=";",
        encoding="cp850",
        usecols=["NUMMER", "SYS_ANLAGE", "QUELLE", "GEBURT", "PLZ", "ANREDE"],
        parse_dates=["SYS_ANLAGE", "GEBURT"],
        low_memory=False,
    )
    
    # Load V2AD1056
    v21056 = pd.read_csv(
        f"/Volumes/MARAL/CSV/{land}/V2AD1056.csv", 
        sep=";", 
        encoding="cp850", 
        parse_dates=["AUF_ANLAGE"]
    )
    
    # Load inxmail data
    inx = pd.read_excel(inx_path, usecols=["NUMMER", "NL_TYPE"])
    
    # Load KW data
    kw = pd.read_csv(
        f"Data/kw_{land}.csv", 
        sep=";", 
        encoding="cp850", 
        usecols=["NUMMER", "Kundengruppe"]
    )
    
    return addresses, v21056, inx, kw

@task
def clean_data(addresses, v21056, inx, kw, land):
    # Pad with zeros
    addresses = pad_column_with_zeros(addresses, "NUMMER")
    inx = pad_column_with_zeros(inx, "NUMMER")
    kw = pad_column_with_zeros(kw, "NUMMER")
    
    # Extract NUMMER from VERWEIS
    v21056["NUMMER"] = v21056["VERWEIS"].str[2:12]
    
    # Convert dates
    addresses["SYS_ANLAGE"] = pd.to_datetime(addresses["SYS_ANLAGE"], format="%Y-%m-%d", errors="coerce")
    addresses["GEBURT"] = pd.to_datetime(addresses["GEBURT"], format="%Y-%m-%d", errors="coerce")
    v21056["AUF_ANLAGE"] = pd.to_datetime(v21056["AUF_ANLAGE"], format="%Y-%m-%d", errors="coerce")
    
    return addresses, v21056, inx, kw

@task
def merge_data(addresses, v21056, inx):
    # Merge all data
    address_details = pd.merge(addresses, v21056, on="NUMMER", how="left")
    address_details = pd.merge(address_details, inx, on="NUMMER", how="left")
    
    # Compute netto umsatz
    address_details["NETTO_UMSATZ"] = (
        address_details["BEST_WERT"]
        - address_details["MWST1"]
        - address_details["MWST2"]
        - address_details["MWST3"]
    )
    
    # Clean up table
    address_details = address_details[
        [
            "NUMMER", "QUELLE", "GEBURT", "SYS_ANLAGE", "PLZ", "ANREDE", "NL_TYPE",
            "AUFTRAG_NR", "MEDIACODE", "BEST_WERT", "MWST1", "MWST2", "MWST3",
            "NETTO_UMSATZ", "AUF_ANLAGE"
        ]
    ]
    
    return address_details

@task
def group_addresses(address_details, half_year_info):
    # First perform the grouping to create first_kaufdatum
    addresses_grouped = (
        address_details.groupby("NUMMER")
        .agg(
            geburt=("GEBURT", "first"),
            anrede=("ANREDE", "first"),
            QUELLE=("QUELLE", "first"),
            plz=("PLZ", "first"),
            nl_type=("NL_TYPE", "first"),
            registered_since=("SYS_ANLAGE", "first"),
            first_kaufdatum=("AUF_ANLAGE", "min"),
            recency=("AUF_ANLAGE", "max"),
            gesamt_frequency=("AUFTRAG_NR", "nunique"),
            gesamt_monetary=("NETTO_UMSATZ", "sum"),
            seasonal_ostern=("AUF_ANLAGE", seasonal_ostern),
            seasonal_weihnachten=("AUF_ANLAGE", seasonal_weihnachten),
        )
        .reset_index()
    )
    
    # Initialize Kundengruppe column
    addresses_grouped["kundengruppe"] = None
    
    return addresses_grouped

@task
def process_customer_groups(addresses_grouped, half_year_info):
    # Clean seasonal flags
    addresses_grouped.loc[addresses_grouped["gesamt_frequency"] == 1, "seasonal_ostern"] = False
    addresses_grouped.loc[addresses_grouped["gesamt_frequency"] == 1, "seasonal_weihnachten"] = False
    
    # Assign New Customers based on first_kaufdatum (using helper's date conversion)
    addresses_grouped.loc[
        (addresses_grouped["first_kaufdatum"] > pd.Timestamp(half_year_info["prev_start"])),
        "kundengruppe",
    ] = "New Customers"
    
    # For records where first_kaufdatum is null (no purchases), assign Interessenten status
    current_year = dt.date.today().year
    mask_no_purchases = addresses_grouped["first_kaufdatum"].isna()
    
    addresses_grouped.loc[
        mask_no_purchases & 
        (addresses_grouped["registered_since"].dt.year >= current_year - 1),
        "kundengruppe"
    ] = "Neu-Interessenten"
    
    addresses_grouped.loc[
        mask_no_purchases & 
        (addresses_grouped["registered_since"].dt.year <= current_year - 1),
        "kundengruppe"
    ] = "Alt-Interessenten"
    
    print(addresses_grouped["kundengruppe"].value_counts())
    return addresses_grouped


@task
def split_time_periods(address_details, five_years_ago_start, two_years_ago_start, today):
    # Ensure all dates are pandas Timestamps for consistent comparison
    address_detail_5to3 = address_details[
        (address_details["AUF_ANLAGE"] >= pd.Timestamp(five_years_ago_start)) &
        (address_details["AUF_ANLAGE"] < pd.Timestamp(two_years_ago_start))
    ]
    
    address_details_2 = address_details[
        (address_details["AUF_ANLAGE"] >= pd.Timestamp(two_years_ago_start)) &
        (address_details["AUF_ANLAGE"] < pd.Timestamp(today))
    ]
    
    return address_detail_5to3, address_details_2

@task
def calculate_time_period_metrics(address_detail_5to3, address_details_2):
    # Calculate metrics for 3-5 years ago
    last_3_to_5_years = (
        address_detail_5to3.groupby("NUMMER")
        .agg(
            freq_3_to_5_years_ago=("AUFTRAG_NR", "nunique"),
            monetary_3_to_5_years_ago=("NETTO_UMSATZ", "sum"),
        )
        .reset_index()
    )
    
    # Calculate metrics for last 2 years
    last_2_years = (
        address_details_2.groupby("NUMMER")
        .agg(
            freq_last_2_years=("AUFTRAG_NR", "nunique"),
            monetary_last_2_years=("NETTO_UMSATZ", "sum"),
        )
        .reset_index()
    )
    
    return last_3_to_5_years, last_2_years

@task
def merge_time_periods(addresses_grouped, last_3_to_5_years, last_2_years):
    # Merge time periods
    last_5_years = last_3_to_5_years.merge(last_2_years, on="NUMMER", how="outer")
    addresses_details_last5years = addresses_grouped.merge(last_5_years, on="NUMMER", how="left")
    
    # Handle missing values
    for col in ["freq_3_to_5_years_ago", "freq_last_2_years", "monetary_last_2_years", "monetary_3_to_5_years_ago"]:
        addresses_details_last5years[col] = addresses_details_last5years[col].fillna(0).astype(int)
    
    # Clip negative values
    for col in ["monetary_last_2_years", "monetary_3_to_5_years_ago", "gesamt_monetary"]:
        addresses_details_last5years[col] = addresses_details_last5years[col].clip(lower=0)
    
    return addresses_details_last5years

@task
def final_processing(final_addresses):
    # Process anrede and age groups
    final_addresses["anrede"] = final_addresses["anrede"].apply(process_anrede)
    final_addresses["anrede"] = final_addresses["anrede"].replace(anrede)
    final_addresses = assign_age(final_addresses)
    final_addresses = final_addresses.rename(columns={"quelle": "QUELLE"})
    final_addresses = assign_sources(final_addresses)
    
    # Reorder columns
    final_addresses = final_addresses[
        [
            "NUMMER", "anrede", "age_group", "SOURCE", "plz", "nl_type",
            "registered_since", "recency", "gesamt_frequency", "gesamt_monetary",
            "seasonal_ostern", "seasonal_weihnachten", "kundengruppe",
            "freq_3_to_5_years_ago", "monetary_3_to_5_years_ago",
            "freq_last_2_years", "monetary_last_2_years"
        ]
    ].rename(columns={"SOURCE": "quelle"})
    
    # Calculate weighted metrics
    final_addresses["freq_last_5_years"] = round(
        (final_addresses["freq_3_to_5_years_ago"] * 0.5) + final_addresses["freq_last_2_years"]
    )
    final_addresses["monetary_last_5_years"] = round(
        (final_addresses["monetary_3_to_5_years_ago"] * 0.5) + final_addresses["monetary_last_2_years"]
    )
    
    return final_addresses

@task
def calculate_rfm_scores(final_addresses, reference_date):
    # Recency score
    bin_edges, bin_labels = get_halfyear_bins(reference_date)
    final_addresses["recency"] = pd.to_datetime(final_addresses["recency"])
    final_addresses["r_score"] = pd.cut(
        final_addresses["recency"],
        bins=bin_edges,
        labels=bin_labels,
        right=False,
        include_lowest=True,
        ordered=False,
    ).astype("Int64").fillna(0).astype(int)
    
    # Monetary score
    monetary_bins = [0, 48, 98, 208, 603, float("inf")]
    monetary_labels = [1, 2, 3, 4, 5]
    final_addresses["monetary_last_5_years"] = final_addresses["monetary_last_5_years"].clip(lower=0)
    final_addresses["m_score"] = pd.cut(
        final_addresses["monetary_last_5_years"],
        bins=monetary_bins,
        labels=monetary_labels,
        include_lowest=True,
        right=False,
    ).astype("Int64")
    
    # Frequency score
    freq_bins = [0, 1, 2, 4, 10, float("inf")]
    freq_labels = [1, 2, 3, 4, 5]
    final_addresses["f_score"] = pd.cut(
        final_addresses["freq_last_5_years"],
        bins=freq_bins,
        labels=freq_labels,
        include_lowest=True,
    ).astype(int)
    
    # Combined score
    final_addresses["m_score"] = final_addresses["m_score"].astype(int)
    final_addresses["mf_score"] = round(
        ((final_addresses["m_score"] * 2) + final_addresses["f_score"]) / 3
    )
    
    # RFM labels
    final_addresses["rfm_label"] = final_addresses.apply(assign_rfm_label, axis=1)
    
    # Special cases
    final_addresses.loc[
        (final_addresses["kundengruppe"] == "Alt-Interessenten") &
        (final_addresses["gesamt_monetary"] == 0),
        "rfm_label",
    ] = "Interessenten"
    final_addresses.loc[
        final_addresses["kundengruppe"] == "Neu-Interessenten", "rfm_label"
    ] = "Interessenten"
    final_addresses.loc[final_addresses["kundengruppe"] == "New Customers", "rfm_label"] = "Neukunden"
    final_addresses.loc[final_addresses["gesamt_monetary"] == 0, "rfm_label"] = "Interessenten"
    
    final_addresses = final_addresses.drop(columns=["kundengruppe"])
    
    return final_addresses

@task
def export_results(final_addresses, kw, land, today):
    # Merge with KW data
    kw = kw.rename(columns={"Kundengruppe": "Alte_Kundengeruppe"})
    filtered_final_merged = final_addresses.merge(kw, on="NUMMER", how="left")
    filtered_final_merged = filtered_final_merged.drop_duplicates(subset="NUMMER")
    
    # Create summary tables
    gesamt_table = (
        filtered_final_merged.groupby(["rfm_label", "Alte_Kundengeruppe"])
        .agg(Anzahl_Kunden=("NUMMER", "count"), NL_KUNDEN=("nl_type", "count"))
        .reset_index()
    )
    gesamt_table["rfm_label"] = pd.Categorical(
        gesamt_table["rfm_label"], categories=KUNDENGRUPPE, ordered=True
    )
    gesamt_table = gesamt_table.sort_values(by="rfm_label")
    
    gesamt_gesamt = (
        gesamt_table.groupby("rfm_label")
        .agg(Anzahl_Kunden=("Anzahl_Kunden", "sum"), NL_KUNDEN=("NL_KUNDEN", "sum"))
        .reset_index()
    )
    gesamt_gesamt["rfm_label"] = pd.Categorical(
        gesamt_gesamt["rfm_label"], categories=KUNDENGRUPPE, ordered=True
    )
    gesamt_gesamt = gesamt_gesamt.sort_values(by="rfm_label")
    
    # Export CSV
    filtered_final_merged.to_csv(
        f'/Volumes/MARAL/Data/rfm_labels/rfm_labels_{land}_prefect.csv',
        sep=';',
        index=False,
        encoding='cp850'
    )
    
    # Export Excel with formatting
    with pd.ExcelWriter(f"Data/rfm_segments_{today.date()}_{land}_prefect.xlsx", engine="xlsxwriter") as writer:
        for item in KUNDENGRUPPE:
            df = filtered_final_merged[filtered_final_merged["rfm_label"] == item]
            df.to_excel(writer, sheet_name=item, index=False)

            workbook = writer.book
            worksheet = writer.sheets[item]

            # Define formats
            date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
            currency_format = workbook.add_format({"num_format": "#,##0 [$€-1];[Red]-#,##0 [$€-1]"})
            yellow_fill = workbook.add_format({"bg_color": "#FFFF00"})

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
            worksheet.set_column("M:P", 22, None, {"hidden": True})

            # Highlight and hide T:U
            worksheet.set_column("T:U", 22, yellow_fill)
            worksheet.set_column("T:U", 22, None, {"hidden": True})

            # Approximate autofit for column X
            worksheet.set_column("X:X", 22)

    # Export summary tables
    with pd.ExcelWriter(
        f"Data/rfm_segments_gesamt_{today.date()}_{land}_prefect.xlsx", engine="xlsxwriter"
    ) as writer:
        # Write both tables
        gesamt_gesamt.to_excel(writer, sheet_name="RFM Gesamt Analytik", index=False)
        gesamt_table.to_excel(
            writer, sheet_name="RFM - Alt Kundengruppe Analytik", index=False
        )

        workbook = writer.book
        int_format = workbook.add_format({"num_format": "#,##0"})

        ### Format Sheet: RFM Gesamt Analytik
        ws1 = writer.sheets["RFM Gesamt Analytik"]
        ws1.set_column("A:A", 22)
        ws1.set_column("B:C", 22, int_format)

        ### Format Sheet: RFM - Alt Kundengruppe Analytik
        ws2 = writer.sheets["RFM - Alt Kundengruppe Analytik"]
        ws2.set_column("A:B", 22)
        ws2.set_column("C:D", 22, int_format)


@flow(name="process_land_data")
def process_land_data(land):
    # Get reference dates using helper functions
    five_years_ago_start, two_years_ago_start, today = get_reference_dates()
    half_year_info = get_half_year_info(land=land)
    
    # Load and clean data
    addresses, v21056, inx, kw = load_data(land)
    addresses, v21056, inx, kw = clean_data(addresses, v21056, inx, kw, land)
    
    # Merge data
    address_details = merge_data(addresses, v21056, inx)
    
    # Group addresses first to create first_kaufdatum
    addresses_grouped = group_addresses(address_details, half_year_info)
    
    # Then process customer groups
    addresses_grouped = process_customer_groups(addresses_grouped, half_year_info)
    
    # Split time periods with proper datetime conversion
    address_detail_5to3, address_details_2 = split_time_periods(
        address_details, 
        five_years_ago_start, 
        two_years_ago_start, 
        today
    )
    
    # Calculate time period metrics
    last_3_to_5_years, last_2_years = calculate_time_period_metrics(address_detail_5to3, address_details_2)
    
    # Merge time periods
    addresses_details_last5years = merge_time_periods(addresses_grouped, last_3_to_5_years, last_2_years)
    
    # Final processing
    final_addresses = final_processing(addresses_details_last5years)
    
    # Calculate RFM scores
    final_addresses = calculate_rfm_scores(final_addresses, today)
    
    # Export results
    export_results(final_addresses, kw, land, today)

    
@flow(name="main_flow")
def main_flow():
    for land in LANDS:
        process_land_data(land)

if __name__ == "__main__":
    main_flow()
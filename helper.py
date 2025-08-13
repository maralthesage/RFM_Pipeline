import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from datetime import  date


anrede = {
    "1": "Herrn",
    "2": "Frau",
    "3": "Frau/Herr",
    "4": "Firma",
    "5": "Leer(Firmenadresse)",
    "6": "Fräulein",
    "7": "Familie",
    "X": "Divers",
}


def process_anrede(value):
    value = str(value)
    if value.startswith("0"):  # Remove leading zeros for numeric values
        return value.replace("0", "")
    if value.endswith(".0"):  # Remove '.0' suffix
        return value.replace(".0", "")
    return value  # Return non-numeric values as they are


def assign_age(aa):
    current_date = dt.datetime.now()
    aa["age"] = aa["geburt"].apply(
        lambda x: current_date.year
        - x.year
        - ((current_date.month, current_date.day) < (x.month, x.day))
    )

    # Define age groups
    def assign_age_group(age):
        if age <= 18:
            return "0-18"
        elif age <= 30:
            return "19-30"
        elif age <= 50:
            return "31-50"
        elif age <= 65:
            return "51-65"
        elif age > 65:
            return "65+"
        else:
            return "Keine Angabe"

    aa["age_group"] = aa["age"].apply(assign_age_group)
    return aa


def assign_sources(aa):
    aa["SOURCE"] = ""

    ## Amazon
    aa.loc[(aa["QUELLE"].str[3:] == "921am"), "SOURCE"] = "Amazon"
    ## AWIN
    aa.loc[(aa["QUELLE"].str[3:6] == "929"), "SOURCE"] = "AWIN"
    ## Blätterkatalog
    aa.loc[(aa["QUELLE"].str[3:6] == "938"), "SOURCE"] = "Blätterkatalog"
    ## Corporate Benefits
    aa.loc[(aa["QUELLE"].str[3:6] == "943"), "SOURCE"] = "Corporate Benefits"
    ## Genussmagazin
    aa.loc[(aa["QUELLE"].str.contains(r"936gm|925gm", case=False, regex=True, na=False)),"SOURCE"] = "Genussmagazin"
    ## Google Shopping
    aa.loc[(aa["QUELLE"].str.contains(r"926gs|924gs", case=False, regex=True, na=False)),"SOURCE"] = "Google Shopping"
    ## Internet Import
    aa.loc[(aa["QUELLE"].str.contains(r"20i|INT", case=False, regex=True, na=False)), "SOURCE"] = "Internet Import"
    ## Inventur Trost
    aa.loc[(aa["QUELLE"].str[3:] == "022iv"), "SOURCE"] = "Inventur Trost"
    ## Lionshome
    aa.loc[(aa["QUELLE"].str[3:] == "921lh"), "SOURCE"] = "Lionshome"
    ## Newsletter
    aa.loc[(aa["QUELLE"].str[3:6] == "923"), "SOURCE"] = "Newsletter"
    ## Newsletter Angebot
    aa.loc[(aa["QUELLE"].str[3:] == "923na"), "SOURCE"] = "Newsletter Angebot"
    ## Newsletter Rezept
    aa.loc[(aa["QUELLE"].str[3:] == "923nr"), "SOURCE"] = "Newsletter Rezept"
    ## Newsletter Thema
    aa.loc[(aa["QUELLE"].str[3:] == "923nt"), "SOURCE"] = "Newsletter Thema"
    ## Otto
    aa.loc[(aa["QUELLE"].str[3:] == "921ot"), "SOURCE"] = "Otto"
    ## Google SEA
    aa.loc[(aa["QUELLE"].str[3:6] == "926"), "SOURCE"] = "Google SEA"
    ## SEA Brand
    aa.loc[(aa["QUELLE"].str[3:] == "926br"), "SOURCE"] = "SEA Brand"
    ## SEA Non-Brand
    aa.loc[(aa["QUELLE"].str[3:] == "926sa"), "SOURCE"] = "SEA Non-Brand"
    ## SEO
    aa.loc[(aa["QUELLE"].str[3:6] == "927"), "SOURCE"] = "SEO"
    ## SEO Brand
    aa.loc[(aa["QUELLE"].str[3:] == "927br"), "SOURCE"] = "SEO Brand"
    ## SEO Non-Brand
    aa.loc[(aa["QUELLE"].str[3:] == "927so"), "SOURCE"] = "SEO Non-Brand"
    ## Social Media
    aa.loc[(aa["QUELLE"].str[3:6] == "925"), "SOURCE"] = "Social Media"
    ## Pinterest
    aa.loc[(aa["QUELLE"].str.contains(
                r"925pi|925pt|932aa|pinterest", regex=True, case=False, na=False)),"SOURCE"] = "Pinterest"
    ## Instagram
    aa.loc[
        (aa["QUELLE"].str.contains(r"925ig", regex=True, case=False, na=False)),"SOURCE"] = "Instagram"
    ## Facebook
    aa.loc[
        (aa["QUELLE"].str.contains(r"925fb", regex=True, case=False, na=False)),"SOURCE"] = "Facebook"
    ## Sovendus
    aa.loc[
        (aa["QUELLE"].str.contains(r"928so|sov", regex=True, case=False, na=False)),"SOURCE"] = "Sovendus"

    ## Fremdadressen
    aa.loc[
        (aa["QUELLE"].str[3].isin(["1", "2", "3", "4"])) & (aa["QUELLE"].str[4:6].isin(["01"])),"SOURCE"] = "Fremdadressen"
    ## Katalog und Karte
    aa.loc[
        (aa["QUELLE"].str[3].isin(["1", "2", "3", "4"])) & (aa["QUELLE"].str[4:6].isin(["02", "03", "04"])), "SOURCE"] = "Katalog und Karte"
    ## Beilage
    aa.loc[(aa["QUELLE"].str[3:6].isin(["011", "012", "013"])), "SOURCE"] = "Beilage"
    aa.loc[(aa["QUELLE"].str[3:6].isin(["040"])), "SOURCE"] = "Beilage"
    ## Geburtstagskarte
    aa.loc[(aa["QUELLE"].str[3:6] == "060"), "SOURCE"] = "Geburtstagskarte"
    ## Kataloganforderung
    aa.loc[(aa["QUELLE"].str[3:6] == "000"), "SOURCE"] = "Kataloganforderung"
    ## Freundschaftswerbung
    aa.loc[(aa["QUELLE"].str[3:6] == "030"), "SOURCE"] = "Freundschaftswerbung"
    ## Mailing
    aa.loc[(aa["QUELLE"].str[3:6] == "014"), "SOURCE"] = "Mailing"
    ## Blackweek
    aa.loc[(aa["QUELLE"].str[3:6] == "016"), "SOURCE"] = "Blackweek"
    ## Altcode
    aa.loc[(aa["SOURCE"] == ""), "SOURCE"] = "Altcode"

    aa["ON-OFF"] = ""
    ## SOURCE -> Online/Offline
    aa.loc[
        aa["SOURCE"].isin(
            [
                "Amazon",
                "AWIN",
                "Blätterkatalog",
                "Corporate Benefits",
                "Genussmagazin",
                "Google Shopping",
                "Internet Import",
                "Inventur Trost",
                "Lionshome",
                "Newsletter",
                "Newsletter Angebot",
                "Newsletter Rezept",
                "Newsletter Thema",
                "Otto",
                "Google SEA",
                "SEA Brand",
                "SEA Non-Brand",
                "SEO",
                "SEO Brand",
                "SEO Non-Brand",
                "Social Media",
                "Pinterest",
                "Instagram",
                "Facebook",
                "Sovendus",
            ]
        ),
        "ON-OFF",
    ] = "Online"
    aa.loc[
        aa["SOURCE"].isin(
            [
                "Fremdadressen",
                "Katalog und Karte",
                "Beilage",
                "Geburtstagskarte",
                "Kataloganforderung",
                "Freundschaftswerbung",
                "Mailing",
                "Blackweek",
            ]
        ),
        "ON-OFF",
    ] = "Offline"
    aa.loc[aa["SOURCE"].isin(["Altcode"]), "ON-OFF"] = "Altcode"
    return aa


def is_one_time_buyer(auftrag):
    return auftrag.nunique() == 1


def not_yet_bought(auftrag):
    return len(auftrag) == 0


def seasonal_ostern(dates):
    ostern = {2, 3, 4}
    unique_months = set(dates.dt.month.unique())
    years = dates.dt.year.nunique()
    if years >= 2 and unique_months.issubset(ostern):
        return True
    return False


def seasonal_weihnachten(dates):
    weihnachten = {10, 11, 12}
    unique_months = set(dates.dt.month.unique())
    years = dates.dt.year.nunique()
    if years >= 2 and unique_months.issubset(weihnachten):
        return True
    return False


def pad_column_with_zeros(df, column_name):
    df[column_name] = df[column_name].astype(str)  # Convert to string
    df[column_name] = df[column_name].str.zfill(10)  # Pad with zeros
    return df


def get_halfyear_reference_dates(today=None):
    # Use today's date if none provided
    if today is None:
        today = pd.Timestamp(dt.date.today())
    else:
        today = pd.Timestamp(today)

    # Determine if we're in the first (Jan–Jun) or second (Jul–Dec) half of the year
    if today.month <= 6:
        month_start = 1
    else:
        month_start = 7

    # Calculate half-year reference dates
    five_years_ago_start = pd.Timestamp(year=today.year - 5, month=month_start, day=1)
    two_years_ago_start = pd.Timestamp(year=today.year - 2, month=month_start, day=1)

    return five_years_ago_start, two_years_ago_start, today


def get_halfyear_bins(reference_date=None):
    if reference_date is None:
        reference_date = pd.Timestamp(dt.date.today())
    else:
        reference_date = pd.Timestamp(reference_date)

    # Determine current half-year start
    if reference_date.month <= 6:
        current_halfyear_start = pd.Timestamp(reference_date.year, 1, 1)
    else:
        current_halfyear_start = pd.Timestamp(reference_date.year, 7, 1)

    # Define earliest bin (for recency before 2017-07-01)
    earliest_bin = pd.Timestamp("1900-01-01")
    cutoff_start = pd.Timestamp("2017-07-01")

    # Generate half-year edges from 2017-07-01 to current half-year
    halfyear_starts = []
    current = cutoff_start
    while current <= current_halfyear_start:
        halfyear_starts.append(current)
        current += relativedelta(months=6)

    # Add final upper bound (1 day after reference date)
    bin_edges = (
        [earliest_bin] + halfyear_starts + [reference_date + pd.Timedelta(days=1)]
    )

    # Build score pattern dynamically
    n_halfyear_bins = len(halfyear_starts)  # excludes pre-2017
    score_labels = []

    if n_halfyear_bins <= 9:
        # fallback: assign increasing scores starting at 2, up to 10
        score_labels = list(range(2, 2 + n_halfyear_bins))
    else:
        # Group older bins together based on your logic
        score_labels = [  # dynamic grouping for longer timelines
            2,
            2,  # 15–14
            3,
            3,  # 13–12
            4,
            4,  # 11–10
            5,
            5,  # 9–8
            6,
            6,  # 7–6
            7,
            7,  # 5–4
            8,  # 3
            9,  # 2
            10,  # 1 (most recent half-year)
        ]
        # If we have *more* bins than labels, pad front with 2s (or compress older bins)
        while len(score_labels) < n_halfyear_bins:
            score_labels.insert(0, 2)
        # If we have *fewer* bins than labels, truncate
        score_labels = score_labels[-n_halfyear_bins:]

    # Final labels: one for pre-2017 + dynamic scores
    bin_labels = [1] + score_labels

    return bin_edges, bin_labels


def assign_rfm_label(row):
    mf = row["mf_score"]
    r = row["r_score"]

    if mf in [4, 5] and r in [10, 9]:
        return "Champions"
    elif mf in [4, 5] and r in [5, 6, 7, 8]:
        return "Treue Kunden"
    elif mf == 5 and r in [1, 2, 3, 4]:
        return 'Nicht zu verlieren'
    elif mf in [2, 3, 4] and r in [7, 8, 9, 10]:
        return "Potenziell loyale Kunden"
    elif mf in [3] and r in [5, 6]:
        return 'Brauchen Aufmerksamkeit'
    elif mf in [3, 4] and r in [1, 2, 3, 4]:
        return "Gefährdete Kunden"
    elif mf == 1 and r in [9, 10]:
        return "Reaktivierte Kunden"
    elif mf == 1 and r in [7, 8]:
        return "Vielversprechende Kunden"
    elif mf in [1, 2] and r in [5, 6]:
        return "Abwandernde Kunden"
    elif mf == 2 and r in [1, 2, 3, 4]:
        return "Schlafende Kunden"
    elif mf == 1 and r in [1, 2, 3, 4]:
        return "Verlorene Kunden"
    else:
        return "Nicht klassifiziert"







def process_id(data):
    data = data.astype(str)
    data = data.replace(".0","")
    data = data.str.zfill(10)
    return data

def process_date(data):
    return pd.to_datetime(data,format='mixed',errors='coerce')



def get_half_year_info(today: date = None, land: str = 'DE'):
    if today is None:
        today = date.today()
    
    # Base reference: H1 2025 -> number 49
    base_half_year_start = date(2025, 1, 1)
    if land == 'DE':
        base_number = 49
    elif land == 'AT':
        base_number = 37
    elif land == 'FR':
        base_number = 28
    elif land == 'CH':
        base_number = 35

    # Determine if we're in H1 or H2
    if today.month <= 6:
        # H1 of current year
        current_half_index = 0
    else:
        # H2 of current year
        current_half_index = 1

    # Total number of half years since base
    years_diff = today.year - base_half_year_start.year
    half_year_offset = years_diff * 2 + current_half_index

    # Calculated number
    number = base_number + half_year_offset

    # Determine previous half-year
    if current_half_index == 0:
        # If we're in H1, previous half-year is H2 of last year
        prev_start = date(today.year - 1, 7, 1)
        prev_end = date(today.year - 1, 12, 31)
    else:
        # If we're in H2, previous half-year is H1 of same year
        prev_start = date(today.year, 1, 1)
        prev_end = date(today.year, 6, 30)

    return {
        'number': number,
        'prev_start': prev_start,
        'prev_end': prev_end
    }


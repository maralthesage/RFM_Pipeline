import pandas as pd
import os
import datetime as dt
from dateutil.relativedelta import relativedelta
from helper import *
from paths import *

## Repetitive setting
enc = "cp850"
sep = ";"


### Defining the dates for the beginning and end of previous and Current HJ, as well as the Number of the Column in KW data
result = get_half_year_info()
last_hj = f"Z{result['number']}"
current_hj = f"Z{result['number'] + 1}"
prev_start = pd.to_datetime(result["prev_start"])
prev_end = pd.to_datetime(result["prev_end"])
current_start = pd.to_datetime(result["prev_start"] + relativedelta(months=6))
current_end = pd.to_datetime(result["prev_end"] + relativedelta(months=6))


## Importing all required data
kunden_segments = pd.read_excel(ks_path)
kunden_segment_dict = dict(zip(kunden_segments["Alt"], kunden_segments["Neu"]))
kw_f01 = pd.read_csv(kw_path_f01, sep=sep, encoding=enc, on_bad_lines="skip")
kw_f02 = pd.read_csv(kw_path_f02, sep=sep, encoding=enc, on_bad_lines="skip")
kw_f03 = pd.read_csv(kw_path_f03, sep=sep, encoding=enc, on_bad_lines="skip")
kw_f04 = pd.read_csv(kw_path_f04, sep=sep, encoding=enc, on_bad_lines="skip")
adresse_f01 = pd.read_csv(address_path_f01, sep=sep, encoding=enc)
adresse_f02 = pd.read_csv(address_path_f02, sep=sep, encoding=enc)
adresse_f03 = pd.read_csv(address_path_f03, sep=sep, encoding=enc)
adresse_f04 = pd.read_csv(address_path_f04, sep=sep, encoding=enc)
stat_f01 = pd.read_csv(stat_path_f01, sep=sep, encoding=enc, usecols=["NUMMER", "ERSTKAUF"])
stat_f02 = pd.read_csv(stat_path_f02, sep=sep, encoding=enc, usecols=["NUMMER", "ERSTKAUF"])
stat_f03 = pd.read_csv(stat_path_f03, sep=sep, encoding=enc, usecols=["NUMMER", "ERSTKAUF"])
stat_f04 = pd.read_csv(stat_path_f04, sep=sep, encoding=enc, usecols=["NUMMER", "ERSTKAUF"])

kw = pd.concat([kw_f01, kw_f02, kw_f03, kw_f04], ignore_index=True)
adresse = pd.concat([adresse_f01, adresse_f02, adresse_f03, adresse_f04], ignore_index=True)
stat = pd.concat([stat_f01, stat_f02, stat_f03, stat_f04], ignore_index=True)


## mapping the names to the codes in the columns related to last HJ and current HJ in the KW data
kw[last_hj] = kw[last_hj].map(kunden_segment_dict)
kw[current_hj] = kw[current_hj].map(kunden_segment_dict)

## Data preprocessing to connect the clean tables together
kw["NUMMER"] = process_id(kw["NUMMER"])
adresse["NUMMER"] = process_id(adresse["NUMMER"])
adresse["SYS_ANLAGE"] = process_date(adresse["SYS_ANLAGE"])
stat["NUMMER"] = process_id(stat["NUMMER"])
stat["ERSTKAUF"] = process_date(stat["ERSTKAUF"])


### Merging tables to each other using the customer ID (NUMMER) column
address_kw = pd.merge(adresse, kw[["NUMMER", last_hj]], on="NUMMER", how="left")
address_kw = pd.merge(address_kw, stat, on="NUMMER", how="left")

### Cleaning up the df table, renaming the KW HJ column to Kundengruppe and removing duplicates from the data
address_kw = address_kw[["NUMMER", "SYS_ANLAGE", last_hj, "ERSTKAUF"]]
address_kw = address_kw.rename(columns={last_hj: "Kundengruppe"})
address_kw = address_kw.sort_values(by='Kundengruppe', na_position='last')
address_kw = address_kw.drop_duplicates(subset="NUMMER",keep='first')

### Defining interessenten customers, those who are in the system but have no orders
address_kw.loc[(address_kw["ERSTKAUF"].isna()), "Kundengruppe"] = "Interessenten"
address_kw = address_kw[address_kw["SYS_ANLAGE"] <= prev_end]


nk_adresse = adresse.copy()
nk_adresse = nk_adresse[nk_adresse["SYS_ANLAGE"] >= current_start]

## Connecting all data
nk_kw = pd.merge(nk_adresse, kw[["NUMMER", current_hj]], on="NUMMER", how="left")
nk_kw = pd.merge(nk_kw, stat, on="NUMMER", how="left")


### Cleaning up the df table, renaming the KW HJ column to Kundengruppe and removing duplicates from the data

nk_kw = nk_kw[["NUMMER", "SYS_ANLAGE", current_hj, "ERSTKAUF"]]
nk_kw = nk_kw.rename(columns={current_hj: "Kundengruppe"})
nk_kw = nk_kw.sort_values(by='Kundengruppe', na_position='last')
nk_kw = nk_kw.drop_duplicates(subset="NUMMER",keep='first')

## Those with ERSTKAUF datum are neukunden-1 those without it are Interessenten
nk_kw.loc[(nk_kw["ERSTKAUF"].isna()), "Kundengruppe"] = "Interessenten"
nk_kw.loc[(nk_kw["ERSTKAUF"].notna()), "Kundengruppe"] = "Neukunden-1"


## Concatenating the last hj customers with current hj customers
address_kw_nk_kw = pd.concat([address_kw, nk_kw])
address_kw_nk_kw = address_kw_nk_kw.sort_values(by='Kundengruppe', na_position='last')
address_kw_nk_kw = address_kw_nk_kw.drop_duplicates(subset="NUMMER",keep='first')
## Expanding the Neukunden-1 to those who are Interessenten but have an ERSTKAUF in the current hj
address_kw_nk_kw.loc[
    (address_kw_nk_kw["SYS_ANLAGE"] <= prev_end)
    & (address_kw_nk_kw["Kundengruppe"] == "Interessenten")
    & (address_kw_nk_kw["ERSTKAUF"] >= current_start),
    "Kundengruppe",
] = "Neukunden-1"


## Remove their Rechnungs data, and once again merge them with the rechnungs data so they don't have missing values
nk = address_kw_nk_kw[address_kw_nk_kw["Kundengruppe"] == "Neukunden-1"][
    [
        "NUMMER",
        "Kundengruppe",
    ]
]


## removing this group from the final_Df and then adding the freshly created neukunden to the final_df_list
address_kw_nk_kw = address_kw_nk_kw[address_kw_nk_kw["Kundengruppe"] != "Neukunden-1"]
all_addresses_labeled = pd.concat([address_kw_nk_kw, nk])
## Removing duplicated Nummers
all_addresses_labeled = all_addresses_labeled.sort_values(by='Kundengruppe', na_position='last')

all_addresses_labeled = all_addresses_labeled.drop_duplicates(subset=["NUMMER"],keep='first')

all_addresses_labeled[["NUMMER", "Kundengruppe"]].to_csv(
    f"Data/kw.csv", sep=";", index=False, encoding="cp850"
)

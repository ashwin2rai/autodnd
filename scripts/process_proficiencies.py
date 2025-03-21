import pandas as pd
import json

from autodnd.utils.project_root import get_project_root
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"


# column names from the original allignments json file
url_col_str = "url"
index_col_str = "index"
reference_index_col_str = "reference_index"
reference_url_col_str = "reference_url"
classes_col_str = "classes"
classes_col_name_key_str = "name"
races_col_str = "races"
races_col_name_key_str = "name"

# String used to fill N/A values
# This should be the same used in the EQUIPMENT dataset as well
unknown_value_fill_string = "Unknown or Not Applicable"

if __name__ == "__main__":
    ## ---- Load Necessary Data First ----###

    # load the EQUIPMENT_CATEGORIES data
    try:
        eqc = pd.read_parquet(
            save_folder / f"{RuleSet.EQUIPMENT_CATEGORIES.value}.parquet"
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The EQUIPMENT_CATEGORIES parquet file is missing. You need to run the process_{RuleSet.EQUIPMENT_CATEGORIES.value} script first to generate the dataset."
        )

    # load the EQUIPMENT data
    try:
        eq = pd.read_parquet(save_folder / f"{RuleSet.EQUIPMENT.value}.parquet")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The EQUIPMENT parquet file is missing. You need to run the process_{RuleSet.EQUIPMENT.value} script first to generate the dataset."
        )

    # load the ABILITY_SCORES data
    try:
        asc = pd.read_parquet(save_folder / f"{RuleSet.ABILITY_SCORES.value}.parquet")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The ABILITY_SCORES parquet file is missing. You need to run the process_{RuleSet.ABILITY_SCORES.value} script first to generate the dataset."
        )

    # load the ABILITY_SCORES data
    try:
        sk = pd.read_parquet(save_folder / f"{RuleSet.SKILLS.value}.parquet")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The SKILLS parquet file is missing. You need to run the process_{RuleSet.SKILLS.value} script first to generate the dataset."
        )

    ## ---- Load Proficiency Data ----###

    # filename string
    ruleset = RuleSet.PROFICIENCIES.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Check if the JSON data has the expected schema
    assert set(df.columns.to_list()) == set(
        [
            "index",
            "type",
            "name",
            "classes",
            "races",
            "url",
            "reference_index",
            "reference_name",
            "reference_url",
        ]
    ), "Unexpected data organization, schema has probably changed"

    # Dropping unnecessary columns
    df = df.drop([index_col_str, url_col_str, reference_index_col_str], axis=1)

    # Creating a new column with the reference type, extracte from the reference url
    df = df.assign(
        reference_type=df[reference_url_col_str].str.split("/").apply(lambda x: x[3])
    )

    # The references are spread between 4 other datasets. Extracting the necessary reference data and
    # adding it to this table in a new 'reference' column so that this table becomes self-contained
    reference_list = []

    for i in df[reference_url_col_str]:
        category = i.split("/")[3]
        if category == "equipment":
            eq_ref = (
                eq[eq["url"] == i]
                .iloc[0]
                .replace(unknown_value_fill_string, None)
                .dropna()
                .drop("index")
                .to_dict()
            )
            reference_list.append(eq_ref)
        elif category == "equipment-categories":
            eqc_ref = eqc[eqc["url"] == i].iloc[0]["equipment_category"]
            reference_list.append(eqc_ref)
        elif category == "ability-scores":
            asc_ref = asc[asc["url"] == i].iloc[0]["desc"]
            reference_list.append(asc_ref)
        elif category == "skills":
            sk_ref = sk[sk["url"] == i].iloc[0]["desc"]
            reference_list.append(sk_ref)
        else:
            reference_list.append("")

    df["reference"] = reference_list

    # Extracting only the necessary information from the races and classes columns (which is just the name)
    df[classes_col_str] = df[classes_col_str].apply(
        lambda x: [i[classes_col_name_key_str] for i in x]
    )
    df[races_col_str] = df[races_col_str].apply(
        lambda x: [i[races_col_name_key_str] for i in x]
    )

    # Exploding the races and classes columns, droping more unecessary columns and filling in NaNs
    df = (
        df.drop(reference_url_col_str, axis=1)
        .explode(classes_col_str)
        .explode(races_col_str)
        .fillna(unknown_value_fill_string)
        .reset_index(drop=True)
        .astype(str)
    )

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(
        [
            "type",
            "name",
            "classes",
            "races",
            "reference_name",
            "reference_type",
            "reference",
        ]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(save_folder / f"{ruleset}.parquet")

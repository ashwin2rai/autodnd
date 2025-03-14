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
desc_col_str = "desc"
ability_score_url_col_str = "ability_score_url"
ability_score_index_col_str = "ability_score_index"
ability_score_name_col = "ability_score_name"

if __name__ == "__main__":
    # load the abilities string
    abilities = RuleSet.ABILITY_SCORES.value

    # load the abilities data
    try:
        abilities_df = pd.read_parquet(save_folder / f"{abilities}.parquet")[
            ["ability_name", "desc", "full_name"]
        ].rename(
            columns={
                "ability_name": ability_score_name_col,
                "desc": "ability_desc",
                "full_name": "ability_full_name",
            }
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The abilities parquet file is missing. You need to run the process_{abilities} script first to generate the dataset."
        )

    # filename string
    ruleset = RuleSet.SKILLS.value

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
            "name",
            "desc",
            "url",
            "ability_score_index",
            "ability_score_name",
            "ability_score_url",
        ]
    ), "Unexpected data organization, schema has probably changed"

    # Dropping unnecessary columns
    df = df.drop(
        [
            url_col_str,
            index_col_str,
            ability_score_url_col_str,
            ability_score_index_col_str,
        ],
        axis=1,
    )
    df = (
        df.assign(temp=lambda x: x[desc_col_str].apply(lambda y: "".join(y)))
        .drop(desc_col_str, axis=1)
        .rename(columns={"temp": desc_col_str})
    )

    output_df = pd.merge(df, abilities_df, on=ability_score_name_col, how="left")

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(save_folder / f"{ruleset}.parquet")

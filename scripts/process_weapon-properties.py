import pandas as pd
import json

from autodnd.utils.project_root import get_project_root
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"


# column names from the original allignments json file
index_col_str = "index"
desc_col_str = "desc"

if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.WEAPON_PROPERTIES.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Check if the JSON data has the expected schema
    assert set(df.columns.to_list()) == set(["index", "name", "desc", "url"]), (
        "Unexpected data organization, schema has probably changed"
    )

    # Dropping unnecessary columns
    df = df.drop([index_col_str], axis=1)

    # cleaning up the desc column
    df[desc_col_str] = df[desc_col_str].apply(lambda x: " ".join(x))

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(["name", "desc", "url"]), (
        "Unexpected column names, schema has probably changed"
    )

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(save_folder / f"{ruleset}.parquet")

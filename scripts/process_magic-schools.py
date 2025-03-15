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

if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.MAGIC_SCHOOLS.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Dropping unnecessary columns
    df = df.drop([url_col_str, index_col_str], axis=1)

    # Check if the JSON data has the expected schema
    assert set(df.columns.to_list()) == set(["name", "desc"]), (
        "Unexpected data organization, schema has probably changed"
    )

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(["name", "desc"]), (
        "Unexpected column names, schema has probably changed"
    )

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(save_folder / f"{ruleset}.parquet")

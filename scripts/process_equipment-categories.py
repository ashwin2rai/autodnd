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
equipment_col_str = "equipment"
name_col_str = "name"

if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.EQUIPMENT_CATEGORIES.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Check if the JSON data has the expected schema
    assert set(df.columns.to_list()) == set(["index", "name", "equipment", "url"]), (
        "Unexpected data organization, schema has probably changed"
    )

    # Dropping unnecessary columns
    df = df.drop([index_col_str], axis=1)

    # Explode the equipment column since it's a list at the moment
    df = df.explode(equipment_col_str).reset_index(drop=True)

    # Extract only the weapon name from the equipment column and rename equipment category column name
    output_df = (
        df.assign(
            equipment_name=lambda x: x[equipment_col_str].apply(lambda y: y["name"])
        )
        .rename(columns={name_col_str: "equipment_category"})
        .drop(equipment_col_str, axis=1)
    )

    # Check if the output data has the expected schema before saving
    assert set(output_df.columns.to_list()) == set(
        ["equipment_category", "url", "equipment_name"]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(save_folder / f"{ruleset}.parquet")

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
desc_col_str = "desc"
index_col_str = "index"
variants_col_str = "variants"
equipment_category_index_col_str = "equipment_category_index"
equipment_category_url_col_str = "equipment_category_url"
variants_row_name_key_str = "name"

if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.MAGIC_ITEMS.value

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
            "variants",
            "variant",
            "desc",
            "image",
            "url",
            "equipment_category_index",
            "equipment_category_name",
            "equipment_category_url",
            "rarity_name",
        ]
    ), "Unexpected data organization, schema has probably changed"

    # Dropping unnecessary columns
    df = df.drop(
        [
            url_col_str,
            index_col_str,
            equipment_category_index_col_str,
            equipment_category_url_col_str,
        ],
        axis=1,
    )

    # Concatenating description strings into a single string.
    df[desc_col_str] = df[desc_col_str].apply(lambda x: ".".join(x))

    # Finding the names of the variants if they exist and concatenating them together into a single string
    df[variants_col_str] = [
        ", ".join([j[variants_row_name_key_str] if len(i) > 0 else "" for j in i])
        for i in df[variants_col_str]
    ]

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(
        [
            "name",
            "variants",
            "variant",
            "desc",
            "image",
            "equipment_category_name",
            "rarity_name",
        ]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(save_folder / f"{ruleset}.parquet")

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
typical_speakers_col_str = "typical_speakers"

if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.LANGUAGES.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Drop unnecessary columns and join the list elements inside the typical_speakers column
    df = (
        df.drop([url_col_str, index_col_str], axis=1)
        .assign(
            temp=lambda x: x[typical_speakers_col_str].apply(lambda y: ", ".join(y))
        )
        .drop(typical_speakers_col_str, axis=1)
        .rename(columns={"temp": typical_speakers_col_str})
    )

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(
        ["name", "type", "script", "desc", "typical_speakers"]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(save_folder / f"{ruleset}.parquet")

import pandas as pd
import json
from typing import Any

from autodnd.utils.project_root import get_project_root
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"


# column names from the original allignments json file
url_col_str = "url"
index_col_str = "index"
class_index_col_str = "class_index"
class_url_col_str = "class_url"
desc_col_str = "desc"
spells_col_str = "spells"


# String used to fill NaN values
unknown_value_fill_string = "Unknown or Not Applicable"


def spell_prereq_mapping(prereq_dict: dict[str, Any]) -> dict[str, str]:
    """Mapping function to map complex spell preq format to a simple one."""

    prereq_string = ""
    assert set(prereq_dict.keys()) == set(["prerequisites", "spell"]), (
        "Spell prerequisites dictionary requires a prerequisite information and a spell "
    )
    for i in prereq_dict["prerequisites"]:
        if i["type"] == "level":
            assert "/classes/" in i["url"], (
                f"Expected Class prerequisite but did not find it: {i['url']}"
            )
            class_name = i["url"].split("classes/")[1].split("/")[0]
            prereq_string += f" This spell requires a minimum of {class_name} level {i['url'].split('levels/')[-1]}."
        elif i["type"] == "feature":
            prereq_string += (
                f" This spell requires also the following feature: {i['name']}"
            )
        else:
            raise ValueError(f"Expected feature or level prereq, found {i['type']}")

    return {
        "name": prereq_dict["spell"]["name"],
        "prerequisites": prereq_string,
        "url": prereq_dict["spell"]["url"],
    }


# TODO Add Joing to Levels and Possibly Spell details
if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.SUBCLASSES.value

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
            "subclass_flavor",
            "desc",
            "subclass_levels",
            "url",
            "class_index",
            "class_name",
            "class_url",
            "spells",
        ]
    ), "Unexpected data organization, schema has probably changed"

    # Dropping unnecessary columns
    df = df.drop(
        [url_col_str, index_col_str, class_index_col_str, class_url_col_str], axis=1
    )

    # Concanetating the strings in the description field
    df = df.assign(desc=df[desc_col_str].apply(lambda x: ", ".join(x)))

    df[spells_col_str] = df[spells_col_str].apply(
        lambda x: [spell_prereq_mapping(i) for i in x] if isinstance(x, list) else x
    )

    output_df = df.fillna(unknown_value_fill_string).astype(str)

    # Check if the output data has the expected schema before saving
    assert set(output_df.columns.to_list()) == set(
        ["name", "subclass_flavor", "desc", "subclass_levels", "class_name", "spells"]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(save_folder / f"{ruleset}.parquet")

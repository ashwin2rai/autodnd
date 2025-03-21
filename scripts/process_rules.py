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
subsections_col_str = "subsections"
subsections_col_url_key_str = "url"
rule_sections_url_col_str = "url"
rule_sections_as_instruction_col_str = "rule_section_as_instructions"
rule_sections_name_col_str = "name"

if __name__ == "__main__":
    # load the damage_type data
    try:
        rulesections_df = pd.read_parquet(
            save_folder / f"{RuleSet.RULE_SECTIONS.value}.parquet"
        )[
            [
                rule_sections_name_col_str,
                rule_sections_url_col_str,
                rule_sections_as_instruction_col_str,
            ]
        ].rename(columns={rule_sections_name_col_str: "rule_section_name"})
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The rule sections parquet file is missing. You need to run the process_{RuleSet.RULE_SECTIONS.value} script first to generate the dataset."
        )

    # filename string
    ruleset = RuleSet.RULES.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(
        ["name", "index", "desc", "subsections", "url"]
    ), "Unexpected column names, schema has probably changed"

    # Dropping unnecessary columns
    df = df.drop([index_col_str, url_col_str], axis=1)

    df = (
        df.explode(subsections_col_str)
        .assign(
            temp=lambda x: x[subsections_col_str].apply(
                lambda y: y[subsections_col_url_key_str]
            )
        )
        .drop(subsections_col_str, axis=1)
    )

    df = pd.merge(
        df,
        rulesections_df,
        left_on="temp",
        right_on=rule_sections_url_col_str,
        how="left",
    ).drop(["temp", rule_sections_url_col_str], axis=1)

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(
        ["name", "desc", "rule_section_name", "rule_section_as_instructions"]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(save_folder / f"{ruleset}.parquet")

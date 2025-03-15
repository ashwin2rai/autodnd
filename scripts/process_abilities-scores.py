import pandas as pd
import json

from autodnd.utils.project_root import get_project_root
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"

# What string to use to fill NA values
na_string = "none"


# column names from the original ability scores json file
description_col_str = "desc"
original_index_col_str = "index"
skills_col_str = "skills"
abilities_col_str = "name"
skills_key_name = "name"
urls_col_str = "url"

if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.ABILITY_SCORES.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Check if the JSON data has the expected schema
    assert set(df.columns.to_list()) == set(
        ["index", "name", "full_name", "desc", "skills", "url"]
    ), "Unexpected data organization, schema has probably changed"

    # Concatenate the description columns and drop the index column
    df = df.assign(
        desc=lambda temp: temp[description_col_str].apply(lambda x: "".join(x))
    ).drop(original_index_col_str, axis=1)

    # Explode the skills column since each record is a dictionary
    exploded_skills = (
        df[[skills_col_str, abilities_col_str]]
        .explode(skills_col_str)
        .rename(columns={abilities_col_str: "ab_name"})
    )

    # Group all the skill descriptions by ability and prep the dataset for joining with the ability table
    skills = (
        pd.concat(
            [
                pd.json_normalize(exploded_skills[skills_col_str].to_list())[
                    skills_key_name
                ].add(", "),
                exploded_skills.ab_name.reset_index(drop=True),
            ],
            axis=1,
        )
        .fillna(na_string)
        .groupby("ab_name")
        .sum()
        .rename(columns={skills_key_name: "skill_names"})
    )

    # Join the ability table with the newly grouped abilities table
    output_df = (
        pd.merge(df, skills, how="left", left_on=abilities_col_str, right_on="ab_name")
        .drop([skills_col_str, urls_col_str], axis=1)
        .fillna(na_string)
        .rename(columns={abilities_col_str: "ability_name"})
    )

    # Check if the output data has the expected schema before saving
    assert set(output_df.columns.to_list()) == set(
        ["ability_name", "full_name", "desc", "skill_names"]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(save_folder / f"{ruleset}.parquet")

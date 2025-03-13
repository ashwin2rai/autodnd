from autodnd.utils.project_root import get_project_root
import pandas as pd
import json

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# filename string
ruleset = "Ability-Scores"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"


# column names from the original ability scores json file
description_col_str = "desc"
original_index_col_str = "index"
skills_col_str = "skills"
abilities_col_str = "name"
skills_key_name = "name"
urls_col_str = "url"

if __name__ == "__main__":
    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")
    assert set(df.columns.to_list()) == set(
        ["index", "name", "full_name", "desc", "skills", "url"]
    ), "Unexpected data organization, schema has probably changed"
    df = df.assign(
        desc=lambda temp: temp[description_col_str].apply(lambda x: "".join(x))
    ).drop(original_index_col_str, axis=1)

    exploded_skills = (
        df[[skills_col_str, abilities_col_str]]
        .explode(skills_col_str)
        .rename(columns={abilities_col_str: "ab_name"})
    )
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
        .groupby("ab_name")
        .sum()
        .rename(columns={skills_key_name: "skill_names"})
    )
    output_df = (
        pd.merge(df, skills, how="left", left_on=abilities_col_str, right_on="ab_name")
        .drop([skills_col_str, urls_col_str], axis=1)
        .apply(lambda x: x.str.lower())
        .fillna("none")
        .rename(columns={abilities_col_str: "ability_name"})
    )

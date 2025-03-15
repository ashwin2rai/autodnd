import pandas as pd
import json

from autodnd.utils.project_root import get_project_root
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"

# String used to fill N/A values
unknown_value_fill_string = "Unknown or Not Applicable"

# column names from the original allignments json file
url_col_str = "url"
properties_col_str = "properties"
properties_col_name_key_str = "name"
equipment_category_index_str = "equipment_category_index"
equipment_category_url_str = "equipment_category_url"
damage_damage_type_index_str = "damage_damage_type_index"
damage_damage_type_url_str = "damage_damage_type_url"

if __name__ == "__main__":
    ## ---- Load Necessary Data First ----###

    # damage_type filename string
    damage_type = RuleSet.DAMAGE_TYPES.value

    # load the damage_type data
    try:
        damage_type_df = pd.read_parquet(save_folder / f"{damage_type}.parquet").rename(
            columns={"desc": "damage_type_desc", "name": "damage_type_name_temp"}
        )[["damage_type_name_temp", "damage_type_desc"]]
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The damage_type parquet file is missing. You need to run the process_{damage_type} script first to generate the dataset."
        )

    # damage_type filename string
    weapon_props = RuleSet.WEAPON_PROPERTIES.value

    # load the damage_type data
    try:
        weapon_props_df = pd.read_parquet(save_folder / f"{weapon_props}.parquet")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The weapon_props parquet file is missing. You need to run the process_{weapon_props} script first to generate the dataset."
        )

    ## --- Load the Equipment JSON File and work on it --- #

    # filename string
    ruleset = RuleSet.EQUIPMENT.value

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
            "weapon_category",
            "weapon_range",
            "category_range",
            "weight",
            "properties",
            "url",
            "equipment_category_index",
            "equipment_category_name",
            "equipment_category_url",
            "cost_quantity",
            "cost_unit",
            "damage_damage_dice",
            "damage_damage_type_index",
            "damage_damage_type_name",
            "damage_damage_type_url",
            "range_normal",
            "throw_range_normal",
            "throw_range_long",
            "two_handed_damage_damage_dice",
            "two_handed_damage_damage_type_index",
            "two_handed_damage_damage_type_name",
            "two_handed_damage_damage_type_url",
            "range_long",
            "special",
            "armor_category",
            "str_minimum",
            "stealth_disadvantage",
            "armor_class_base",
            "armor_class_dex_bonus",
            "armor_class_max_bonus",
            "gear_category_index",
            "gear_category_name",
            "gear_category_url",
            "desc",
            "quantity",
            "contents",
            "tool_category",
            "vehicle_category",
            "capacity",
            "speed_quantity",
            "speed_unit",
        ]
    ), "Unexpected data organization, schema has probably changed"

    # Dropping unnecessary columns
    df = df.drop(
        [
            url_col_str,
            equipment_category_index_str,
            equipment_category_url_str,
            damage_damage_type_index_str,
            damage_damage_type_url_str,
        ],
        axis=1,
    )

    # Filling N/A values
    df = df.fillna(unknown_value_fill_string)

    # Looping though the content in the equipment properties column, extracting the name of the property and adding the property description
    # from the weapon_props_df data
    properties_vector = []
    for i in df[properties_col_str]:
        if unknown_value_fill_string not in i:
            properties_vector.append(
                ", ".join(
                    [
                        f"Equipment property - [{j[properties_col_name_key_str]}]: {weapon_props_df[weapon_props_df.name == j[properties_col_name_key_str]].desc.iloc[0]}"
                        for j in i
                    ]
                )
            )
        else:
            properties_vector.append(unknown_value_fill_string)

    # Replacing the original properties column with this processed one
    df[properties_col_str] = properties_vector

    # Joining the damage descriptions from the damage_type_df data
    output_df = (
        pd.merge(
            df,
            damage_type_df,
            how="left",
            left_on="damage_damage_type_name",
            right_on="damage_type_name_temp",
        )
        .drop("damage_type_name_temp", axis=1)
        .fillna(unknown_value_fill_string)
        .astype(str)
    )

    # Check if the output data has the expected schema before saving
    assert set(output_df.columns.to_list()) == set(
        [
            "index",
            "name",
            "weapon_category",
            "weapon_range",
            "category_range",
            "weight",
            "properties",
            "equipment_category_name",
            "cost_quantity",
            "cost_unit",
            "damage_damage_dice",
            "damage_damage_type_name",
            "range_normal",
            "throw_range_normal",
            "throw_range_long",
            "two_handed_damage_damage_dice",
            "two_handed_damage_damage_type_index",
            "two_handed_damage_damage_type_name",
            "two_handed_damage_damage_type_url",
            "range_long",
            "special",
            "armor_category",
            "str_minimum",
            "stealth_disadvantage",
            "armor_class_base",
            "armor_class_dex_bonus",
            "armor_class_max_bonus",
            "gear_category_index",
            "gear_category_name",
            "gear_category_url",
            "desc",
            "quantity",
            "contents",
            "tool_category",
            "vehicle_category",
            "capacity",
            "speed_quantity",
            "speed_unit",
            "damage_type_desc",
        ]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(save_folder / f"{ruleset}.parquet")

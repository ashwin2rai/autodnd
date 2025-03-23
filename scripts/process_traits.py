import pandas as pd
import json
import ast
import math
from typing import Any, NamedTuple

from autodnd.utils.project_root import get_project_root
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"


# column names from the original allignments json file
url_col_str = "url"
index_col_str = "index"
subraces_col_str = "subraces"
subrances_col_name_key_str = "name"
races_col_str = "races"
races_col_str_name_key_str = "name"
desc_col_str = "desc"
proficiencies_col_str = "proficiencies"
draconic_specific_trait_cols = [
    "trait_specific_damage_type_name",
    "trait_specific_breath_weapon_name",
    "trait_specific_breath_weapon_desc",
    "trait_specific_breath_weapon_area_of_effect_size",
    "trait_specific_breath_weapon_area_of_effect_type",
    "trait_specific_breath_weapon_usage_type",
    "trait_specific_breath_weapon_usage_times",
    "trait_specific_breath_weapon_dc_dc_type_name",
    "trait_specific_breath_weapon_dc_success_type",
    "trait_specific_breath_weapon_damage",
]

proficiency_choices_choose_col_str = "proficiency_choices_choose"
unnecessary_cols = [
    "proficiency_choices_choose",
    "proficiency_choices_type",
    "proficiency_choices_from_option_set_type",
    "proficiency_choices_from_options",
    "trait_specific_spell_options_choose",
    "trait_specific_spell_options_from_option_set_type",
    "trait_specific_spell_options_from_options",
    "trait_specific_spell_options_type",
    "language_options_choose",
    "language_options_type",
    "language_options_from_option_set_type",
    "language_options_from_options",
    "trait_specific_subtrait_options_choose",
    "trait_specific_subtrait_options_from_option_set_type",
    "trait_specific_subtrait_options_from_options",
    "trait_specific_subtrait_options_type",
    "parent_index",
    "parent_name",
    "parent_url",
    "trait_specific_damage_type_index",
    "trait_specific_damage_type_name",
    "trait_specific_damage_type_url",
    "trait_specific_breath_weapon_name",
    "trait_specific_breath_weapon_desc",
    "trait_specific_breath_weapon_area_of_effect_size",
    "trait_specific_breath_weapon_area_of_effect_type",
    "trait_specific_breath_weapon_usage_type",
    "trait_specific_breath_weapon_usage_times",
    "trait_specific_breath_weapon_dc_dc_type_index",
    "trait_specific_breath_weapon_dc_dc_type_name",
    "trait_specific_breath_weapon_dc_dc_type_url",
    "trait_specific_breath_weapon_dc_success_type",
    "trait_specific_breath_weapon_damage",
]

trait_specific_breath_weapon_damage_col_str = "trait_specific_breath_weapon_damage"
trait_specific_breath_weapon_damage_col_str_damage_at_character_level_key = (
    "damage_at_character_level"
)


class DfIterTupleNames(NamedTuple):
    """To keep MyPy happy. Type hinting the typle names for later itertuples use."""

    Index: int
    races: str
    subraces: str
    name: str
    desc: str
    proficiencies: str
    url: str
    proficiency_choices_choose: float
    proficiency_choices_type: str
    proficiency_choices_from_option_set_type: str
    proficiency_choices_from_options: dict[Any, Any]
    trait_specific_spell_options_choose: float
    trait_specific_spell_options_from_option_set_type: str
    trait_specific_spell_options_from_options: dict[Any, Any]
    trait_specific_spell_options_type: str
    language_options_choose: float
    language_options_type: str
    language_options_from_option_set_type: str
    language_options_from_options: dict[Any, Any]
    trait_specific_subtrait_options_choose: float
    trait_specific_subtrait_options_from_option_set_type: str
    trait_specific_subtrait_options_from_options: dict[Any, Any]
    trait_specific_subtrait_options_type: str
    parent_index: str
    parent_name: str
    parent_url: str
    trait_specific_damage_type_index: str
    trait_specific_damage_type_name: str
    trait_specific_damage_type_url: str
    trait_specific_breath_weapon_name: str
    trait_specific_breath_weapon_desc: str
    trait_specific_breath_weapon_area_of_effect_size: float
    trait_specific_breath_weapon_area_of_effect_type: str
    trait_specific_breath_weapon_usage_type: str
    trait_specific_breath_weapon_usage_times: float
    trait_specific_breath_weapon_dc_dc_type_index: str
    trait_specific_breath_weapon_dc_dc_type_name: str
    trait_specific_breath_weapon_dc_dc_type_url: str
    trait_specific_breath_weapon_dc_success_type: str
    trait_specific_breath_weapon_damage: str


if __name__ == "__main__":
    # load the PROFICIENCIES data
    try:
        prof_df = pd.read_parquet(
            save_folder / f"{RuleSet.PROFICIENCIES.value}.parquet"
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"The PROFICIENCIES parquet file is missing. You need to run the process_{RuleSet.PROFICIENCIES.value} script first to generate the dataset."
        )

    # ---- Defining a helper function to join data from the proficiencies dataset --- #
    def get_proficiency_reference(prof_url: str) -> dict[str, str]:
        """Simple function to map profieciency url to the reference details."""
        ref_value = prof_df[prof_df.url == prof_url].reference.values[0]
        # Checking if the reference is a dictionary
        try:
            return ast.literal_eval(ref_value)
        except SyntaxError:
            # If the reference isn't a dictionary, then it's a string. Turn it into a dict before sending it.
            ref_name = prof_df[prof_df.url == prof_url].name.values[0]
            return {"name": ref_name, "properties": ref_value}

    # filename string
    ruleset = RuleSet.TRAITS.value

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
            "races",
            "subraces",
            "name",
            "desc",
            "proficiencies",
            "url",
            "proficiency_choices_choose",
            "proficiency_choices_type",
            "proficiency_choices_from_option_set_type",
            "proficiency_choices_from_options",
            "trait_specific_spell_options_choose",
            "trait_specific_spell_options_from_option_set_type",
            "trait_specific_spell_options_from_options",
            "trait_specific_spell_options_type",
            "language_options_choose",
            "language_options_type",
            "language_options_from_option_set_type",
            "language_options_from_options",
            "trait_specific_subtrait_options_choose",
            "trait_specific_subtrait_options_from_option_set_type",
            "trait_specific_subtrait_options_from_options",
            "trait_specific_subtrait_options_type",
            "parent_index",
            "parent_name",
            "parent_url",
            "trait_specific_damage_type_index",
            "trait_specific_damage_type_name",
            "trait_specific_damage_type_url",
            "trait_specific_breath_weapon_name",
            "trait_specific_breath_weapon_desc",
            "trait_specific_breath_weapon_area_of_effect_size",
            "trait_specific_breath_weapon_area_of_effect_type",
            "trait_specific_breath_weapon_usage_type",
            "trait_specific_breath_weapon_usage_times",
            "trait_specific_breath_weapon_dc_dc_type_index",
            "trait_specific_breath_weapon_dc_dc_type_name",
            "trait_specific_breath_weapon_dc_dc_type_url",
            "trait_specific_breath_weapon_dc_success_type",
            "trait_specific_breath_weapon_damage",
        ]
    ), "Unexpected data organization, schema has probably changed"

    # Preprocessing these following columns, converting lists into a single string
    df[subraces_col_str] = df[subraces_col_str].apply(
        lambda x: ", ".join([i[subrances_col_name_key_str] for i in x])
    )
    df[races_col_str] = df[races_col_str].apply(
        lambda x: ", ".join([i[races_col_str_name_key_str] for i in x])
    )
    df[desc_col_str] = df[desc_col_str].apply(lambda x: " ".join(x))

    # Joining the proficiencies with the proficiency table so this dataset is self-contained
    df[proficiencies_col_str] = df[proficiencies_col_str].apply(
        lambda x: [get_proficiency_reference(i["url"]) for i in x]
    )

    # ------ We want to package all the draconic specific traits together. ------ #
    draconic_details = df[draconic_specific_trait_cols]

    draconic_traits = (
        draconic_details[
            ~draconic_details.isna().all(axis=1)
        ]  # Remove any rows that don't have all its records filled
        .assign(
            damage_levels=lambda x: x[  # extract the damage levels key:value pair thats inside the records in this field
                trait_specific_breath_weapon_damage_col_str
            ].apply(
                lambda y: y[0][
                    trait_specific_breath_weapon_damage_col_str_damage_at_character_level_key  # extracting the damage levels
                ]
            )
        )
        .drop(
            trait_specific_breath_weapon_damage_col_str, axis=1
        )  # no need for this column anymore
        .assign(
            index=lambda x: df.loc[x.index, index_col_str]
        )  # add the index column because we'll need the indexes to be keys later
        .set_index("index")
        .T.to_dict()  # the indexes (draconic ancestry type) are now keys, and the details of the skills are the value
    )

    # ------ Packaging these various additional detials into a single column called additional_choices ---- #
    prof_vector: list[str] = []
    additional_choices_vec: list[dict[str, Any]] = []

    # TODO: Clean up column strings
    i: DfIterTupleNames  # To keep MyPy happy
    for i in df.itertuples():  # type: ignore[assignment]
        prof_vector_str = i.desc
        additional_choices_vec.append({})
        # These contain traits that give you additional choices for skills or tools
        if not math.isnan(i.proficiency_choices_choose):
            prof_vector_str += f" Note: You can choose only upto {int(i.proficiency_choices_choose)} additional items."
            additional_choices_vec[-1]["proficiency_skill_or_tool"] = [
                j["item"]["name"] for j in i.proficiency_choices_from_options
            ]

        # These contain traits that give you additional cantrips
        elif not math.isnan(i.trait_specific_spell_options_choose):
            prof_vector_str += f" Note: You can choose only upto {int(i.trait_specific_spell_options_choose)} additional spell."
            additional_choices_vec[-1]["spell"] = [
                j["item"]["name"] for j in i.trait_specific_spell_options_from_options
            ]

        # These contain traits that give you additional languages
        elif not math.isnan(i.language_options_choose):
            prof_vector_str += f" Note: You can choose only upto {int(i.language_options_choose)} additional languages."
            additional_choices_vec[-1]["languages"] = [
                j["item"]["name"] for j in i.language_options_from_options
            ]

        # This contains choices of draconic ancestries
        elif not math.isnan(i.trait_specific_subtrait_options_choose):
            prof_vector_str += " With this trait, first choose your ancestry, then go to the trait relating to that specific ancestry. For example if you select Black, then go to 'Draconic Ancestry (Black)' trait and add the abilities for that trait."
            additional_choices_vec[-1]["draconic_ancestry"] = [
                j["item"]["name"]
                for j in i.trait_specific_subtrait_options_from_options
            ]

        # These contain specific additional skill for a specific draconic ancestry
        elif isinstance(i.parent_index, str):
            additional_choices_vec[-1]["draconic_ancestry_skill"] = [
                draconic_traits[i.index]
            ]
        else:
            pass
        prof_vector.append(prof_vector_str)

    df.desc = prof_vector
    df["additional_choices"] = additional_choices_vec

    # ---- drop all these unnecessary columns ---- #
    df = df.drop([index_col_str] + unnecessary_cols, axis=1).astype(str)

    # Check if the output data has the expected schema before saving
    assert set(df.columns.to_list()) == set(
        [
            "races",
            "subraces",
            "name",
            "desc",
            "proficiencies",
            "url",
            "additional_choices",
        ]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(save_folder / f"{ruleset}.parquet")

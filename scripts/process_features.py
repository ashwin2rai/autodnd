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
spell_name_col_str = "name"
level_col_str = "level"
desc_col_str = "desc"
index_col_str = "index"
prerequisites_col_str = "prerequisites"
prerequisites_col_type_key_str = "type"
class_index_str = "class_index"
class_name_str = "class_name"
class_url_str = "class_url"
subclass_index_str = "subclass_index"
subclass_name_str = "subclass_name"
subclass_url_str = "subclass_url"
reference_col_str = "reference"
parent_index_col_str = "parent_index"
parent_url_col_str = "parent_url"
feature_specific_expertise_options_choose_col_str = (
    "feature_specific_expertise_options_choose"
)
feature_specific_expertise_options_type_col_str = (
    "feature_specific_expertise_options_type"
)
feature_specific_expertise_options_from_option_set_type_col_str = (
    "feature_specific_expertise_options_from_option_set_type"
)
feature_specific_expertise_options_from_options_col_str = (
    "feature_specific_expertise_options_from_options"
)
feature_specific_subfeature_options_choose_col_str = (
    "feature_specific_subfeature_options_choose"
)
feature_specific_subfeature_options_type_col_str = (
    "feature_specific_subfeature_options_type"
)
feature_specific_subfeature_options_from_option_set_type_col_str = (
    "feature_specific_subfeature_options_from_option_set_type"
)
feature_specific_subfeature_options_from_options_col_str = (
    "feature_specific_subfeature_options_from_options"
)
feature_specific_invocations_col_str = "feature_specific_invocations"
feature_specific_terrain_cols = [
    "feature_specific_terrain_type_options_type",
    "feature_specific_terrain_type_options_from_options",
    "feature_specific_terrain_type_options_choose",
    "feature_specific_terrain_type_options_desc",
    "feature_specific_terrain_type_options_from_option_set_type",
]

feature_specific_favored_enemies_cols = [
    "feature_specific_enemy_type_options_from_options",
    "feature_specific_enemy_type_options_type",
    "feature_specific_enemy_type_options_desc",
    "feature_specific_enemy_type_options_from_option_set_type",
    "feature_specific_enemy_type_options_choose",
]


classes_name_col_str = "name"
class_spellcasting_info_col_str = "spellcasting_info"

# String used to fill NaN values
unknown_value_fill_string = "Unknown or Not Applicable"

if __name__ == "__main__":
    # TODO replace load from JSON with parquets once done. Parquets will have assured data.

    ### ------ Load Spells for Reference ---- ###

    # Load the JSON file
    json_rule_path = data_folder / f"{RuleSet.SPELLS.value}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    spells = pd.json_normalize(data, sep="_")

    ### ------ Load Classes for Reference ---- ###

    # Load the JSON file
    json_rule_path = data_folder / f"{RuleSet.CLASSES.value}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    classes = pd.json_normalize(data, sep="_")

    ### ------ Load Features for Processing ---- ###

    # filename string
    ruleset = RuleSet.FEATURES.value

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
            "level",
            "prerequisites",
            "desc",
            "url",
            "class_index",
            "class_name",
            "class_url",
            "subclass_index",
            "subclass_name",
            "subclass_url",
            "reference",
            "feature_specific_expertise_options_choose",
            "feature_specific_expertise_options_type",
            "feature_specific_expertise_options_from_option_set_type",
            "feature_specific_expertise_options_from_options",
            "feature_specific_subfeature_options_choose",
            "feature_specific_subfeature_options_type",
            "feature_specific_subfeature_options_from_option_set_type",
            "feature_specific_subfeature_options_from_options",
            "feature_specific_terrain_type_options_type",
            "feature_specific_terrain_type_options_from_options",
            "feature_specific_terrain_type_options_choose",
            "feature_specific_terrain_type_options_desc",
            "feature_specific_terrain_type_options_from_option_set_type",
            "feature_specific_enemy_type_options_from_options",
            "feature_specific_enemy_type_options_type",
            "feature_specific_enemy_type_options_desc",
            "feature_specific_enemy_type_options_from_option_set_type",
            "feature_specific_enemy_type_options_choose",
            "parent_index",
            "parent_name",
            "parent_url",
            "feature_specific_invocations",
        ]
    ), "Unexpected data organization, schema has probably changed"

    # Adding a column with the number of prerequisites
    df = df.assign(
        number_of_prerequisites=lambda x: x[prerequisites_col_str].apply(
            lambda y: len(y)
        )
    )

    # Converting the prerequisite text so it includes a prerequisite description which means we need to
    # join with a couple of other data tables since the originals only has the URLs
    types_of_prereq = []
    for i in df[prerequisites_col_str]:
        pre_req_dict = {}
        for j in i:
            if j[prerequisites_col_type_key_str] == "level":
                pre_req_dict["level"] = j["level"]
            elif j[prerequisites_col_type_key_str] == "feature":
                feature_row = df[df[url_col_str] == j["feature"]]
                pre_req_dict["feature"] = feature_row.name.iloc[0]
            elif j["type"] == "spell":
                feature_row = spells[spells[url_col_str] == j["spell"]]
                pre_req_dict["spell"] = feature_row[spell_name_col_str].iloc[0]
            else:
                pre_req_dict["general"] = str(j)
        types_of_prereq.append(pre_req_dict)

    df[prerequisites_col_str] = types_of_prereq

    # Concatenating the description list of strings
    df[desc_col_str] = df[desc_col_str].apply(lambda x: ",".join(x))

    # Dropping unnecessary columns
    df = df.drop(
        [
            url_col_str,
            index_col_str,
            class_index_str,
            class_url_str,
            subclass_index_str,
            subclass_url_str,
            parent_index_col_str,
            parent_url_col_str,
        ],
        axis=1,
    )

    # Converting the reference text so it includes a reference description. This is very manual processing. Couldn't
    # find a clear generalization here.
    reference_vector = []
    for i, j in zip(df[reference_col_str].fillna(""), df[class_name_str]):
        # Not sure about general rules but I observed only two kinds of references
        # either for spell casting or the draconic subclass.
        ref_str = ""
        if ("/classes" in i) and ("spellcasting" in i):
            spellcasting_info = classes[classes[classes_name_col_str] == j][
                class_spellcasting_info_col_str
            ].iloc[0]
            ref_str = "; ".join(
                [f"{i['name']}: {i['desc'][0]}" for i in spellcasting_info]
            )
        # Manual addition
        elif "/api/2014/subclasses/draconic" in i:
            ref_str = "Starting at the 6th level, when you cast a spell that deals damage of the type associated with your draconic ancestry, you can add your Charisma modifier to one damage roll of that spell. At the same time, you can spend 1 sorcery point to gain resistance to that damage type for 1 hour."
        reference_vector.append(ref_str)

    df[reference_col_str] = reference_vector

    # Some very manual data extraction and concatenation. Bard and Rogues have expertises where they can choose any 2 skills
    # Adding this to the description so I can drop the unnecessary columns
    new_desc = []
    for i in df[
        ~df[feature_specific_expertise_options_type_col_str].isna()
    ].itertuples():
        if getattr(i, class_name_str) == "Rogue" and getattr(i, level_col_str) == 1:
            list_of_options = getattr(
                i, feature_specific_expertise_options_from_options_col_str
            )
            options_str = ", ".join(
                [
                    i["item"]["name"].split("Skill: ")[1]
                    for i in list_of_options[0]["choice"]["from"]["options"]
                ]
            )
            new_desc.append(
                getattr(i, desc_col_str)
                + " These are the additional skills you may choose from: "
                + options_str
            )
        else:
            list_of_options = getattr(
                i, feature_specific_expertise_options_from_options_col_str
            )
            options_str = ", ".join(
                [i["item"]["name"].split("Skill: ")[-1] for i in list_of_options]
            )
            new_desc.append(
                getattr(i, desc_col_str)
                + " These are the additional skills you may choose from: "
                + options_str
            )

    df.loc[
        ~df[feature_specific_expertise_options_type_col_str].isna(), desc_col_str
    ] = new_desc

    # The expertise content for level 6 and 10 seemed wrong so just dropping them
    df.drop(
        df.loc[
            (~df[feature_specific_expertise_options_type_col_str].isna())
            & (df[level_col_str] > 3)
        ].index,
        axis=0,
    )

    # Dropping more unnecessary columns after all the pre-processing work above
    df = df.drop(
        [
            feature_specific_expertise_options_choose_col_str,
            feature_specific_expertise_options_type_col_str,
            feature_specific_expertise_options_from_option_set_type_col_str,
            feature_specific_expertise_options_from_options_col_str,
        ],
        axis=1,
    )

    # Some very manual data extraction and concatenation of the subclass feature options.
    new_desc = []
    for i in df[
        ~df[feature_specific_subfeature_options_from_options_col_str].isna()
    ].itertuples():
        list_of_options = getattr(
            i, feature_specific_subfeature_options_from_options_col_str
        )
        options_str = ", ".join([i["item"]["name"] for i in list_of_options])
        new_desc.append(
            getattr(i, desc_col_str)
            + ". These are some additional options you may choose from or clarifications for your additional options: "
            + options_str
        )

    df.loc[
        ~df["feature_specific_subfeature_options_from_options"].isna(), desc_col_str
    ] = new_desc

    # Dropping more unnecessary columns after all the pre-processing work above
    # Dropping all the terrain related cols because the description covers it all
    # Dropping all the favoured enemies related cols because the description covers it all
    df = df.drop(
        [
            feature_specific_subfeature_options_choose_col_str,
            feature_specific_subfeature_options_type_col_str,
            feature_specific_subfeature_options_from_option_set_type_col_str,
            feature_specific_subfeature_options_from_options_col_str,
        ]
        + feature_specific_terrain_cols
        + feature_specific_favored_enemies_cols,
        axis=1,
    )

    # TODO: Deal with invocations. For now just drop it
    df = df.drop(df[df[spell_name_col_str].str.contains("Invocations")].index, axis=0)
    df = df.drop([feature_specific_invocations_col_str], axis=1)

    output_df = df.fillna(unknown_value_fill_string)

    # Check if the output data has the expected schema before saving
    assert set(output_df.columns.to_list()) == set(
        [
            "name",
            "level",
            "prerequisites",
            "desc",
            "class_name",
            "subclass_name",
            "reference",
            "parent_name",
            "number_of_prerequisites",
        ]
    ), "Unexpected column names, schema has probably changed"

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(save_folder / f"{ruleset}.parquet")

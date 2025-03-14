import pandas as pd
import json
import io


from autodnd.utils.project_root import get_project_root
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# folder to save the content
save_folder = get_project_root() / "data" / "processed"


# column names from the original allignments json file
name_col_str = "name"
description_col_str = "desc"
url_col_str = "url"
index_col_str = "index"

# switch to include the below manual additions or not
manual_additions = True

# manual additions to the damage types since the JSON was lacking details
additional_info = io.StringIO("""
name	examples	description	strong_against	weak_against
Piercing	Spears, bites	Damage from stabbing or puncturing with weapons like spears or arrows.	Rakshasa (if magical)	Swarms, Ghosts, Treants
Bludgeoning	Maces, unarmed strikes	Damage from blunt force impacts, such as maces or fists.	Skeletons, Ice Mephits	Swarms, Ghosts, Treants
Slashing	Swords, talons	Cutting damage from sharp weapons like swords or axes.	None	Swarms, Ghosts, Oozes
Cold	Cone of Cold, Ice Devil’s spear	Damage from extreme cold or freezing effects, often from spells or creatures.	Salamanders	Liches, Storm Giants, Fiends
Fire	Burning Hands, Salamander’s tail	Damage from burning heat or flames, commonly through spells or environmental hazards.	Ice Mephits, Scarecrows, Treants, Wood Woads	Salamanders, Fiends, some Oozes
Lightning	Storm Giant’s Lightning Bolt	Damage from electrical energy, like lightning spells or magical effects.	None	Air Elementals, Liches, some Fiends
Thunder	Shatter, Thunderwave	Damage from intense sound shockwaves, causing physical force damage.	Zaratan, Earth Elementals	Air Elementals, some Undead
Poison	Stings and bites	Damage from harmful toxins, through contact, ingestion, or inhalation.	None	Assassins, Duergar, Fiends, Elementals, Undead
Acid	Acid Arrow, Black Dragon’s Breath Weapon	Corrosive damage from substances that dissolve materials on contact.	None	Black Dragons, Golems, Mimics
Necrotic	Wight’s Life Drain attack	Damage from life-draining or death-associated energy, often from undead or dark magic.	None	Most Undead
Radiant	Angel’s weapon attack	Damage from holy light or divine power, effective against shadowy or evil creatures.	Shadows, Shadow Demons	Skulks, Celestial monsters
Force	Magic Missile	Pure magical energy damage, often from forceful spells that don't rely on physical or elemental properties.	None	Helmed Horrors (and that’s it!)
Psychic	Psionic Blast, Mind Flayer’s powers	Damage that affects the mind, such as mental assaults, illusions, or telepathic attacks.	Flumphs	Constructs and Aberrations
""")

if __name__ == "__main__":
    # filename string
    ruleset = RuleSet.DAMAGE_TYPES.value

    # Load the JSON file
    json_rule_path = data_folder / f"{ruleset}.json"
    with open(json_rule_path, "r") as file:
        data = json.load(file)

    # Normalize the JSON data to flatten it
    df = pd.json_normalize(data, sep="_")

    # Check if the JSON data has the expected schema
    assert set(df.columns.to_list()) == set(["index", "name", "desc", "url"]), (
        "Unexpected data organization, schema has probably changed"
    )

    # Dropping unnecessary columns and creating a coherent description column
    df = df.drop([url_col_str, index_col_str], axis=1).assign(
        desc=lambda temp: temp.desc.apply(lambda x: " ".join(x))
    )

    if manual_additions:
        manual_additions_df = pd.read_csv(additional_info, sep="\t")

        # Quick check to ensure our manual additions and the original JSONs are alligned
        assert set(manual_additions_df.name.values) == set(df.name.values), (
            "Detected a change to the damage types. Adjust the manual additions or change manual_additions switch to False and rerun the script."
        )

        # Merge manual additions and original json and also merge descriptions. Include the original JSON descriptions as 'Additional Descriptions'
        output_df = (
            pd.merge(df, manual_additions_df, on=name_col_str, how="left")
            .assign(
                description_new=lambda x: x.description
                + " Additional Description: "
                + x[description_col_str]
            )
            .drop([description_col_str, "description"], axis=1)
            .rename(columns={"description_new": description_col_str})
        )
    else:
        # Adding some placeholder columns which the manual additions include to keep schema consistent
        df[["examples", "strong_against", "weak_against"]] = ""
        output_df = df

    # Save the table as a parquet file
    save_folder.mkdir(parents=True, exist_ok=True)
    output_df.to_parquet(save_folder / f"{ruleset}.parquet")

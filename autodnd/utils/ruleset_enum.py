from enum import Enum


class RuleSet(Enum):
    ABILITY_SCORES = "Ability-Scores"
    ALIGNMENTS = "Alignments"
    BACKGROUNDS = "Backgrounds"
    CLASSES = "Classes"
    CONDITIONS = "Conditions"
    DAMAGE_TYPES = "Damage-Types"
    EQUIPMENT_CATEGORIES = "Equipment-Categories"
    EQUIPMENT = "Equipment"
    FEATS = "Feats"
    FEATURES = "Features"
    LANGUAGES = "Languages"
    LEVELS = "Levels"
    MAGIC_ITEMS = "Magic-Items"
    MAGIC_SCHOOLS = "Magic-Schools"
    MONSTERS = "Monsters"
    PROFICIENCIES = "Proficiencies"
    RACES = "Races"
    RULE_SECTIONS = "Rule-Sections"
    RULES = "Rules"
    SKILLS = "Skills"
    SPELLS = "Spells"
    SUBCLASSES = "Subclasses"
    SUBRACES = "Subraces"
    TRAITS = "Traits"
    WEAPON_PROPERTIES = "Weapon-Properties"

    @classmethod
    def to_list(cls) -> list:
        """Return all RuleSet values as a list."""
        return [member.value for member in cls]

from autodnd.utils.project_root import get_project_root
from autodnd.utils.fetch_tools import download_file
from autodnd.utils.ruleset_enum import RuleSet

# folder where you want the jsons to reside
data_folder = get_project_root() / "data" / "jsonrules"

# the various ruleset strings
ruleset_list = RuleSet.to_list()

# the year of the rules, currently only 2014 exists
year = "2014"

if __name__ == "__main__":
    for ruleset in ruleset_list:
        json_rule_path = data_folder / f"{ruleset}.json"
        ruleset_url = f"https://raw.githubusercontent.com/5e-bits/5e-database/main/src/{year}/5e-SRD-{ruleset}.json"
        download_file(json_rule_path.parents[0], json_rule_path.name, ruleset_url)

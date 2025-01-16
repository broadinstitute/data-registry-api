from abc import ABC, abstractmethod


class Validator(ABC):

    def __init__(self, required_columns: list, optional_columns: list):
        self.required_columns = required_columns
        self.optional_columns = optional_columns

    @abstractmethod
    def validate(self, data: dict) -> list:
        pass

    def check_required_columns(self, cols: list) -> list:
        missing_columns = [c for c in self.required_columns if c not in cols]
        return [f'{col} is required' for col in missing_columns]

    def column_options(self) -> dict:
        return {'required': self.required_columns, 'optional': self.optional_columns}


class HermesValidator(Validator):
    required_fields = ['chromosome', 'position', 'non-effect allele', 'effect allele', 'pValue', 'N total', 'se']
    optional_fields = ['maf', 'eaf', 'N cases', 'beta', 'oddsRatio', 'oddsRatioUB', 'oddsRatioLB']

    def __init__(self):
        super().__init__(HermesValidator.required_fields, HermesValidator.optional_fields)

    def validate(self, data: dict) -> list:
        errors = []
        col_map = data.get("column_map")
        errors.extend(self.check_required_columns(list(col_map.keys())))
        required_metadata = ["cohort", "ancestry", "case_ascertainment", "case_type", "phenotype", "participants",
                             "cases", "sex_proportion", "age_at_first_documented_study_phenotype",
                             "analysis_software", "statistical_model", "covariates"]
        for field in required_metadata:
            if not data.get(field):
                errors.append(f"You must specify {field}")

        return errors

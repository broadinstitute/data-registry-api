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

    @staticmethod
    def infer_columns(data) -> dict:
        res = {}
        res.update(data.get("column_map"))
        if res.get("beta") is None and res.get("oddsRatio") is not None:
            res["beta"] = 'derived'
        if res.get("stdErr") is None and res.get("beta") is not None:
            res["stdErr"] = 'derived'
        if res.get("zScore") is None and res.get("beta") is not None and res.get("stdErr") is not None:
            res["zScore"] = 'derived'
        if res.get("maf") is None and res.get("eaf") is not None:
            res["maf"] = 'derived'
        return res


class HermesValidator(Validator):
    required_fields = ['chromosome', 'position', 'reference', 'alt', 'pValue']
    optional_fields = ['beta', 'oddsRatio', 'stdErr', 'n', 'zScore', 'maf', 'eaf', 'rsid']

    def __init__(self):
        super().__init__(HermesValidator.required_fields, HermesValidator.optional_fields)

    def validate(self, data: dict) -> list:
        errors = []
        col_map = data.get("column_map")
        errors.extend(self.check_required_columns(list(col_map.keys())))
        inferred_cols = Validator.infer_columns(data)
        if inferred_cols.get("beta") is None:
            errors.append("You must specify beta or oddsRatio")
        if inferred_cols.get("stdErr") is None:
            errors.append("You must specify stdErr or beta")
        if inferred_cols.get("zScore") is None:
            errors.append("You must specify zScore or beta and stdErr")
        if inferred_cols.get("maf") is None:
            errors.append("You must specify maf or eaf")
        return errors

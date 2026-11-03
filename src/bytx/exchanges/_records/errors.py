class OrderValidationError(Exception):
    exchange: str = 'Undefined'
    is_valid: bool = False
    errors: list = []
    warnings: list = []
    calculated_values: dict = {}

    def __init__(self, exchange: str, is_valid: bool,
                 errors: list, warnings: list,
                 calculated_values: dict):
        self.exchange = exchange
        self.is_valid = is_valid
        self.errors = errors
        self.warnings = warnings
        self.calculated_values = calculated_values
        super().__init__(f'{self.exchange}:{self.is_valid}:{self.errors}:{self.warnings}:{self.calculated_values}')

    def get_errors(self):
        return self.errors

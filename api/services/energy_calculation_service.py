

class EnergyCalculationService:

    def __init__(self):
        self.avg_days_in_year = 365.25
        self.hours_in_day = 24
        self.max_coefficient = 1.2
        self.min_coefficient = 1.01
        self.guess_coefficient = 1.1

    def min_consumption(self, profitability_equipment, hash_rate: float) -> float:
        return min(profitability_equipment) * hash_rate * self.avg_days_in_year * self.hours_in_day / 1e9 * self.min_coefficient

    def max_consumption(self, profitability_equipment, hash_rate: float) -> float:
        return max(profitability_equipment) * hash_rate * self.avg_days_in_year * self.hours_in_day / 1e9 * self.max_coefficient

    def guess_consumption(self, profitability_equipment, hash_rate: float, hash_rates, typed_avg_efficiency) -> float:
        return self._calculate_guess(
            profitability_equipment,
            hash_rates,
            typed_avg_efficiency
        ) * hash_rate * self.avg_days_in_year * self.hours_in_day / 1e9 * self.guess_coefficient

    def min_power(self, profitability_equipment, hash_rate: float) -> float:
        return min(profitability_equipment) * hash_rate / 1e6 * self.min_coefficient

    def max_power(self, profitability_equipment, hash_rate: float) -> float:
        return max(profitability_equipment) * hash_rate / 1e6 * self.max_coefficient

    def guess_power(self, profitability_equipment, hash_rate: float, hash_rates, typed_avg_efficiency) -> float:
        return self._calculate_guess(
            profitability_equipment,
            hash_rates,
            typed_avg_efficiency
        ) * hash_rate / 1e6 * self.guess_coefficient

    def _calculate_guess(self, profitability_equipment, hash_rates, typed_avg_efficiency):
        guess_consumption = self._get_avg(profitability_equipment)
        if guess_consumption == 0:
            return 0
        for t, hr in hash_rates.items():
            guess_consumption += hr * typed_avg_efficiency.get(t.lower(), 0)
        return guess_consumption

    @staticmethod
    def _get_avg(values) -> float:
        len_values = len(values)
        return sum(values) / len_values if len_values > 0 else 0

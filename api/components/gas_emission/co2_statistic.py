import numpy as np


class Co2Statistic:
    """
    [1] Life Cycle Greenhouse Gas Emissions from Electricity Generation: Update (September 2021) https://www.nrel.gov/docs/fy21osti/80580.pdf
    [2] Emissions of selected electricity supply technologies (gCO2eq/kWh): https://www.ipcc.ch/site/assets/uploads/2018/02/ipcc_wg3_ar5_annex-iii.pdf Lifecycle emissions (incl. albedo effect); Median values being used.
    """

    @staticmethod
    def hydro():
        return 24  # [2]

    @staticmethod
    def wind():
        return np.mean([11, 12])  # onshore & offshore [2]

    @staticmethod
    def nuclear():
        return 12  # [2]

    @staticmethod
    def solar():
        return np.mean([41, 48, 27])  # solar rooftop, solar utility, concentrated solar power [2]

    @staticmethod
    def gas():
        return 469  # Natural Gas [2]

    @staticmethod
    def coal():
        return 820  # [2]

    @staticmethod
    def oil():
        return 840  # [1]

    @staticmethod
    def other():
        return np.mean([38, 740, 230])  # geothermal, Biomass (cofiring) & Biomass (dedicated) [2]

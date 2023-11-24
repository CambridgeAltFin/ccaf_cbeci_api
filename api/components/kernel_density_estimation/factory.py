from components.kernel_density_estimation.validator import KernelDensityEstimationValidator
from components.kernel_density_estimation.service import KernelDensityEstimationService


class KernelDensityEstimationFactory:
    @staticmethod
    def create_validator():
        return KernelDensityEstimationValidator()

    @staticmethod
    def create_service():
        return KernelDensityEstimationService()
from sklearn.neighbors import KernelDensity
import numpy as np

class KernelDensityEstimationService:
    def __init__(self):
        pass

    def get_kde(self, values, bandwidth = 1):
        if (not bandwidth):
            bandwidth = 1
        X_plot = np.linspace(995, 1005, 25)[:, np.newaxis]
        data = np.array([[val * 1000] for val in values])
        kde = KernelDensity(kernel='gaussian', bandwidth=bandwidth).fit(data)
        samples = kde.score_samples(X_plot)
        result = []
        for i in range(samples.size):
            result.append({
                "y": samples[i],
                "x": round(0.9952 + i * 0.0004, 4)
            })
        return result
        # return samples
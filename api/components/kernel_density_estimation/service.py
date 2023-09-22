from sklearn.neighbors import KernelDensity
import numpy as np

class KernelDensityEstimationService:
    def __init__(self):
        pass

    def get_kde(self, values, maxY = 10, bandwidth = 1, start = 0.995, end = 1.005, count = 27):
        if (not bandwidth):
            bandwidth = 1
        if (not maxY):
            maxY = 10
        if (not start):
            start = 0.995
        if (not end):
            end = 1.005
        if (not count):
            count = 27

        X_plot = np.linspace(start * 1000, end * 1000, count)[:, np.newaxis]
        data = np.array([[val * 1000] for val in values])
        kde = KernelDensity(kernel='gaussian', bandwidth=bandwidth).fit(data)
        samples = kde.score_samples(X_plot)
        result = []
        maxX = max(samples)
        minX = min(samples)
        k = -maxY / (minX - maxX)
        c = maxY - k * maxX
        for i in range(samples.size):
            result.append({
                "y": round(samples[i] * k + c, 4),
                "x": round(X_plot[i][0] / 1000, 4)
            })
        # for i in range(samples.size):
        #     result.append(round(samples[i] * k + c, 4))
        return result
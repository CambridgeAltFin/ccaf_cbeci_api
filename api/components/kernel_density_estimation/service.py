from sklearn.neighbors import KernelDensity
import numpy as np

class KernelDensityEstimationService:
    def __init__(self):
        pass

    def get_kde(self, values, maxY = 10, bandwidth = 1, start = 0.995, end = 1.005, count = 27):
        X_plot = [[False]]
        add_to_end = False
        if (not bandwidth):
            bandwidth = 1
        if (not maxY):
            maxY = 10
        if (not start):
            X_plot = [[995]]
            start = 0.9952
        if (not end):
            add_to_end = True
            end = 1.0048
        if (not count):
            count = 25

        if(X_plot[0][0]):
            X_plot = np.concatenate((X_plot, np.linspace(start * 1000, end * 1000, count)[:, np.newaxis]))
        else:
            X_plot = np.linspace(start * 1000, end * 1000, count)[:, np.newaxis]
        if(add_to_end):
            X_plot = np.concatenate((X_plot, [[1005]]))

        data = np.array([[val * 1000] for val in values])
        kde = KernelDensity(kernel='gaussian', bandwidth=bandwidth).fit(data)
        samples = np.exp(kde.score_samples(X_plot))
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
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# from sklearn.linear_model import LinearRegression

# def classify_trend(csv_path, keyword):
#     # Load data
#     df = pd.read_csv(csv_path, index_col=0)
#     df = df.dropna()

#     y = df[keyword].values
#     x = np.arange(len(y)).reshape(-1, 1)

#     # Linear regression
#     model = LinearRegression()
#     model.fit(x, y)
#     slope = model.coef_[0]

#     # Early vs recent averages
#     n = len(y)
#     early_avg = np.mean(y[: n // 3])
#     recent_avg = np.mean(y[-n // 3 :])

#     # Classification rules
#     if slope > 0.5 and recent_avg > early_avg * 1.3:
#         label = "Accelerating"
#     elif slope > 0.1:
#         label = "Emerging"
#     elif slope < -0.1:
#         label = "Declining"
#     else:
#         label = "Stable"

#     return label, slope, df, model

# def plot_trend(df, keyword, model, label):
#     x = np.arange(len(df)).reshape(-1, 1)
#     y = df[keyword].values
#     y_pred = model.predict(x)

#     plt.figure(figsize=(10, 5))
#     plt.plot(df.index, y, label="Search Interest", linewidth=2)
#     plt.plot(df.index, y_pred, linestyle="--", label="Trend Line")
#     plt.title(f"{keyword.capitalize()} — {label}")
#     plt.xlabel("Date")
#     plt.ylabel("Google Trends Interest")
#     plt.legend()
#     plt.xticks(rotation=45)
#     plt.tight_layout()
#     plt.show()

# label, slope, df, model = classify_trend(
#     csv_path="trends_ramen.csv",
#     keyword="ramen"
# )

# print(f"Trend classification: {label}")
# print(f"Slope: {slope:.3f}")

# plot_trend(df, "ramen", model, label)

#_______________FOR GDELT DATA_____________________

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import matplotlib.dates as mdates
import os

CSV_DIR = "results/csv"
PLOT_DIR = "results/plots"
SUMMARY_FILE = "results/trend_summary.csv"

os.makedirs(PLOT_DIR, exist_ok=True)

summary_rows = []

def classify_trend(df, keyword):
    y = df[keyword].values
    x = np.arange(len(y)).reshape(-1, 1)

    model = LinearRegression()
    model.fit(x, y)
    slope = model.coef_[0]

    n = len(y)
    early_avg = np.mean(y[: n // 3])
    recent_avg = np.mean(y[-n // 3 :])

    if slope > 0.5 and recent_avg > early_avg * 1.3:
        label = "Accelerating"
    elif slope > 0.1:
        label = "Emerging"
    elif slope < -0.1:
        label = "Declining"
    else:
        label = "Stable"

    return label, slope, model


def plot_trend(df, keyword, model, label):
    # Convert index to datetime
    df.index = pd.to_datetime(df.index)

    x = np.arange(len(df)).reshape(-1, 1)
    y = df[keyword].values
    y_pred = model.predict(x)

    plt.figure(figsize=(10, 5))
    plt.plot(df.index, y, label="Search Interest", linewidth=2)
    plt.plot(df.index, y_pred, linestyle="--", label="Trend Line")

    plt.title(f"{keyword.title()} — {label}")
    plt.xlabel("Month")
    plt.ylabel("Google Trends Interest")
    plt.legend()

    # ---- FIX: Month-wise ticks ----
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

    plt.xticks(rotation=0)
    plt.tight_layout()

    filename = keyword.replace(" ", "_")
    plt.savefig(f"{PLOT_DIR}/{filename}.png")
    plt.close()


# -------- MAIN LOOP --------
for file in os.listdir(CSV_DIR):
    if not file.endswith(".csv"):
        continue

    keyword = file.replace(".csv", "").replace("_", " ")
    path = os.path.join(CSV_DIR, file)

    df = pd.read_csv(path, index_col=0)
    df = df.dropna()

    if df.empty:
        continue

    label, slope, model = classify_trend(df, keyword)
    plot_trend(df, keyword, model, label)

    summary_rows.append({
        "food_item": keyword,
        "trend_label": label,
        "slope": round(slope, 3)
    })

    print(f"{keyword}: {label} (slope={slope:.3f})")


# Save summary CSV
summary_df = pd.DataFrame(summary_rows)
summary_df.to_csv(SUMMARY_FILE, index=False)

print("\nSaved trend summary →", SUMMARY_FILE)

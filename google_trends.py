# from pytrends.request import TrendReq
# import time

# pytrends = TrendReq(
#     hl='en-IN',
#     tz=330,
#     retries=3,
#     backoff_factor=2
# )

# print("Connected to Google Trends")

# ___________________ FOR ONE KEYWORD ________________
# keyword = ["ramen"]

# pytrends.build_payload(
#     kw_list=keyword,
#     timeframe="today 3-m",
#     geo="IN"
# )

# interest_over_time = pytrends.interest_over_time()

# if not interest_over_time.empty:
#     interest_over_time = interest_over_time.drop(columns=["isPartial"])
#     print(interest_over_time.head())
# else:
#     print("No data returned")

# time.sleep(10)

# ___________________ FOR MULTIPLE KEYWORDS ________________
# keywords = ["ramen", "hot chocolate", "protein coffee"]

# for kw in keywords:
#     print(f"Fetching trends for: {kw}")

#     pytrends.build_payload(
#         kw_list=[kw],
#         timeframe="today 3-m",
#         geo="IN"
#     )

#     data = pytrends.interest_over_time()

#     if not data.empty:
#         data = data.drop(columns=["isPartial"])
#         data.to_csv(f"trends_{kw.replace(' ', '_')}.csv")

#     time.sleep(15)

# ___________________ FOR GDELT RESULTS ________________
from pytrends.request import TrendReq
import time
import os

# Create output directories
os.makedirs("results/csv", exist_ok=True)

pytrends = TrendReq(
    hl="en-IN",
    tz=330,
    retries=3,
    backoff_factor=2
)

print("Connected to Google Trends")

# Example food phrases from GDELT output
food_items = [
    "mac cheese",
    "dubai chocolate",
    "japanese cheesecake",
    "air fryer bacon",
    "sweet potato",
    "fried chicken",
    "cheddar soup",
    "paratha burger",
    "protein coffee",
    "hot chocolate",
    "jell o salad"
]

for food in food_items:
    print(f"Fetching Google Trends data for: {food}")

    try:
        pytrends.build_payload(
            kw_list=[food],
            timeframe="today 3-m",
            geo="IN"
        )

        data = pytrends.interest_over_time()

        if not data.empty:
            data = data.drop(columns=["isPartial"])
            filename = food.replace(" ", "_")
            data.to_csv(f"results/csv/{filename}.csv")
            print(f"Saved → results/csv/{filename}.csv")
        else:
            print(f"No data for {food}")

        time.sleep(15)  # VERY important to avoid 429

    except Exception as e:
        print(f"Error fetching {food}: {e}")




